use crate::iterator::StorageIter;
use crate::types::Value;

use bytes::Bytes;
use std::cmp::Ordering;
use std::collections::{BinaryHeap, VecDeque};

pub(crate) struct MergeIterator<I> {
    iters: Vec<I>,
    heap: BinaryHeap<HeapItem>,
    last_key: Option<Bytes>,
}

#[derive(Debug, Eq)]
struct HeapItem {
    key: Bytes,
    value: Value,
    iter_idx: usize,
}

// Reverse the natural ordering in order to get a min-heap.
impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .key
            .cmp(&self.key)
            .then_with(|| other.iter_idx.cmp(&self.iter_idx))
    }
}

impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for HeapItem {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.iter_idx == other.iter_idx
    }
}

impl<I: StorageIter<KeyVal = (Bytes, Value)>> MergeIterator<I> {
    pub(crate) fn new(memtable_iters: VecDeque<I>) -> Self {
        if memtable_iters.is_empty() {
            return Self {
                iters: vec![],
                heap: BinaryHeap::new(),
                last_key: None,
            };
        }

        let mut iters = Vec::with_capacity(memtable_iters.len());
        let mut heap = BinaryHeap::with_capacity(memtable_iters.len());

        for (idx, iter) in memtable_iters.into_iter().enumerate() {
            let mut iter = iter;

            if let Some((k, v)) = StorageIter::next(&mut iter) {
                heap.push(HeapItem {
                    key: k,
                    value: v,
                    iter_idx: idx,
                });
            }

            iters.push(iter);
        }

        Self {
            iters,
            heap,
            last_key: None,
        }
    }
}

impl<I: StorageIter<KeyVal = (Bytes, Value)>> Iterator for MergeIterator<I> {
    type Item = (Bytes, Value);

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(item) = self.heap.pop() {
            let HeapItem {
                key,
                value,
                iter_idx,
            } = item;

            if let Some((k, v)) = StorageIter::next(&mut self.iters[iter_idx]) {
                self.heap.push(HeapItem {
                    key: k.clone(),
                    value: v.clone(),
                    iter_idx,
                })
            }

            if let Some(last) = &self.last_key
                && *last == key
            {
                continue;
            }

            self.last_key = Some(key.clone());
            return Some((key.clone(), value.clone()));
        }

        None
    }
}

#[cfg(test)]
mod tests {

    use crate::iterator::merge_iterator::{HeapItem, MergeIterator};
    use crate::memtable::MemtableIterator;
    use crate::types::Value;
    use bytes::Bytes;
    use crossbeam_skiplist::SkipMap;
    use std::collections::{BinaryHeap, VecDeque};
    use std::ops::Bound;
    use std::sync::Arc;

    #[test]
    fn heap_item_ordering() {
        let item_one = HeapItem {
            key: Bytes::from("key_1"),
            value: Value::Tombstone,
            iter_idx: 1,
        };

        let item_two = HeapItem {
            key: Bytes::from("key_2"),
            value: Value::Tombstone,
            iter_idx: 2,
        };

        let item_three = HeapItem {
            key: Bytes::from("key_1"),
            value: Value::Tombstone,
            iter_idx: 3,
        };

        let mut heap = BinaryHeap::new();

        heap.push(item_two);
        heap.push(item_one);
        heap.push(item_three);

        let mut out = heap.pop().unwrap();
        assert_eq!(out.key, Bytes::from("key_1"));
        assert_eq!(out.iter_idx, 1);

        out = heap.pop().unwrap();
        assert_eq!(out.key, Bytes::from("key_1"));
        assert_eq!(out.iter_idx, 3);

        out = heap.pop().unwrap();
        assert_eq!(out.key, Bytes::from("key_2"));
        assert_eq!(out.iter_idx, 2);
    }

    fn build_map_with(items: Vec<(String, String)>) -> SkipMap<Bytes, Value> {
        let map = SkipMap::new();
        for (k, v) in items.iter() {
            let val = if v.is_empty() {
                Value::Tombstone
            } else {
                Value::Plain(Bytes::from(v.clone()))
            };
            map.insert(Bytes::from(k.clone()), val);
        }

        map
    }

    fn build_multiple_maps() -> Vec<Arc<SkipMap<Bytes, Value>>> {
        let mut maps = vec![];

        let map_one: SkipMap<Bytes, Value> = build_map_with(vec![
            ("key_1".into(), "key_1_value".into()),
            ("key_2".into(), "key_2_value".into()),
            ("key_3".into(), "key_3_value".into()),
        ]);
        let map_two: SkipMap<Bytes, Value> = build_map_with(vec![
            ("key_1".into(), "key_1_value_2".into()),
            ("key_4".into(), "key_4_value".into()),
        ]);
        let map_three: SkipMap<Bytes, Value> = build_map_with(vec![
            ("key_2".into(), "".into()),
            ("key_5".into(), "key_5_value".into()),
        ]);
        maps.push(Arc::new(map_three));
        maps.push(Arc::new(map_two));
        maps.push(Arc::new(map_one));
        maps
    }

    #[test]
    fn merge_iter_allows_iteration_over_multiple_memtables() {
        let maps = build_multiple_maps();

        let iters: VecDeque<MemtableIterator> = maps
            .iter()
            .map(|map| MemtableIterator::from_map(map.clone()))
            .collect();

        let mut merge_iter = MergeIterator::new(iters);

        let mut result: Vec<(Bytes, Value)> = vec![];
        for (k, v) in merge_iter.by_ref() {
            result.push((k, v));
        }

        let mut result_iter = result.iter();
        assert_eq!(
            *result_iter.next().unwrap(),
            (
                Bytes::from("key_1"),
                Value::Plain(Bytes::from("key_1_value_2"))
            )
        );
        assert_eq!(
            *result_iter.next().unwrap(),
            (Bytes::from("key_2"), Value::Tombstone)
        );
        assert_eq!(
            *result_iter.next().unwrap(),
            (
                Bytes::from("key_3"),
                Value::Plain(Bytes::from("key_3_value"))
            )
        );
        assert_eq!(
            *result_iter.next().unwrap(),
            (
                Bytes::from("key_4"),
                Value::Plain(Bytes::from("key_4_value"))
            )
        );
        assert_eq!(
            *result_iter.next().unwrap(),
            (
                Bytes::from("key_5"),
                Value::Plain(Bytes::from("key_5_value"))
            )
        );
    }

    #[test]
    fn merge_iter_over_subset_of_keys() {
        let maps = build_multiple_maps();
        let lower_bound: Bound<Bytes> = Bound::Included("key_2".into());
        let upper_bound: Bound<Bytes> = Bound::Included("key_4".into());

        let iters: VecDeque<MemtableIterator> = maps
            .iter()
            .map(|map| {
                MemtableIterator::range(map.clone(), lower_bound.clone(), upper_bound.clone())
            })
            .collect();

        let mut merge_iter = MergeIterator::new(iters);

        let mut result: Vec<(Bytes, Value)> = vec![];
        for (k, v) in merge_iter.by_ref() {
            result.push((k, v));
        }
        let mut result_iter = result.iter();

        assert_eq!(
            *result_iter.next().unwrap(),
            (Bytes::from("key_2"), Value::Tombstone)
        );
        assert_eq!(
            *result_iter.next().unwrap(),
            (
                Bytes::from("key_3"),
                Value::Plain(Bytes::from("key_3_value"))
            )
        );
        assert_eq!(
            *result_iter.next().unwrap(),
            (
                Bytes::from("key_4"),
                Value::Plain(Bytes::from("key_4_value"))
            )
        );
    }
}
