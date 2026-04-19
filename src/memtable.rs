use crate::index::IndexBuilder;
use crate::iterator::StorageIter;
use crate::sst::block::{Block, BlockBuilder};
use crate::sst::{self, SSTable};

use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use crossbeam_skiplist::map::{Iter, Range};
use ouroboros::self_referencing;

use std::ops::Bound;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

const WAL_DIR: &str = "wal";
pub(crate) const MAX_TABLE_SIZE: usize = 128 * 1024 * 1024;

#[derive(Debug, Clone)]
pub(crate) struct TableOptions {
    pub(crate) max_size: usize,
    pub(crate) data_dir: PathBuf,
}

impl TableOptions {
    pub(crate) fn new(max_size: usize, data_dir: &Path) -> Self {
        Self {
            max_size,
            data_dir: data_dir.to_path_buf(),
        }
    }
}

impl Default for TableOptions {
    fn default() -> Self {
        Self {
            max_size: MAX_TABLE_SIZE,
            data_dir: PathBuf::from(crate::core::DATA_DIR),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Value {
    Plain(Bytes),
    Tombstone,
}

#[derive(Debug, Clone)]
pub(crate) struct Memtable {
    id: usize,
    max_size: usize,
    size: Arc<AtomicUsize>,
    store: Arc<SkipMap<Bytes, Value>>,
    wal_dir: PathBuf,
}

#[derive(thiserror::Error, Debug)]
pub(crate) enum MemError {
    #[error("WAL directory initialization failure")]
    WalInitFailure(#[from] std::io::Error),
    #[error("Key already deleted")]
    ValueTombstoned,
    #[error("Invalid range key")]
    InvalidRange,
    #[error("Unable to freeze memtable")]
    FreezeFailure,
    #[error("Memtable size limit exceeded")]
    MaxSizeExceeded,
}

fn create_wal_dir(path: &Path) -> Result<(), MemError> {
    match std::fs::exists(path) {
        Ok(wal_dir_exists) if wal_dir_exists => Ok(()),
        Ok(_) => Ok(std::fs::create_dir_all(path)?),
        Err(e) => Err(MemError::WalInitFailure(e)),
    }
}

pub(crate) trait Buffer {
    fn create(id: usize, options: Option<TableOptions>) -> Self;

    fn size(&self) -> usize;

    fn put(&self, key: &Bytes, value: &Bytes) -> Result<usize, MemError>;

    fn delete(&self, key: &Bytes) -> Result<(), MemError>;

    fn get(&self, key: &Bytes) -> Option<Bytes>;

    fn scan(&self, start: Bound<Bytes>, end: Bound<Bytes>) -> MemtableIterator;

    fn flush(&self) -> Result<SSTable, MemError>;
}

impl Buffer for Memtable {
    fn create(id: usize, options: Option<TableOptions>) -> Self {
        let options = options.unwrap_or_default();

        let mut wal_dir = options.data_dir;
        wal_dir.push(WAL_DIR);
        wal_dir.push(format!("{}", id));

        match create_wal_dir(&wal_dir) {
            Ok(_) => {}
            Err(e) => panic!("Failed to create memtable: {e}"),
        }

        Memtable {
            id,
            max_size: options.max_size,
            store: Arc::new(SkipMap::new()),
            size: Arc::new(0.into()),
            wal_dir,
        }
    }

    fn size(&self) -> usize {
        self.size.load(Ordering::Relaxed)
    }

    fn put(&self, key: &Bytes, value: &Bytes) -> Result<usize, MemError> {
        let pair_size: usize = key.len() + value.len();
        self.size.fetch_add(pair_size, Ordering::SeqCst);
        self.store.insert(key.clone(), Value::Plain(value.clone()));
        Ok(self.size.load(Ordering::SeqCst))
    }

    fn get(&self, key: &Bytes) -> Option<Bytes> {
        self.store.get(key).and_then(|entry| match entry.value() {
            Value::Plain(val) => Some(val.clone()),
            Value::Tombstone => None,
        })
    }

    fn delete(&self, key: &Bytes) -> Result<(), MemError> {
        self.get(key)
            .map(|_| {
                self.size.fetch_add(key.len(), Ordering::Relaxed);
                self.store.insert(key.clone(), Value::Tombstone);
            })
            .ok_or(MemError::ValueTombstoned)
    }

    fn scan(&self, start: Bound<Bytes>, end: Bound<Bytes>) -> MemtableIterator {
        MemtableIterator::range(self.store.clone(), start, end)
    }

    fn flush(&self) -> Result<SSTable, MemError> {
        build_sstable(self.store.iter(), sst::BLOCK_SIZE)
    }
}

type SkipMapRange<'a> = Range<'a, Bytes, (Bound<Bytes>, Bound<Bytes>), Bytes, Value>;

#[self_referencing]
#[derive(Debug)]
pub(crate) struct MemtableIterator {
    map: Arc<SkipMap<Bytes, Value>>,
    #[borrows(map)]
    #[not_covariant]
    inner: SkipMapRange<'this>,
    current: Option<(Bytes, Value)>,
}

impl MemtableIterator {
    pub(crate) fn from_map(map: Arc<SkipMap<Bytes, Value>>) -> Self {
        MemtableIteratorBuilder {
            map,
            inner_builder: |map| map.range((Bound::Unbounded, Bound::Unbounded)),
            current: None,
        }
        .build()
    }

    pub(crate) fn range(
        map: Arc<SkipMap<Bytes, Value>>,
        start: Bound<Bytes>,
        end: Bound<Bytes>,
    ) -> Self {
        MemtableIteratorBuilder {
            map,
            inner_builder: |map| map.range((start, end)),
            current: None,
        }
        .build()
    }
}

impl StorageIter for MemtableIterator {
    type KeyVal = (Bytes, Value);

    fn next(&mut self) -> Option<Self::KeyVal> {
        std::iter::Iterator::next(self)
    }
}

impl Iterator for MemtableIterator {
    type Item = (Bytes, Value);

    fn next(&mut self) -> Option<Self::Item> {
        self.with_inner_mut(|inner| {
            inner
                .next()
                .map(|item| (item.key().clone(), item.value().clone()))
        })
    }
}

pub(crate) fn build_sstable(
    memtable_iter: Iter<'_, Bytes, Value>,
    block_size: usize,
) -> Result<SSTable, MemError> {
    let mut block_builder = BlockBuilder::init(1, block_size);
    let mut index_builder = IndexBuilder::builder();
    memtable_iter.for_each(|entry| {
        let key = entry.key().to_vec();
        let value = match entry.value() {
            Value::Plain(v) => v.to_vec(),
            Value::Tombstone => vec![],
        };

        block_builder.encode_pair(&key, &value);
        let block_offset = block_builder.latest_block_offset();
        if let Some(key_offset) = block_builder.latest_key_offset() {
            index_builder.add_entry(&key, block_offset, key_offset);
        }
    });

    SSTable::new(&mut block_builder, &mut index_builder, sst::MAX_TABLE_SIZE)
        .map_err(|_| MemError::FreezeFailure)
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;
    use temp_dir::TempDir;

    fn test_memtable() -> Memtable {
        let tempdir = TempDir::new();
        let options = TableOptions {
            data_dir: tempdir.unwrap().path().to_path_buf(),
            max_size: 256,
        };
        Memtable::create(1, Some(options))
    }

    #[test]
    fn memtable_stores_and_retrieves_key_value_pairs() {
        let table = test_memtable();
        let key = Bytes::from("1");
        let value = Bytes::from("123");

        let result = table.put(&key, &value);
        assert!(result.is_ok());

        let val = table.get(&key);
        assert!(val.is_some());
        assert_eq!(val.unwrap(), value);
    }

    #[test]
    fn memtable_stores_tombstone_marker_for_key_being_deleted() {
        let table = test_memtable();
        let key = Bytes::from("1");
        let value = Bytes::from("123");

        let put_result = table.put(&key, &value);
        assert!(put_result.is_ok());

        let result = table.delete(&key);
        assert!(result.is_ok());

        let get_result = table.get(&key);
        assert!(get_result.is_none());
    }

    #[test]
    fn memtable_returns_empty_if_queried_key_does_not_exist() {
        let table = test_memtable();
        let key = Bytes::from("1");

        let result = table.get(&key);

        assert!(result.is_none());
    }

    #[test]
    fn memtable_initializes_wal_directory() {
        let tempdir = TempDir::new().unwrap();
        let path = tempdir.path();
        let options = TableOptions {
            data_dir: path.to_path_buf(),
            max_size: MAX_TABLE_SIZE,
        };
        let _ = Memtable::create(1, Some(options));

        let mut expected_wal_dir = PathBuf::from(path);
        expected_wal_dir.push("wal/1/");

        let result = std::fs::exists(expected_wal_dir);
        assert!(result.is_ok());
        assert!(result.unwrap());
    }

    #[test]
    fn build_sstable_from_memtable_contents() {
        let table = test_memtable();
        let k1 = Bytes::from("1");
        let v1 = Bytes::from("123");

        let k2 = Bytes::from("2");
        let v2 = Bytes::from("456");

        let k3 = Bytes::from("3");
        let v3 = Bytes::from("789");

        let k4 = Bytes::from("4");
        let v4 = Bytes::from("0000");

        let _ = table.put(&k1, &v1);
        let _ = table.put(&k2, &v2);
        let _ = table.put(&k3, &v3);
        let _ = table.put(&k4, &v4);

        let result = build_sstable(table.store.iter(), 32);
        assert!(result.is_ok());

        let table: SSTable = result.unwrap();

        assert_eq!(table.first_key, k1.to_vec());
        assert_eq!(table.last_key, k4.to_vec());
        assert_eq!(table.blocks.len(), 2);

        let k1_idx = table.index.find_block_by_key(&k1).unwrap();
        assert_eq!(k1_idx.offsets(), (0, 0));

        let k4_idx = table.index.find_block_by_key(&k4).unwrap();
        let first_block = table.blocks[0].clone();
        assert_eq!(k4_idx.offsets(), (first_block.size() as u64, 0));
    }

    #[test]
    fn memtable_freeze_creates_sst() {
        let table = test_memtable();
        let k1 = Bytes::from("1");
        let v1 = Bytes::from("123");

        let k2 = Bytes::from("2");
        let v2 = Bytes::from("456");

        let k3 = Bytes::from("3");
        let v3 = Bytes::from("789");

        let _ = table.put(&k1, &v1);
        let _ = table.put(&k2, &v2);
        let _ = table.put(&k3, &v3);

        let result = table.flush();
        assert!(result.is_ok());

        let sst = result.unwrap();
        assert_eq!(sst.blocks.len(), 1);
    }

    #[test]
    fn memtable_iter_iterates_through_all_elements_and_exhausts_itself() {
        let map: SkipMap<Bytes, Value> = SkipMap::new();
        map.insert(
            Bytes::from("key_1"),
            Value::Plain(Bytes::from("key_1_value")),
        );
        map.insert(
            Bytes::from("key_2"),
            Value::Plain(Bytes::from("key_2_value")),
        );
        map.insert(
            Bytes::from("key_3"),
            Value::Plain(Bytes::from("key_3_value")),
        );
        map.insert(Bytes::from("key_4"), Value::Tombstone);

        let mut iter = MemtableIterator::from_map(Arc::new(map));

        let first = Iterator::next(&mut iter).unwrap();
        assert_eq!(Bytes::from("key_1"), first.0);
        assert_eq!(Value::Plain(Bytes::from("key_1_value")), first.1);

        let second = Iterator::next(&mut iter).unwrap();
        assert_eq!(Bytes::from("key_2"), second.0);
        assert_eq!(Value::Plain(Bytes::from("key_2_value")), second.1);

        let third = Iterator::next(&mut iter).unwrap();
        assert_eq!(Bytes::from("key_3"), third.0);
        assert_eq!(Value::Plain(Bytes::from("key_3_value")), third.1);

        let fourth = Iterator::next(&mut iter).unwrap();
        assert_eq!(Bytes::from("key_4"), fourth.0);
        assert_eq!(Value::Tombstone, fourth.1);
    }

    #[test]
    fn memtable_iter_iteratres_over_provided_map_by_range() {
        let map = SkipMap::new();
        let lower = 4u8;
        let upper = 15u8;
        for v in 0u8..=30u8 {
            let key = Bytes::from(vec![v]);
            let value = Bytes::from(vec![v]);
            map.insert(key, Value::Plain(value));
        }

        let lower_bound = Bound::Included(Bytes::from(vec![lower]));
        let upper_bound = Bound::Included(Bytes::from(vec![upper]));

        let mut iter: MemtableIterator =
            MemtableIterator::range(Arc::new(map), lower_bound, upper_bound);
        for i in lower..=upper {
            match Iterator::next(&mut iter) {
                Some(item) => {
                    let expected_key = Bytes::from(vec![i]);
                    let expected_value = Bytes::from(vec![i]);
                    assert_eq!(expected_key, item.0.clone());
                    assert_eq!(Value::Plain(expected_value), item.1.clone());
                }
                None => panic!(""),
            }
        }
    }
}
