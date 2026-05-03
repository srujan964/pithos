use bytes::Bytes;

use crate::{
    iterator::merge_iterator::{MergeIterator, MultiMergeIterator},
    memtable::MemtableIterator,
    sst::iterator::{ConcatenatingIterator, SSTableIterator},
    types::{Pair, Value},
};

pub(crate) mod merge_iterator;

/// A small trait to manage iterator of storage buffers that yields ```(bytes::Bytes, memtable::Value)```.
pub(crate) trait StorageIter: Iterator<Item = Self::KeyVal> {
    type KeyVal;

    fn next(&mut self) -> Option<Self::KeyVal>;
}

type FullIterator = MultiMergeIterator<
    MultiMergeIterator<MergeIterator<MemtableIterator>, MergeIterator<SSTableIterator>>,
    MergeIterator<ConcatenatingIterator>,
>;

pub(crate) struct CombinedIterator {
    inner: FullIterator,
    end_bound: Bytes,
    exhausted: bool,
}

impl CombinedIterator {
    pub(crate) fn new(inner: FullIterator, end: &Bytes) -> CombinedIterator {
        CombinedIterator {
            inner,
            end_bound: end.clone(),
            exhausted: false,
        }
    }
}

impl Iterator for CombinedIterator {
    type Item = Pair;

    fn next(&mut self) -> Option<Self::Item> {
        StorageIter::next(self)
    }
}

impl StorageIter for CombinedIterator {
    type KeyVal = Pair;

    fn next(&mut self) -> Option<Self::KeyVal> {
        if self.exhausted {
            return None;
        }

        loop {
            match std::iter::Iterator::next(&mut self.inner) {
                None => return None,
                Some((_, Value::Tombstone)) => continue,
                Some((key, value)) => {
                    if key > self.end_bound {
                        self.exhausted = true;
                        return None;
                    }
                    if key == self.end_bound {
                        self.exhausted = true;
                    }

                    return Some((key, value));
                }
            }
        }
    }
}
