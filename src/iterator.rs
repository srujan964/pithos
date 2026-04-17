pub(crate) mod merge_iterator;

/// A small trait to manage iterator of storage buffers that yields ```(bytes::Bytes, memtable::Value)```.
pub(crate) trait StorageIter: Iterator {
    type KeyVal;

    fn next(&mut self) -> Option<Self::KeyVal>;
}
