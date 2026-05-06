use crate::block;
use crate::iterator::StorageIter;
use crate::sst::{self, SSTable};
use crate::types::Value;
use crate::wal::{Op, WalError, WalWriter};

use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use crossbeam_skiplist::map::Range;
use ouroboros::self_referencing;

use std::ops::Bound;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

pub(crate) const WAL_DIR: &str = "wal";
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

#[derive(Debug, Clone)]
pub(crate) struct Memtable {
    pub(crate) id: usize,
    max_size: usize,
    size: Arc<AtomicUsize>,
    pub(crate) store: Arc<SkipMap<Bytes, Value>>,
    options: TableOptions,
    wal_dir: PathBuf,
    wal_writer: WalWriter,
}

#[derive(thiserror::Error, Debug)]
pub(crate) enum MemError {
    #[error("WAL directory initialization failure")]
    WalInitFailure(#[from] std::io::Error),
    #[error("Unable to log operation to WAL")]
    WalWriteFailure(#[from] WalError),
    #[error("Invalid range key")]
    InvalidRange,
    #[error("Unable to freeze memtable")]
    FreezeFailure,
    #[error("Memtable size limit exceeded")]
    MaxSizeExceeded,
    #[error("Failed to delete WAL directory")]
    WalCleanupFailure,
}

fn create_wal_dir(path: &Path) -> Result<(), MemError> {
    match std::fs::exists(path) {
        Ok(wal_dir_exists) if wal_dir_exists => Ok(()),
        Ok(_) => {
            std::fs::create_dir_all(path)?;
            // Fsync parent so the new directory entry is durable.
            if let Some(parent) = path.parent() {
                let _ = std::fs::File::open(parent).and_then(|d| d.sync_all());
            }
            Ok(())
        }
        Err(e) => Err(MemError::WalInitFailure(e)),
    }
}

impl Memtable {
    pub(crate) fn wal_dir(&self) -> &Path {
        &self.wal_dir
    }

    fn remove_wal_files(&self) -> Result<(), MemError> {
        std::fs::remove_dir_all(&self.wal_dir).map_err(|_| MemError::WalCleanupFailure)
    }
}

pub(crate) trait Buffer {
    fn create(id: usize, options: Option<TableOptions>) -> Self;

    fn id(&self) -> usize;

    fn size(&self) -> usize;

    fn put(&self, key: &Bytes, value: &Bytes) -> Result<usize, MemError>;

    fn delete(&self, key: &Bytes) -> Result<(), MemError>;

    fn get(&self, key: &Bytes) -> Option<Bytes>;

    fn scan(&self, start: Bound<Bytes>, end: Bound<Bytes>) -> MemtableIterator;

    fn flush(&self, path: PathBuf) -> Result<SSTable, MemError>;
}

impl Buffer for Memtable {
    fn create(id: usize, options: Option<TableOptions>) -> Self {
        let options = options.unwrap_or_default();

        let mut wal_dir = options.data_dir.clone();
        wal_dir.push(WAL_DIR);
        wal_dir.push(format!("{}", id));

        match create_wal_dir(&wal_dir) {
            Ok(_) => {}
            Err(e) => panic!("Failed to create memtable: {e}"),
        }

        let writer = match WalWriter::open(&wal_dir) {
            Ok(wal_writer) => wal_writer,
            Err(e) => panic!("Failed to create WAL writer for memtable: {e}"),
        };

        Memtable {
            id,
            max_size: options.max_size,
            store: Arc::new(SkipMap::new()),
            size: Arc::new(0.into()),
            options: options.clone(),
            wal_dir,
            wal_writer: writer,
        }
    }

    fn id(&self) -> usize {
        self.id
    }

    fn size(&self) -> usize {
        self.size.load(Ordering::Relaxed)
    }

    fn put(&self, key: &Bytes, value: &Bytes) -> Result<usize, MemError> {
        let op = Op::Put(key.to_vec(), value.to_vec());
        self.wal_writer.write(&op)?;

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
        let op = Op::Delete(key.to_vec());
        self.wal_writer.write(&op)?;

        self.size.fetch_add(key.len(), Ordering::Relaxed);
        self.store.insert(key.clone(), Value::Tombstone);
        Ok(())
    }

    fn scan(&self, start: Bound<Bytes>, end: Bound<Bytes>) -> MemtableIterator {
        MemtableIterator::range(self.store.clone(), start, end)
    }

    fn flush(&self, path: PathBuf) -> Result<SSTable, MemError> {
        let iter = MemtableIterator::from_map(self.store.clone());
        sst::build_sstable(self.id, iter, block::BLOCK_SIZE, path, false)
            .map_err(|_| MemError::FreezeFailure)
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

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;
    use temp_dir::TempDir;

    fn test_memtable(path: PathBuf) -> Memtable {
        let options = TableOptions {
            data_dir: path,
            max_size: 256,
        };
        Memtable::create(1, Some(options))
    }

    #[test]
    fn memtable_stores_and_retrieves_key_value_pairs() {
        let tempdir = TempDir::new().unwrap();
        let path = tempdir.path();
        let table = test_memtable(path.to_path_buf());
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
        let tempdir = TempDir::new().unwrap();
        let path = tempdir.path();
        let table = test_memtable(path.to_path_buf());
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
        let tempdir = TempDir::new().unwrap();
        let path = tempdir.path();
        let table = test_memtable(path.to_path_buf());
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
