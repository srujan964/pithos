use crate::index::IndexBuilder;
use crate::sst::block::{Block, BlockBuilder};
use crate::sst::{self, SSTable};
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use crossbeam_skiplist::map::Iter;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::{ops::Bound, path::PathBuf};

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

#[derive(Clone, Debug)]
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

    fn scan(&self, start: &Bytes, end: &Bytes) -> Result<Vec<(Bytes, Bytes)>, MemError>;

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
        self.store.get(key).and_then(|v| match v.value() {
            Value::Plain(val) => Some(val.clone()),
            Value::Tombstone => None,
        })
    }

    fn delete(&self, key: &Bytes) -> Result<(), MemError> {
        match self.get(key) {
            Some(_) => {
                self.size.fetch_add(key.len(), Ordering::Relaxed);
                self.store.insert(key.clone(), Value::Tombstone);
            }
            None => return Err(MemError::ValueTombstoned),
        };
        Ok(())
    }

    fn scan(&self, start: &Bytes, end: &Bytes) -> Result<Vec<(Bytes, Bytes)>, MemError> {
        if start > end {
            return Err(MemError::InvalidRange);
        }

        Ok(self
            .store
            .range::<Bytes, (Bound<&Bytes>, Bound<&Bytes>)>((
                Bound::Included(start),
                Bound::Included(end),
            ))
            .filter_map(|entry| match entry.value() {
                Value::Plain(value) => Some((entry.key().clone(), value.clone())),
                Value::Tombstone => None,
            })
            .collect())
    }

    fn flush(&self) -> Result<SSTable, MemError> {
        build_sstable(self.store.iter(), sst::BLOCK_SIZE)
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
    let blocks: Vec<Block> = block_builder.build();

    let index_start_offset = blocks.iter().map(|b| b.size()).sum::<usize>() as u64;
    let index = index_builder.build(index_start_offset);

    SSTable::new(&blocks, &index, sst::MAX_TABLE_SIZE).map_err(|_| MemError::FreezeFailure)
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
    fn memtable_returns_vec_of_key_value_pairs_when_scanned() {
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

        let result = table.scan(&k2, &k4);
        assert!(result.is_ok());

        let pairs = result.unwrap();
        let mut iter = pairs.iter();
        assert_eq!(iter.next(), Some(&(k2, v2)));
        assert_eq!(iter.next(), Some(&(k3, v3)));
        assert_eq!(iter.next(), Some(&(k4, v4)));
    }

    #[test]
    fn memtable_returns_err_if_scan_end_lesser_than_start() {
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

        let result = table.scan(&k3, &k1);
        assert!(result.is_err());
    }

    #[test]
    fn memtable_returns_empty_vec_if_unseen_keys_scanned() {
        let table = test_memtable();
        let k1 = Bytes::from("1");
        let v1 = Bytes::from("123");

        let k2 = Bytes::from("2");
        let v2 = Bytes::from("456");

        let _ = table.put(&k1, &v1);
        let _ = table.put(&k2, &v2);

        let result = table.scan(&Bytes::from("s1"), &Bytes::from("s2"));
        assert!(result.is_ok());

        let pairs = result.unwrap();
        assert!(pairs.is_empty());
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
}
