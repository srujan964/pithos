use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::{ops::Bound, path::PathBuf};

const STORAGE_DIR: &str = "/usr/local/lethe/data";
const WAL_DIR: &str = "wal";
const MAX_TABLE_SIZE: usize = 128 * 1024 * 1024;

pub(crate) struct TableOptions {
    max_size: usize,
}

impl TableOptions {
    fn new(max_size: usize) -> Self {
        Self { max_size }
    }
}

impl Default for TableOptions {
    fn default() -> Self {
        Self {
            max_size: MAX_TABLE_SIZE,
        }
    }
}

#[derive(Clone, Debug)]
enum Value {
    Plain(Bytes),
    Tombstone,
}

#[derive(Clone, Debug)]
pub(crate) struct Memtable {
    id: usize,
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
}

fn create_wal_dir(path: &Path) -> Result<(), MemError> {
    match std::fs::exists(path) {
        Ok(wal_dir_exists) if wal_dir_exists => Ok(()),
        Ok(_) => Ok(std::fs::create_dir_all(path)?),
        Err(e) => Err(MemError::WalInitFailure(e)),
    }
}

impl Memtable {
    pub(crate) fn create(id: usize, data_dir: &Path) -> Memtable {
        let _options = TableOptions::default();

        let mut wal_dir = PathBuf::from(data_dir);
        wal_dir.push(WAL_DIR);
        wal_dir.push(format!("{}", id));

        match create_wal_dir(&wal_dir) {
            Ok(_) => {}
            Err(e) => panic!("Failed to create memtable: {e}"),
        }

        Memtable {
            id,
            store: Arc::new(SkipMap::new()),
            size: Arc::new(0.into()),
            wal_dir,
        }
    }

    pub(crate) fn put(&self, key: &Bytes, value: &Bytes) -> Result<(), MemError> {
        let pair_size: usize = key.len() + value.len();
        self.size.fetch_add(pair_size, Ordering::Relaxed);
        self.store.insert(key.clone(), Value::Plain(value.clone()));
        Ok(())
    }

    pub(crate) fn get(&self, key: &Bytes) -> Option<Bytes> {
        self.store.get(key).and_then(|v| match v.value() {
            Value::Plain(val) => Some(val.clone()),
            Value::Tombstone => None,
        })
    }

    pub(crate) fn delete(&self, key: &Bytes) -> Result<(), MemError> {
        match self.get(key) {
            Some(_) => self.store.insert(key.clone(), Value::Tombstone),
            None => return Err(MemError::ValueTombstoned),
        };
        Ok(())
    }

    pub(crate) fn scan(&self, start: &Bytes, end: &Bytes) -> Result<Vec<(Bytes, Bytes)>, MemError> {
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

    pub(crate) fn freeze(&self) -> Result<(), MemError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;
    use temp_dir::TempDir;

    fn test_memtable() -> Memtable {
        let tempdir = TempDir::new();
        Memtable::create(1, tempdir.unwrap().path())
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
        let _ = Memtable::create(1, path);

        let mut expected_wal_dir = PathBuf::from(path);
        expected_wal_dir.push("wal/1/");

        let result = std::fs::exists(expected_wal_dir);
        assert!(result.is_ok());
        assert!(result.unwrap());
    }
}
