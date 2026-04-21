use crate::iterator::merge_iterator::MergeIterator;
use crate::memtable::{self, Buffer, MemtableIterator, TableOptions};
use crate::sst::SSTable;

use arc_swap::ArcSwap;
use bytes::Bytes;

use std::collections::VecDeque;
use std::fmt::Debug;
use std::ops::Bound;
use std::path::PathBuf;
use std::sync::{
    Arc, Mutex, MutexGuard,
    atomic::{AtomicUsize, Ordering},
};
use std::vec;

pub(crate) const DATA_DIR: &str = "/usr/local/pithos/data";

#[derive(Clone, Debug)]
pub(crate) struct CoreOptions {
    pub(crate) data_dir: PathBuf,
    pub(crate) max_memtable_size: usize,
    pub(crate) memtable_limit: usize,
}

impl Default for CoreOptions {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from(DATA_DIR),
            max_memtable_size: memtable::MAX_TABLE_SIZE,
            memtable_limit: 3,
        }
    }
}

#[derive(Debug, Clone)]
struct State<B> {
    memtable: Arc<B>,
    frozen: VecDeque<Arc<B>>,
    level_zero: Vec<SSTable>,
}

impl<B: Buffer> State<B> {
    fn freeze(&self, _state_lock: &MutexGuard<'_, ()>, new_memtable: B) -> Self {
        let old = Arc::clone(&self.memtable);

        let mut frozen = self.frozen.clone();
        frozen.push_back(old);

        Self {
            memtable: Arc::new(new_memtable),
            frozen,
            level_zero: self.level_zero.clone(),
        }
    }

    fn flush_oldest(&self, sst: SSTable) -> Result<Self, OrchestrationError> {
        let mut level_zero = self.level_zero.clone();
        let mut frozen = self.frozen.clone();

        match frozen.pop_back() {
            Some(_) => {}
            None => {
                return Err(OrchestrationError::NothingToFlush);
            }
        }

        level_zero.insert(0, sst);
        let new = Self {
            memtable: self.memtable.clone(),
            frozen,
            level_zero,
        };
        Ok(new)
    }
}

#[derive(Debug)]
pub(crate) struct CoreStorage<B> {
    cur_memtable_id: AtomicUsize,
    state: Arc<ArcSwap<State<B>>>,
    freeze_lock: Mutex<()>,
    options: CoreOptions,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum OrchestrationError {
    #[error("Underlying storage system unavailable")]
    Unavailable,
    #[error("No frozen memtables found")]
    NothingToFlush,
    #[error("Failed to create SSTable")]
    FlushFailure,
}

impl<B: Buffer + Clone> CoreStorage<B> {
    pub(crate) fn new(options: Option<CoreOptions>) -> Self {
        let memtable_id = 1;
        let options = options.unwrap_or_default();
        let table_options = TableOptions::new(options.max_memtable_size, &options.data_dir);
        let memtable = B::create(memtable_id, Some(table_options));
        let state = State {
            memtable: Arc::new(memtable),
            frozen: VecDeque::new(),
            level_zero: vec![],
        };

        Self {
            cur_memtable_id: AtomicUsize::new(memtable_id),
            state: Arc::new(ArcSwap::new(Arc::new(state))),
            freeze_lock: Mutex::new(()),
            options: options.clone(),
        }
    }

    pub(crate) fn put(&self, key: Bytes, value: Bytes) -> Result<(), OrchestrationError> {
        let state = self.state.load();

        match state.memtable.put(&key, &value) {
            Ok(size) if size >= self.options.max_memtable_size => self.try_freeze(),
            Ok(_) => Ok(()),
            Err(_) => Err(OrchestrationError::Unavailable),
        }
    }

    pub(crate) fn get(&self, key: Bytes) -> Result<Bytes, OrchestrationError> {
        let state = self.state.load();

        if let Some(value) = state.memtable.get(&key) {
            return Ok(value);
        } else {
            for immutable_table in state.frozen.clone() {
                if let Some(value) = immutable_table.get(&key) {
                    return Ok(value);
                }
            }
        }

        Err(OrchestrationError::Unavailable)
    }

    pub(crate) fn delete(&self, key: Bytes) -> Result<(), OrchestrationError> {
        let state = self.state.load();

        state
            .memtable
            .delete(&key)
            .map_err(|_| OrchestrationError::Unavailable)
    }

    pub(crate) fn scan(&self, start: Bytes, end: Bytes) -> MergeIterator<MemtableIterator> {
        let state = self.state.load();
        let lower_bound = Bound::Included(start);
        let upper_bound = Bound::Included(end);
        let mut iters: VecDeque<MemtableIterator> = VecDeque::with_capacity(state.frozen.len() + 1);

        iters.push_back(
            state
                .memtable
                .scan(lower_bound.clone(), upper_bound.clone()),
        );

        for memtable in state.frozen.iter() {
            iters.push_back(memtable.scan(lower_bound.clone(), upper_bound.clone()));
        }

        MergeIterator::new(iters)
    }

    fn try_freeze(&self) -> Result<(), OrchestrationError> {
        let state_lock = self.freeze_lock.lock().unwrap();
        let state = self.state.load_full();

        if state.memtable.size() >= self.options.max_memtable_size {
            let memtable: B = B::create(
                self.next_memtable_id(),
                Some(TableOptions::new(
                    self.options.max_memtable_size,
                    &self.options.data_dir,
                )),
            );

            let new_state = self.state.load_full().freeze(&state_lock, memtable);
            self.state.store(Arc::new(new_state));
        }

        Ok(())
    }

    // Freeze current memtable and continously flush all frozen memtables to disk.
    pub(crate) fn force_flush_all(&self) -> Result<(), OrchestrationError> {
        let _ = self.try_freeze();

        let state_lock = self.freeze_lock.lock().unwrap();
        let mut state = self.state.load_full();
        while !state.frozen.is_empty() {
            match self.flush_oldest_memtable(&state_lock) {
                Ok(_) => {
                    state = self.state.load_full();
                }
                Err(_) => {
                    eprintln!("Error while flushing oldest memtable. It was possibly empty.");
                    break;
                }
            }
        }
        Ok(())
    }

    // Flush the last memtable in the frozen list and pop it.
    fn flush_oldest_memtable(
        &self,
        _state_lock: &MutexGuard<'_, ()>,
    ) -> Result<(), OrchestrationError> {
        let oldest;
        let state = self.state.load();
        match state.frozen.back() {
            Some(memtable) => oldest = memtable.clone(),
            None => {
                return Err(OrchestrationError::NothingToFlush);
            }
        }

        let sst = match oldest.flush() {
            Ok(sst) => sst,
            Err(_) => {
                return Err(OrchestrationError::FlushFailure);
            }
        };

        state.flush_oldest(sst).map(|updated| {
            self.state.swap(Arc::new(updated));
        })
    }

    fn next_memtable_id(&self) -> usize {
        self.cur_memtable_id.fetch_add(1, Ordering::SeqCst);
        self.cur_memtable_id.load(Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Bound;

    use super::*;
    use crate::memtable::{MemError, Memtable, Value};
    use bytes::Bytes;
    use temp_dir::TempDir;

    #[derive(Clone, Debug)]
    struct TestMemtable {
        size: usize,
    }

    const TEST_KEY: &str = "test_key";
    const TEST_VALUE: &str = "test_value";

    impl Buffer for TestMemtable {
        fn create(_id: usize, _options: Option<TableOptions>) -> Self {
            TestMemtable { size: 0 }
        }

        fn id(&self) -> usize {
            1
        }

        fn size(&self) -> usize {
            self.size
        }

        fn put(&self, key: &Bytes, value: &Bytes) -> Result<usize, MemError> {
            if key.eq(TEST_KEY) && value.eq(TEST_VALUE) {
                let total = key.len() + value.len();
                Ok(total)
            } else {
                Err(MemError::InvalidRange)
            }
        }

        fn delete(&self, key: &Bytes) -> Result<(), MemError> {
            if key.eq(TEST_KEY) {
                Ok(())
            } else {
                Err(MemError::InvalidRange)
            }
        }

        fn get(&self, key: &Bytes) -> Option<Bytes> {
            if key.eq(TEST_KEY) {
                Some(Bytes::from(TEST_VALUE))
            } else {
                None
            }
        }

        fn scan(&self, _start: Bound<Bytes>, _end: Bound<Bytes>) -> MemtableIterator {
            todo!()
        }

        fn flush(&self) -> Result<SSTable, MemError> {
            todo!()
        }
    }

    fn init_storage<B: Buffer + Clone>(size: usize, path: PathBuf) -> CoreStorage<B> {
        let options = CoreOptions {
            max_memtable_size: size,
            data_dir: path.to_path_buf(),
            memtable_limit: 3,
        };
        CoreStorage::new(Some(options))
    }

    #[test]
    fn storage_stores_key_value_pair_in_memtable() {
        let tempdir = TempDir::new().unwrap();
        let path = tempdir.path();
        let storage = init_storage::<TestMemtable>(memtable::MAX_TABLE_SIZE, path.to_path_buf());

        let result = storage.put(TEST_KEY.into(), TEST_VALUE.into());
        assert!(result.is_ok());
    }

    #[test]
    fn storage_retrieves_value_from_memtable() {
        let tempdir = TempDir::new().unwrap();
        let path = tempdir.path();
        let storage = init_storage::<TestMemtable>(memtable::MAX_TABLE_SIZE, path.to_path_buf());

        let result = storage.get(TEST_KEY.into());
        assert!(result.is_ok());
        let actual_value = result.unwrap();
        assert_eq!(actual_value, TEST_VALUE);
    }

    #[test]
    fn storage_deletes_value_from_memtable() {
        let tempdir = TempDir::new().unwrap();
        let path = tempdir.path();
        let storage = init_storage::<TestMemtable>(memtable::MAX_TABLE_SIZE, path.to_path_buf());

        let result = storage.delete(TEST_KEY.into());
        assert!(result.is_ok());
    }

    #[test]
    fn storage_freezes_memtable_after_capacity_exceeds() {
        #[derive(Clone)]
        struct SmallMemtable {}

        impl Buffer for SmallMemtable {
            fn create(_id: usize, _options: Option<TableOptions>) -> Self {
                SmallMemtable {}
            }

            fn id(&self) -> usize {
                1
            }

            fn size(&self) -> usize {
                8
            }

            fn put(&self, key: &Bytes, value: &Bytes) -> Result<usize, MemError> {
                if key.eq(TEST_KEY) && value.eq(TEST_VALUE) {
                    Ok(1024)
                } else {
                    Err(MemError::InvalidRange)
                }
            }

            fn delete(&self, _key: &Bytes) -> Result<(), MemError> {
                todo!()
            }

            fn get(&self, _key: &Bytes) -> Option<Bytes> {
                todo!()
            }

            fn scan(&self, _start: Bound<Bytes>, _end: Bound<Bytes>) -> MemtableIterator {
                todo!()
            }

            fn flush(&self) -> Result<SSTable, MemError> {
                todo!()
            }
        }

        let tempdir = TempDir::new().unwrap();
        let path = tempdir.path();
        let storage = init_storage::<SmallMemtable>(3, path.to_path_buf());

        let result = storage.put(TEST_KEY.into(), TEST_VALUE.into());
        assert!(result.is_ok());
    }

    fn test_memtable(id: usize, path: PathBuf) -> Memtable {
        let options = TableOptions {
            data_dir: path,
            max_size: 256,
        };
        Memtable::create(id, Some(options))
    }

    #[test]
    fn force_flush_memtable_empties_frozen_list_and_updates_level_zero() {
        let tempdir = TempDir::new().unwrap();
        let path = tempdir.path();
        let memtable = test_memtable(3, path.to_path_buf());
        let frozen_table_one = test_memtable(1, path.to_path_buf());
        let frozen_table_two = test_memtable(2, path.to_path_buf());

        let key = Bytes::from("Key");
        let value = Bytes::from("Value");

        let _ = frozen_table_one.put(&key, &value);
        let _ = frozen_table_one.delete(&key);

        let _ = frozen_table_two.put(&key, &value);
        let _ = frozen_table_two.delete(&key);

        let _ = memtable.put(&key, &value);
        let _ = memtable.delete(&key);

        let mut q = VecDeque::new();
        q.push_back(Arc::new(frozen_table_one));
        q.push_back(Arc::new(frozen_table_two));
        let state = State {
            memtable: Arc::new(memtable),
            frozen: q,
            level_zero: vec![],
        };

        let tempdir = TempDir::new().unwrap();
        let storage = CoreStorage {
            cur_memtable_id: AtomicUsize::new(1),
            state: Arc::new(ArcSwap::new(Arc::new(state))),
            freeze_lock: Mutex::new(()),
            options: CoreOptions {
                data_dir: tempdir.path().to_path_buf(),
                max_memtable_size: 4,
                memtable_limit: 3,
            },
        };

        let result = storage.force_flush_all();
        assert!(result.is_ok());

        let state = storage.state.load_full();
        let frozen_tables = state.frozen.clone();
        let l0_sstables = state.level_zero.clone();

        assert!(frozen_tables.is_empty());
        assert_eq!(l0_sstables.len(), 3);
    }

    #[test]
    fn scan_creates_an_iterator_over_memtables() {
        let tempdir = TempDir::new().unwrap();
        let path = tempdir.path();
        let memtable = test_memtable(3, path.to_path_buf());
        let intermediate_table = test_memtable(2, path.to_path_buf());
        let oldest_table = test_memtable(1, path.to_path_buf());

        let key = Bytes::from("key_1");
        let key_2 = Bytes::from("key_2");
        let key_3 = Bytes::from("key_3");
        let value_1 = Bytes::from("value_1");
        let value_2 = Bytes::from("value_2");
        let value_3 = Bytes::from("value_3");

        let _ = oldest_table.put(&key, &value_1);
        let _ = oldest_table.put(&key_2, &value_2);

        let _ = intermediate_table.put(&key, &value_2);
        let _ = intermediate_table.put(&key_3, &value_2);
        let _ = intermediate_table.delete(&key);

        let _ = memtable.put(&key, &value_3);
        let _ = memtable.put(&key_3, &value_1);

        let mut q = VecDeque::new();
        q.push_back(Arc::new(intermediate_table));
        q.push_back(Arc::new(oldest_table));
        let state = State {
            memtable: Arc::new(memtable),
            frozen: q,
            level_zero: vec![],
        };

        let tempdir = TempDir::new().unwrap();
        let storage = CoreStorage {
            cur_memtable_id: AtomicUsize::new(1),
            state: Arc::new(ArcSwap::new(Arc::new(state))),
            freeze_lock: Mutex::new(()),
            options: CoreOptions {
                data_dir: tempdir.path().to_path_buf(),
                max_memtable_size: 4,
                memtable_limit: 3,
            },
        };

        let mut iter = storage.scan(key.clone(), key_3.clone());

        assert_eq!(iter.next().unwrap(), (key, Value::Plain(value_3)));
        assert_eq!(iter.next().unwrap(), (key_2, Value::Plain(value_2)));
        assert_eq!(iter.next().unwrap(), (key_3, Value::Plain(value_1)));
    }
}
