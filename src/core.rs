use crate::memtable::{self};
use crate::memtable::{Buffer, TableOptions};
use bytes::Bytes;
use std::fmt::Debug;
use std::path::PathBuf;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::sync::{Mutex, RwLock};
use std::vec;

pub(crate) const DATA_DIR: &str = "/usr/local/pithos/data";

#[derive(Clone, Debug)]
pub(crate) struct CoreOptions {
    pub(crate) data_dir: PathBuf,
    pub(crate) max_memtable_size: usize,
}

impl Default for CoreOptions {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from(DATA_DIR),
            max_memtable_size: memtable::MAX_TABLE_SIZE,
        }
    }
}

#[derive(Debug, Clone)]
struct State<B> {
    memtable: Arc<B>,
    frozen: Vec<Arc<B>>,
}

#[derive(Debug)]
pub(crate) struct CoreStorage<B> {
    cur_memtable_id: AtomicUsize,
    state: Arc<RwLock<Arc<State<B>>>>,
    freeze_lock: Mutex<()>,
    options: CoreOptions,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum OrchestrationError {
    #[error("Underlying storage system unavailable")]
    Unavailable,
}

impl<B: Buffer + Clone> CoreStorage<B> {
    pub(crate) fn new(options: Option<CoreOptions>) -> Self {
        let memtable_id = 1;
        let options = options.unwrap_or_default();
        let table_options = TableOptions::new(options.max_memtable_size, &options.data_dir);
        let memtable = B::create(memtable_id, Some(table_options));
        let state = State {
            memtable: Arc::new(memtable),
            frozen: vec![],
        };

        Self {
            cur_memtable_id: AtomicUsize::new(memtable_id),
            state: Arc::new(RwLock::new(Arc::new(state))),
            freeze_lock: Mutex::new(()),
            options: options.clone(),
        }
    }

    pub(crate) fn put(&self, key: Bytes, value: Bytes) -> Result<(), OrchestrationError> {
        let guard = self.state.read().unwrap();

        match guard.memtable.put(&key, &value) {
            Ok(size) if size >= self.options.max_memtable_size => {
                drop(guard);
                self.try_freeze_memtable()
            }
            Ok(_) => Ok(()),
            Err(_) => Err(OrchestrationError::Unavailable),
        }
    }

    pub(crate) fn get(&self, key: Bytes) -> Result<Bytes, OrchestrationError> {
        let guard = self.state.read().unwrap();

        if let Some(value) = guard.memtable.get(&key) {
            return Ok(value);
        } else {
            for immutable_table in guard.frozen.clone() {
                if let Some(value) = immutable_table.get(&key) {
                    return Ok(value);
                }
            }
        }

        Err(OrchestrationError::Unavailable)
    }

    pub(crate) fn delete(&self, key: Bytes) -> Result<(), OrchestrationError> {
        let guard = self.state.read().unwrap();
        guard
            .memtable
            .delete(&key)
            .map_err(|_| OrchestrationError::Unavailable)
    }

    // Check if the current memtable has exceeded max capacity. If so, freeze it
    // and add it to the frozen list.
    fn try_freeze_memtable(&self) -> Result<(), OrchestrationError> {
        let _guard = self.freeze_lock.lock().unwrap();
        let read_state = self.state.read().unwrap();

        // if memetable was frozen already, do nothing
        if read_state.memtable.size() >= self.options.max_memtable_size {
            drop(read_state);

            // Create the new memtable before acquiring a write lock on the storage state.
            // Then we can swap the current memtable with a new one.
            let memtable: Arc<B> = Arc::new(B::create(
                self.next_memtable_id(),
                Some(TableOptions::new(
                    self.options.max_memtable_size,
                    &self.options.data_dir,
                )),
            ));

            {
                let mut write_state = self.state.write().unwrap();

                // Create a clone of current storage state
                let mut snapshot = write_state.as_ref().clone();

                // Replace current memtable with new one and add it to the immutable list
                let old = std::mem::replace(&mut snapshot.memtable, memtable);
                snapshot.frozen.insert(0, old.clone());

                *write_state = Arc::new(snapshot);
            }
        }

        Ok(())
    }

    fn next_memtable_id(&self) -> usize {
        self.cur_memtable_id.fetch_add(1, Ordering::SeqCst);
        self.cur_memtable_id.load(Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{memtable::MemError, sst::SSTable};
    use bytes::Bytes;
    use std::path::PathBuf;

    #[derive(Clone, Debug)]
    struct TestMemtable {
        size: usize,
    }

    const TEST_KEY: &str = "test_key";
    const TEST_VALUE: &str = "test_value";
    const TEST_KEY_2: &str = "test_key_2";
    const TEST_VALUE_2: &str = "test_value_2";

    impl Buffer for TestMemtable {
        fn create(_id: usize, _options: Option<TableOptions>) -> Self {
            TestMemtable { size: 0 }
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

        fn scan(&self, start: &Bytes, end: &Bytes) -> Result<Vec<(Bytes, Bytes)>, MemError> {
            if start.eq(TEST_KEY) && end.eq(TEST_KEY_2) {
                Ok(vec![
                    (Bytes::from(TEST_KEY), Bytes::from(TEST_VALUE)),
                    (Bytes::from(TEST_KEY_2), Bytes::from(TEST_VALUE_2)),
                ])
            } else {
                Err(MemError::InvalidRange)
            }
        }

        fn freeze(&self) -> Result<SSTable, MemError> {
            todo!()
        }
    }

    fn init_storage<B: Buffer + Clone>(size: usize) -> CoreStorage<B> {
        let path = PathBuf::from("stub");
        let options = CoreOptions {
            max_memtable_size: size,
            data_dir: path,
        };
        CoreStorage::new(Some(options))
    }

    #[test]
    fn storage_stores_key_value_pair_in_memtable() {
        let storage = init_storage::<TestMemtable>(memtable::MAX_TABLE_SIZE);

        let result = storage.put(TEST_KEY.into(), TEST_VALUE.into());
        assert!(result.is_ok());
    }

    #[test]
    fn storage_retrieves_value_from_memtable() {
        let storage = init_storage::<TestMemtable>(memtable::MAX_TABLE_SIZE);

        let result = storage.get(TEST_KEY.into());
        assert!(result.is_ok());
        let actual_value = result.unwrap();
        assert_eq!(actual_value, TEST_VALUE);
    }

    #[test]
    fn storage_deletes_value_from_memtable() {
        let storage = init_storage::<TestMemtable>(memtable::MAX_TABLE_SIZE);

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

            fn scan(&self, _start: &Bytes, _end: &Bytes) -> Result<Vec<(Bytes, Bytes)>, MemError> {
                todo!()
            }

            fn freeze(&self) -> Result<SSTable, MemError> {
                todo!()
            }
        }

        let storage = init_storage::<SmallMemtable>(3);

        let result = storage.put(TEST_KEY.into(), TEST_VALUE.into());
        assert!(result.is_ok());
    }
}
