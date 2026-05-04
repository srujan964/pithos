use crate::compaction::{CompactionOptions, level::LeveledCompactionOptions};
use crate::iterator::CombinedIterator;
use crate::iterator::merge_iterator::{MergeIterator, MultiMergeIterator};
use crate::manifest::{MANIFEST_FILE, Manifest, ManifestRecord};
use crate::memtable::{self, Buffer, MemtableIterator, TableOptions};
use crate::sst::iterator::{ConcatenatingIterator, SSTableIterator};
use crate::sst::{SST_FILE_PREFIX, SSTable};

use arc_swap::ArcSwap;
use bytes::Bytes;

use std::any::Any;
use std::collections::{HashMap, VecDeque};
use std::fmt::Debug;
use std::fs::{File, OpenOptions, create_dir};
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::{
    Arc, Mutex, MutexGuard,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;
use std::vec;

pub(crate) const DATA_DIR: &str = "/usr/local/pithos/data";

#[derive(Clone, Debug)]
pub(crate) struct CoreOptions {
    pub(crate) data_dir: PathBuf,
    pub(crate) max_memtable_size: usize,
    pub(crate) memtable_limit: usize,
    pub(crate) compaction_opts: CompactionOptions,
}

impl Default for CoreOptions {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from(DATA_DIR),
            max_memtable_size: memtable::MAX_TABLE_SIZE,
            memtable_limit: 3,
            compaction_opts: CompactionOptions::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct State<B> {
    pub(crate) memtable: Arc<B>,
    pub(crate) frozen: VecDeque<Arc<B>>,
    pub(crate) level_zero: Vec<usize>,
    pub(crate) levels: Vec<(usize, Vec<usize>)>,
    pub(crate) sstables: HashMap<usize, Arc<SSTable>>,
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
            levels: self.levels.clone(),
            sstables: self.sstables.clone(),
        }
    }

    fn flush_oldest(&self, sst: SSTable) -> Result<Self, OrchestrationError> {
        let mut level_zero = self.level_zero.clone();
        let mut frozen = self.frozen.clone();
        let mut sstables = self.sstables.clone();

        match frozen.pop_front() {
            Some(_) => {}
            None => {
                return Err(OrchestrationError::NothingToFlush);
            }
        }

        level_zero.insert(0, sst.id());
        sstables.insert(sst.id(), Arc::new(sst));

        let new = Self {
            memtable: self.memtable.clone(),
            frozen,
            level_zero,
            levels: self.levels.clone(),
            sstables,
        };
        Ok(new)
    }
}

#[derive(Debug)]
pub(crate) struct CoreStorage<B> {
    inner: Arc<CoreStorageInner<B>>,
    compaction_notifier: crossbeam_channel::Sender<()>,
    flush_notifier: crossbeam_channel::Sender<()>,
    compaction_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    flush_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl<B> CoreStorage<B>
where
    B: Buffer + Clone + Send + Sync + 'static,
{
    pub(crate) fn open(options: Option<CoreOptions>) -> Result<Arc<Self>, OrchestrationError> {
        let inner = Arc::new(CoreStorageInner::open(options));

        let (comp_tx, comp_rx) = crossbeam_channel::unbounded();
        let (flush_tx, flush_rx) = crossbeam_channel::unbounded();

        let compaction_thread = inner.spawn_compaction_thread(comp_rx)?;
        let flush_thread = inner.spawn_flush_thread(flush_rx)?;

        Ok(Arc::new(CoreStorage {
            inner,
            compaction_notifier: comp_tx,
            flush_notifier: flush_tx,
            compaction_thread: Mutex::new(compaction_thread),
            flush_thread: Mutex::new(flush_thread),
        }))
    }

    pub(crate) fn close(&self) -> Result<(), OrchestrationError> {
        self.inner.sync()?;

        self.compaction_notifier.send(()).ok();
        self.flush_notifier.send(()).ok();

        let mut compaction_thread = self.compaction_thread.lock().unwrap();
        if let Some(compaction_thread) = compaction_thread.take() {
            compaction_thread
                .join()
                .map_err(|e| OrchestrationError::CloseError(e))?;
        }

        let mut flush_thread = self.flush_thread.lock().unwrap();
        if let Some(flush_thread) = flush_thread.take() {
            flush_thread
                .join()
                .map_err(|e| OrchestrationError::CloseError(e))?;
        }

        self.inner.force_flush_all()?;
        self.inner.sync()?;

        Ok(())
    }

    pub(crate) fn get(&self, key: Bytes) -> Result<Bytes, OrchestrationError> {
        self.inner.get(key)
    }

    pub(crate) fn put(&self, key: Bytes, value: Bytes) -> Result<(), OrchestrationError> {
        self.inner.put(key, value)
    }

    pub(crate) fn delete(&self, key: Bytes) -> Result<(), OrchestrationError> {
        self.inner.delete(key)
    }

    pub(crate) fn scan(
        &self,
        start: Bytes,
        end: Bytes,
    ) -> Result<CombinedIterator, OrchestrationError> {
        self.inner.scan(start, end)
    }
}

#[derive(Debug)]
pub(crate) struct CoreStorageInner<B> {
    cur_memtable_id: AtomicUsize,
    pub(crate) state: Arc<ArcSwap<State<B>>>,
    freeze_lock: Mutex<()>,
    manifest: Manifest,
    pub(crate) options: CoreOptions,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum OrchestrationError {
    #[error("Data directory unavailable")]
    MissingDataDir,
    #[error("Underlying storage system unavailable")]
    Unavailable,
    #[error("No frozen memtables found")]
    NothingToFlush,
    #[error("Failed to create SSTable")]
    FlushFailure,
    #[error("Compaction failed")]
    CompactionFailure,
    #[error("Error closing thread")]
    CloseError(Box<dyn Any + Send + 'static>),
}

impl<B> CoreStorageInner<B>
where
    B: Buffer + Clone + Send + Sync + 'static,
{
    pub(crate) fn open(options: Option<CoreOptions>) -> Self {
        let mut memtable_id = 1;
        let options = options.unwrap_or_default();
        let table_options = TableOptions::new(options.max_memtable_size, &options.data_dir);
        let memtable = B::create(memtable_id, Some(table_options));
        let mut state = State {
            memtable: Arc::new(memtable),
            frozen: VecDeque::new(),
            level_zero: vec![],
            levels: match options.compaction_opts {
                CompactionOptions::Leveled(LeveledCompactionOptions { max_levels, .. }) => {
                    (0..max_levels).map(|l| (l, vec![])).collect()
                }
            },
            sstables: HashMap::new(),
        };

        let manifest = if options.data_dir.join(MANIFEST_FILE).exists() {
            match Manifest::recover(&options.data_dir) {
                Ok((manifest, records)) => {
                    let mut memtables: BTreeSet<usize> = BTreeSet::new();

                    for record in records {
                        match record {
                            ManifestRecord::Flush(sst_id) => {
                                memtables.remove(&sst_id);
                                state.level_zero.insert(0, sst_id);
                                memtable_id = memtable_id.max(sst_id);
                            }
                            ManifestRecord::NewMemtable(id) => {
                                memtable_id = memtable_id.max(id);
                                memtables.insert(id);
                            }
                            ManifestRecord::CompactionResult(compaction_task, output) => {
                                let (new_state, _) = Self::apply_compaction_result(
                                    &state,
                                    &compaction_task,
                                    &output,
                                );

                                state = new_state;
                                memtable_id =
                                    memtable_id.max(*output.iter().max().unwrap_or(&1usize));
                            }
                        }
                    }

                    manifest
                }
                Err(_) => create_fresh_manifest(&options.data_dir),
            }
        } else {
            create_fresh_manifest(&options.data_dir)
        };

        Self::open_all_ssts(&mut state, &options);

        match manifest.add_record(ManifestRecord::NewMemtable(memtable_id)) {
            Ok(_) => {}
            Err(e) => eprintln!("Failed to write record to manifest: {e}"),
        };

        Self {
            cur_memtable_id: AtomicUsize::new(memtable_id),
            state: Arc::new(ArcSwap::new(Arc::new(state))),
            manifest,
            freeze_lock: Mutex::new(()),
            options: options.clone(),
        }
    }

    pub(crate) fn sync(&self) -> Result<(), OrchestrationError> {
        match File::open(self.options.data_dir.clone()) {
            Ok(dir) => {
                let _ = dir.sync_all();
            }
            Err(_) => return Err(OrchestrationError::Unavailable),
        }

        Ok(())
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
            Ok(value)
        } else {
            for immutable_table in state.frozen.clone() {
                if let Some(value) = immutable_table.get(&key) {
                    return Ok(value);
                }
            }

            // Look through all L0 files
            for id in &state.level_zero {
                if let Some(sst) = state.sstables.get(id) {
                    if sst.probe(&key) {
                        if let Ok(v) = sst.fetch(&key) {
                            return Ok(v);
                        }
                    }
                }
            }

            // Look through L1+ files
            for (_, level_sst_ids) in &state.levels {
                for id in level_sst_ids {
                    if let Some(sst) = state.sstables.get(id) {
                        if !sst.contains(&key) {
                            continue;
                        }
                        if sst.probe(&key) {
                            if let Ok(v) = sst.fetch(&key) {
                                return Ok(v);
                            }
                        }
                        break;
                    }
                }
            }

            Err(OrchestrationError::Unavailable)
        }
    }

    pub(crate) fn delete(&self, key: Bytes) -> Result<(), OrchestrationError> {
        let state = self.state.load();

        state
            .memtable
            .delete(&key)
            .map_err(|_| OrchestrationError::Unavailable)
    }

    pub(crate) fn scan(
        &self,
        start: Bytes,
        end: Bytes,
    ) -> Result<CombinedIterator, OrchestrationError> {
        let state = self.state.load();
        let lower_bound = Bound::Included(start.clone());
        let upper_bound = Bound::Included(end.clone());
        let mut iters: VecDeque<MemtableIterator> = VecDeque::with_capacity(state.frozen.len() + 1);

        iters.push_back(
            state
                .memtable
                .scan(lower_bound.clone(), upper_bound.clone()),
        );

        for memtable in state.frozen.iter() {
            iters.push_back(memtable.scan(lower_bound.clone(), upper_bound.clone()));
        }

        let memtable_iter = MergeIterator::new(iters);

        let mut sst_iters = VecDeque::new();
        for id in state.level_zero.iter() {
            let l0_sst = state.sstables[id].clone();
            if !should_traverse(l0_sst.first_key(), l0_sst.last_key(), &start, &end) {
                continue;
            }

            if let Ok(iter) = SSTableIterator::new_and_seek_to_key(l0_sst, &start) {
                sst_iters.push_back(iter);
            }
        }

        let l0_iter = MergeIterator::new(sst_iters);

        let mut level_n_iters = VecDeque::with_capacity(state.levels.len());
        for (_, level_sst_ids) in &state.levels {
            let mut level_ssts = Vec::with_capacity(level_sst_ids.len());
            for id in level_sst_ids {
                let sst = state.sstables[id].clone();
                if should_traverse(sst.first_key(), sst.last_key(), &start, &end) {
                    level_ssts.push(sst);
                }
            }

            if let Ok(level_iter) = ConcatenatingIterator::new_and_seek_to_key(level_ssts, &start) {
                level_n_iters.push_back(level_iter);
            };
        }

        let iter = MultiMergeIterator::new(memtable_iter, l0_iter);
        let iter = MultiMergeIterator::new(iter, MergeIterator::new(level_n_iters));

        Ok(CombinedIterator::new(iter, &end))
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
            let new_memtable_id = memtable.id();

            let new_state = self.state.load_full().freeze(&state_lock, memtable);
            match self
                .manifest
                .add_record(ManifestRecord::NewMemtable(new_memtable_id))
            {
                Ok(_) => {}
                Err(e) => eprintln!("Failed to write new memtable record into manifest: {e}"),
            };
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
        match state.frozen.front() {
            Some(memtable) => oldest = memtable.clone(),
            None => {
                return Err(OrchestrationError::NothingToFlush);
            }
        }

        let sst = match oldest.flush(self.get_sst_path(oldest.id())) {
            Ok(sst) => sst,
            Err(e) => {
                eprintln!("Failed to flush: {}", e);
                return Err(OrchestrationError::FlushFailure);
            }
        };

        match self.manifest.add_record(ManifestRecord::Flush(sst.id())) {
            Ok(_) => {}
            Err(e) => eprintln!("Failed to write new memtable flush to manifest: {e}"),
        };

        state.flush_oldest(sst).map(|updated| {
            self.state.swap(Arc::new(updated));
        })
    }

    pub(crate) fn trigger_compact(&self) -> Result<(), OrchestrationError> {
        let state = self.state.load_full();

        let task = self.generate_compaction_task(&state);

        let Some(task) = task else {
            return Ok(());
        };

        let sstables = self.compact(&task)?;
        let new_files = sstables.len();
        let new_sst_ids: Vec<usize> = sstables.iter().map(|sst| sst.id()).collect();
        let ssts_to_remove = {
            let _state_lock = self.freeze_lock.lock().unwrap();
            let (mut new_state, files_to_remove) =
                Self::apply_compaction_result(&self.state.load_full(), &task, &new_sst_ids);

            let mut ssts_to_remove = Vec::with_capacity(files_to_remove.len());

            for file in &files_to_remove {
                let removed = new_state.sstables.remove(file);
                if let Some(id) = removed {
                    ssts_to_remove.push(id);
                };
            }

            let mut new_sst_ids = Vec::new();
            for file_to_add in sstables {
                new_sst_ids.push(file_to_add.id());
                new_state.sstables.insert(file_to_add.id(), file_to_add);
            }
            self.state.swap(Arc::new(new_state));
            let manifest_record = ManifestRecord::CompactionResult(task, new_sst_ids);

            match self.manifest.add_record(manifest_record) {
                Ok(_) => {}
                Err(e) => eprintln!("Failed to write compaction info to manifest: {e}"),
            };

            ssts_to_remove
        };
        println!(
            "Compaction completed. {} files removed. {} files added.",
            ssts_to_remove.len(),
            new_files
        );

        for sst in ssts_to_remove {
            let _ = std::fs::remove_file(self.get_sst_path(sst.id()));
        }

        Ok(())
    }

    fn trigger_flush(&self) -> Result<(), OrchestrationError> {
        let state = self.state.load();

        if state.frozen.len() >= self.options.memtable_limit {
            let state_lock = self.freeze_lock.lock().unwrap();
            match self.flush_oldest_memtable(&state_lock) {
                Ok(_) => {}
                Err(e) => {
                    eprintln!("Error during flush: {}", e);
                    drop(state_lock);
                }
            }
        }

        Ok(())
    }

    pub(crate) fn spawn_flush_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>, OrchestrationError> {
        let this = self.clone();
        let handle = std::thread::spawn(move || {
            let ticker = crossbeam_channel::tick(Duration::from_millis(50));
            loop {
                crossbeam_channel::select! {
                    recv(ticker) -> _ => if let Err(e) = this.trigger_flush() {
                        eprintln!("Flush failed: {}", e);
                    },
                    recv(rx) -> _ => return
                }
            }
        });

        Ok(Some(handle))
    }

    pub(crate) fn next_memtable_id(&self) -> usize {
        self.cur_memtable_id.fetch_add(1, Ordering::SeqCst);
        self.cur_memtable_id.load(Ordering::SeqCst)
    }

    pub(crate) fn get_sst_path(&self, id: usize) -> PathBuf {
        Self::sst_path(&self.options.data_dir, id)
    }

    fn sst_path(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref()
            .join(format!("{}-{:05}.sst", SST_FILE_PREFIX, id))
    }

    fn open_all_ssts(state: &mut State<B>, options: &CoreOptions) {
        for sst_id in state
            .level_zero
            .iter()
            .chain(state.levels.iter().flat_map(|(l, ids)| ids))
        {
            let id = *sst_id;
            let filepath = Self::sst_path(&options.data_dir, id);
            let file = OpenOptions::new()
                .read(true)
                .open(&filepath)
                .expect("SST file should be found at this path");

            match SSTable::open(id, &filepath, Arc::new(file)) {
                Ok(sst) => {
                    state.sstables.insert(id, Arc::new(sst));
                }
                Err(e) => eprintln!("Unable to open SST file: {e}"),
            };
        }
    }
}

fn create_fresh_manifest(data_dir: &Path) -> Manifest {
    match Manifest::create(data_dir) {
        Ok(manifest) => manifest,
        Err(e) => panic!("Unable to create manifest file. Original error: {e}"),
    }
}

fn should_traverse(table_start: &[u8], table_end: &[u8], start: &[u8], end: &[u8]) -> bool {
    !(start > table_end || end < table_start)
}

#[cfg(test)]
mod tests {
    use std::ops::Bound;

    use super::*;
    use crate::{
        memtable::{MemError, Memtable},
        types::Value,
    };
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

        fn flush(&self, path: PathBuf) -> Result<SSTable, MemError> {
            todo!()
        }
    }

    fn init_storage<B: Buffer + Clone + Send + Sync + 'static>(
        size: usize,
        path: PathBuf,
    ) -> CoreStorageInner<B> {
        let options = CoreOptions {
            max_memtable_size: size,
            data_dir: path.to_path_buf(),
            memtable_limit: 3,
            compaction_opts: CompactionOptions::default(),
        };
        CoreStorageInner::open(Some(options))
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

            fn flush(&self, path: PathBuf) -> Result<SSTable, MemError> {
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
            levels: (0..=4).map(|l| (l, vec![])).collect(),
            sstables: HashMap::new(),
        };

        let storage = CoreStorageInner {
            cur_memtable_id: AtomicUsize::new(3),
            state: Arc::new(ArcSwap::new(Arc::new(state))),
            freeze_lock: Mutex::new(()),
            options: CoreOptions {
                data_dir: path.to_path_buf(),
                max_memtable_size: 4,
                memtable_limit: 3,
                compaction_opts: CompactionOptions::default(),
            },
            manifest: Manifest::create(path).unwrap(),
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
            levels: (0..=4).map(|l| (l, vec![])).collect(),
            sstables: HashMap::new(),
        };

        let tempdir = TempDir::new().unwrap();
        let storage = CoreStorageInner {
            cur_memtable_id: AtomicUsize::new(3),
            state: Arc::new(ArcSwap::new(Arc::new(state))),
            freeze_lock: Mutex::new(()),
            options: CoreOptions {
                data_dir: tempdir.path().to_path_buf(),
                max_memtable_size: 4,
                memtable_limit: 3,
                compaction_opts: CompactionOptions::default(),
            },
            manifest: Manifest::create(path).unwrap(),
        };

        let iter = storage.scan(key.clone(), key_3.clone());
        assert!(iter.is_ok());

        let mut iter = iter.unwrap();
        assert_eq!(iter.next().unwrap(), (key, Value::Plain(value_3)));
        assert_eq!(iter.next().unwrap(), (key_2, Value::Plain(value_2)));
        assert_eq!(iter.next().unwrap(), (key_3, Value::Plain(value_1)));
    }

    #[test]
    fn get_key_from_sstable_when_not_found_in_any_memtable() {
        let tempdir = TempDir::new().unwrap();
        let path = tempdir.path();

        //Oldest memtable
        let frozen_table_one = test_memtable(1, path.to_path_buf());

        // Intermediate memtable
        let frozen_table_two = test_memtable(2, path.to_path_buf());

        // Current memtable
        let memtable = test_memtable(3, path.to_path_buf());

        let key = Bytes::from("new_key");
        let value = Bytes::from("previous value");
        let replacement_value = Bytes::from("current value");

        let scanning_key_1 = Bytes::from("key_1");
        let scanning_value_1 = Bytes::from("value_1");

        let scanning_key_2 = Bytes::from("key_2");
        let scanning_value_2 = Bytes::from("value_2");

        let scanning_key_3 = Bytes::from("key_3");
        let scanning_value_3 = Bytes::from("value_3");

        let _ = frozen_table_one.put(&key, &value);
        let _ = frozen_table_one.delete(&key);
        let _ = frozen_table_one.put(&scanning_key_3, &scanning_value_3);
        let _ = frozen_table_one.put(&scanning_key_2, &scanning_value_2);

        let _ = frozen_table_two.put(&key, &replacement_value);
        let _ = frozen_table_two.put(&scanning_key_1, &scanning_value_1);
        let _ = frozen_table_two.put(&scanning_key_2, &scanning_value_1);

        let _ = memtable.delete(&key);

        let mut q = VecDeque::new();
        q.push_back(Arc::new(frozen_table_one));
        q.push_back(Arc::new(frozen_table_two));
        let state = State {
            memtable: Arc::new(memtable),
            frozen: q,
            level_zero: vec![],
            levels: (0..=4).map(|l| (l, vec![])).collect(),
            sstables: HashMap::new(),
        };

        let storage = CoreStorageInner {
            cur_memtable_id: AtomicUsize::new(3),
            state: Arc::new(ArcSwap::new(Arc::new(state))),
            freeze_lock: Mutex::new(()),
            options: CoreOptions {
                data_dir: path.to_path_buf(),
                max_memtable_size: 4,
                memtable_limit: 3,
                compaction_opts: CompactionOptions::default(),
            },
            manifest: Manifest::create(path).unwrap(),
        };

        let _ = storage.force_flush_all();

        let result = storage.get(key);
        assert_eq!(result.unwrap(), replacement_value);

        let full_storage_iter = storage.scan(scanning_key_1.clone(), scanning_key_3.clone());
        assert!(full_storage_iter.is_ok());

        let mut full_storage_iter = full_storage_iter.unwrap();
        assert_eq!(
            full_storage_iter.next().unwrap(),
            (
                scanning_key_1.clone(),
                Value::Plain(scanning_value_1.clone())
            )
        );
        assert_eq!(
            full_storage_iter.next().unwrap(),
            (
                scanning_key_2.clone(),
                Value::Plain(scanning_value_1.clone())
            )
        );
        assert_eq!(
            full_storage_iter.next().unwrap(),
            (
                scanning_key_3.clone(),
                Value::Plain(scanning_value_3.clone())
            )
        );
    }
}
