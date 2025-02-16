#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

use std::collections::{BTreeSet, HashMap};
use std::fs::{self, File};
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use farmhash::fingerprint32;
use parking_lot::{Mutex, MutexGuard, RwLock};

use crate::block::Block;
use crate::compact::{
    CompactionController, CompactionOptions, LeveledCompactionController, LeveledCompactionOptions,
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, TieredCompactionController,
};
use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::key::{KeyBytes, KeySlice};
use crate::lsm_iterator::{FusedIterator, LsmIterator};
use crate::manifest::{Manifest, ManifestRecord};
use crate::mem_table::MemTable;
use crate::mvcc::LsmMvccInner;
use crate::table::{FileObject, SsTable, SsTableBuilder, SsTableIterator};

pub type BlockCache = moka::sync::Cache<(usize, usize), Arc<Block>>;

pub(crate) fn map_bound(bound: Bound<&[u8]>) -> Bound<Bytes> {
    match bound {
        Bound::Included(x) => Bound::Included(Bytes::copy_from_slice(x)),
        Bound::Excluded(x) => Bound::Excluded(Bytes::copy_from_slice(x)),
        Bound::Unbounded => Bound::Unbounded,
    }
}

pub(crate) fn range_overlap(
    lower: Bound<&[u8]>,
    upper: Bound<&[u8]>,
    first_key: &KeyBytes,
    last_key: &KeyBytes,
) -> bool {
    let below_lower = match lower {
        Bound::Unbounded => false,
        Bound::Excluded(lower) => lower >= last_key.raw_ref(),
        Bound::Included(lower) => lower > last_key.raw_ref(),
    };

    let above_upper = match upper {
        Bound::Unbounded => false,
        Bound::Excluded(upper) => upper <= first_key.raw_ref(),
        Bound::Included(upper) => upper < first_key.raw_ref(),
    };

    let disjoint = below_lower || above_upper;
    !disjoint
}

pub(crate) fn key_maybe_within(sstable: &Arc<SsTable>, key: &[u8]) -> bool {
    let within_bounds = key >= sstable.first_key().raw_ref() && key <= sstable.last_key().raw_ref();
    let h = fingerprint32(key);

    let in_bloom_filter = sstable
        .bloom
        .as_ref()
        .map(|bloom| bloom.may_contain(h))
        .unwrap_or(true);

    within_bounds && in_bloom_filter
}

/// Represents the state of the storage engine.
#[derive(Clone)]
pub struct LsmStorageState {
    /// The current memtable.
    pub memtable: Arc<MemTable>,
    /// Immutable memtables, from latest to earliest.
    pub imm_memtables: Vec<Arc<MemTable>>,
    /// L0 SSTs, from latest to earliest.
    pub l0_sstables: Vec<usize>,
    /// SsTables sorted by key range; L1 - L_max for leveled compaction, or tiers for tiered
    /// compaction.
    pub levels: Vec<(usize, Vec<usize>)>,
    /// SST objects.
    pub sstables: HashMap<usize, Arc<SsTable>>,
}

pub enum WriteBatchRecord<T: AsRef<[u8]>> {
    Put(T, T),
    Del(T),
}

impl LsmStorageState {
    fn create(options: &LsmStorageOptions) -> Self {
        let levels = match &options.compaction_options {
            CompactionOptions::Leveled(LeveledCompactionOptions { max_levels, .. })
            | CompactionOptions::Simple(SimpleLeveledCompactionOptions { max_levels, .. }) => (1
                ..=*max_levels)
                .map(|level| (level, Vec::new()))
                .collect::<Vec<_>>(),
            CompactionOptions::Tiered(_) => Vec::new(),
            CompactionOptions::NoCompaction => vec![(1, Vec::new())],
        };
        Self {
            memtable: Arc::new(MemTable::create(0)),
            imm_memtables: Vec::new(),
            l0_sstables: Vec::new(),
            levels,
            sstables: Default::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LsmStorageOptions {
    // Block size in bytes
    pub block_size: usize,
    // SST size in bytes, also the approximate memtable capacity limit
    pub target_sst_size: usize,
    // Maximum number of memtables in memory, flush to L0 when exceeding this limit
    pub num_memtable_limit: usize,
    pub compaction_options: CompactionOptions,
    pub enable_wal: bool,
    pub serializable: bool,
}

impl LsmStorageOptions {
    pub fn default_for_week1_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 50,
            serializable: false,
        }
    }

    pub fn default_for_week1_day6_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }

    pub fn default_for_week2_test(compaction_options: CompactionOptions) -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 1 << 20, // 1MB
            compaction_options,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }
}

#[derive(Clone, Debug)]
pub enum CompactionFilter {
    Prefix(Bytes),
}

/// The storage interface of the LSM tree.
pub(crate) struct LsmStorageInner {
    pub(crate) state: Arc<RwLock<Arc<LsmStorageState>>>,
    pub(crate) state_lock: Mutex<()>,
    path: PathBuf,
    pub(crate) block_cache: Arc<BlockCache>,
    next_sst_id: AtomicUsize,
    pub(crate) options: Arc<LsmStorageOptions>,
    pub(crate) compaction_controller: CompactionController,
    pub(crate) manifest: Option<Manifest>,
    pub(crate) mvcc: Option<LsmMvccInner>,
    pub(crate) compaction_filters: Arc<Mutex<Vec<CompactionFilter>>>,
}

/// A thin wrapper for `LsmStorageInner` and the user interface for MiniLSM.
pub struct MiniLsm {
    pub(crate) inner: Arc<LsmStorageInner>,
    /// Notifies the L0 flush thread to stop working. (In week 1 day 6)
    flush_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the flush thread. (In week 1 day 6)
    flush_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// Notifies the compaction thread to stop working. (In week 2)
    compaction_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the compaction thread. (In week 2)
    compaction_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl Drop for MiniLsm {
    fn drop(&mut self) {
        self.compaction_notifier.send(()).ok();
        self.flush_notifier.send(()).ok();
    }
}

impl MiniLsm {
    pub fn close(&self) -> Result<()> {
        self.inner.sync_dir()?;
        self.compaction_notifier.send(()).ok();
        self.flush_notifier.send(()).ok();

        {
            // wait for compaction thread
            let mut guard = self.compaction_thread.lock();
            if let Some(t) = guard.take() {
                t.join().map_err(|e| anyhow::anyhow!("{:?}", e))?;
            }
        }
        {
            // wait for flush thread
            let mut guard = self.flush_thread.lock();

            if let Some(t) = guard.take() {
                t.join().map_err(|e| anyhow::anyhow!("{:?}", e))?;
            }
        }

        if !self.inner.options.enable_wal {
            // flush any in-memory memtables to sstables
            if !self.inner.state.read().memtable.is_empty() {
                self.inner
                    .freeze_memtable_with_memtable(Arc::new(MemTable::create(
                        self.inner.next_sst_id(),
                    )))?;
            }

            while {
                let snapshot = self.inner.state.read();
                !snapshot.imm_memtables.is_empty()
            } {
                self.inner.force_flush_next_imm_memtable()?;
            }
            self.inner.sync_dir()?;
        } else {
            self.inner.sync()?;
            self.inner.sync_dir()?;
        }

        Ok(())
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Arc<Self>> {
        if !path.as_ref().is_dir() {
            fs::create_dir(path.as_ref())?
        }
        let inner = Arc::new(LsmStorageInner::open(path, options)?);
        let (tx1, rx) = crossbeam_channel::unbounded();
        let compaction_thread = inner.spawn_compaction_thread(rx)?;
        let (tx2, rx) = crossbeam_channel::unbounded();
        let flush_thread = inner.spawn_flush_thread(rx)?;
        Ok(Arc::new(Self {
            inner,
            flush_notifier: tx2,
            flush_thread: Mutex::new(flush_thread),
            compaction_notifier: tx1,
            compaction_thread: Mutex::new(compaction_thread),
        }))
    }

    pub fn new_txn(&self) -> Result<()> {
        self.inner.new_txn()
    }

    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        self.inner.write_batch(batch)
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        self.inner.add_compaction_filter(compaction_filter)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.get(key)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.put(key, value)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.inner.delete(key)
    }

    pub fn sync(&self) -> Result<()> {
        self.inner.sync()
    }

    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        self.inner.scan(lower, upper)
    }

    /// Only call this in test cases due to race conditions
    pub fn force_flush(&self) -> Result<()> {
        if !self.inner.state.read().memtable.is_empty() {
            self.inner
                .force_freeze_memtable(&self.inner.state_lock.lock())?;
        }
        if !self.inner.state.read().imm_memtables.is_empty() {
            self.inner.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        self.inner.force_full_compaction()
    }
}

impl LsmStorageInner {
    pub(crate) fn next_sst_id(&self) -> usize {
        self.next_sst_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub(crate) fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Self> {
        let path = path.as_ref();
        let mut state = LsmStorageState::create(&options);
        let manifest_path = path.join("MANIFEST");
        let manifest;
        let mut next_sst_id = 1;
        let block_cache = Arc::new(BlockCache::new(1024));
        let mut memtables = BTreeSet::new();

        let compaction_controller = match &options.compaction_options {
            CompactionOptions::Leveled(options) => {
                CompactionController::Leveled(LeveledCompactionController::new(options.clone()))
            }
            CompactionOptions::Tiered(options) => {
                CompactionController::Tiered(TieredCompactionController::new(options.clone()))
            }
            CompactionOptions::Simple(options) => CompactionController::Simple(
                SimpleLeveledCompactionController::new(options.clone()),
            ),
            CompactionOptions::NoCompaction => CompactionController::NoCompaction,
        };

        if !manifest_path.exists() {
            if options.enable_wal {
                let id = state.memtable.id();
                state.memtable = Arc::new(MemTable::create_with_wal(
                    id,
                    Self::path_of_wal_static(path, id),
                )?);
            }

            manifest = Manifest::create(manifest_path)?;
            manifest.add_record_when_init(ManifestRecord::NewMemtable(state.memtable.id()))?;
        } else {
            let records;
            (manifest, records) = Manifest::recover(manifest_path)?;

            for record in records {
                match record {
                    ManifestRecord::Flush(sst_id) => {
                        let removed = memtables.remove(&sst_id);
                        assert!(removed, "missing NewMemTable?");
                        if compaction_controller.flush_to_l0() {
                            state.l0_sstables.insert(0, sst_id);
                        } else {
                            state.levels.insert(0, (sst_id, vec![sst_id]));
                        }
                        next_sst_id = next_sst_id.max(sst_id);
                    }
                    ManifestRecord::NewMemtable(sst_id) => {
                        memtables.insert(sst_id);
                        next_sst_id = next_sst_id.max(sst_id);
                    }
                    ManifestRecord::Compaction(task, output_ssts) => {
                        let (new_state, _) = compaction_controller.apply_compaction_result(
                            &state,
                            &task,
                            &output_ssts,
                            true,
                        );
                        state = new_state;
                        next_sst_id =
                            next_sst_id.max(output_ssts.iter().max().copied().unwrap_or_default());
                    }
                }
            }

            // populate sstables map
            for sst_id in state
                .l0_sstables
                .iter()
                .chain(state.levels.iter().flat_map(|(_, files)| files))
            {
                let sst_id = *sst_id;
                let sst = SsTable::open(
                    sst_id,
                    Some(block_cache.clone()),
                    FileObject::open(&Self::path_of_sst_static(path, sst_id))?,
                )?;
                state.sstables.insert(sst_id, Arc::new(sst));
            }

            // sort leveled compaction levels
            if let CompactionController::Leveled(_) = compaction_controller {
                for (_id, ssts) in &mut state.levels {
                    ssts.sort_by(|a, b| {
                        state.sstables[a]
                            .first_key()
                            .cmp(state.sstables[b].first_key())
                    });
                }
            }

            next_sst_id += 1;

            if options.enable_wal {
                // recover memtables from WAL
                for id in memtables {
                    let path = Self::path_of_wal_static(path, id);
                    let memtable = MemTable::recover_from_wal(id, path)?;
                    if !memtable.is_empty() {
                        state.imm_memtables.insert(0, Arc::new(memtable));
                    }
                }

                state.memtable = Arc::new(MemTable::create_with_wal(
                    next_sst_id,
                    Self::path_of_wal_static(path, next_sst_id),
                )?)
            } else {
                state.memtable = Arc::new(MemTable::create(next_sst_id));
            }

            manifest.add_record_when_init(ManifestRecord::NewMemtable(state.memtable.id()))?;
            next_sst_id += 1;
        }

        let storage = Self {
            state: Arc::new(RwLock::new(Arc::new(state))),
            state_lock: Mutex::new(()),
            path: path.to_path_buf(),
            block_cache,
            next_sst_id: AtomicUsize::new(next_sst_id),
            compaction_controller,
            manifest: Some(manifest),
            options: options.into(),
            mvcc: None,
            compaction_filters: Arc::new(Mutex::new(Vec::new())),
        };

        Ok(storage)
    }

    pub fn sync(&self) -> Result<()> {
        self.state.read().memtable.sync_wal()
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        let mut compaction_filters = self.compaction_filters.lock();
        compaction_filters.push(compaction_filter);
    }

    /// Get a key from the storage. In day 7, this can be further optimized by using a bloom filter.
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let read_memtable = |mem_table: &Arc<MemTable>| -> Option<Bytes> { mem_table.get(key) };

        let handle_tombstones = |bytes: Bytes| -> Option<Bytes> {
            if bytes.is_empty() {
                // tombstoned key
                None
            } else {
                Some(bytes)
            }
        };

        let snapshot = {
            let guard = self.state.read();
            guard.clone()
        };
        if let Some(res) = read_memtable(&snapshot.memtable) {
            return Ok(handle_tombstones(res));
        }
        if let Some(res) = snapshot.imm_memtables.iter().find_map(read_memtable) {
            return Ok(handle_tombstones(res));
        }

        for sstable in &snapshot.l0_sstables {
            let ss_table = snapshot.sstables.get(sstable).unwrap().clone();

            if key_maybe_within(&ss_table, key) {
                let ss_table_iter =
                    SsTableIterator::create_and_seek_to_key(ss_table, KeySlice::from_slice(key))?;

                if ss_table_iter.is_valid() && ss_table_iter.key().raw_ref() == key {
                    let val = ss_table_iter.value();
                    return Ok(handle_tombstones(Bytes::copy_from_slice(val)));
                }
            }
        }

        let mut level_iters = Vec::with_capacity(snapshot.levels.len());
        for (_, level_sst_ids) in &snapshot.levels {
            let mut level_ssts = Vec::new();
            for sst in level_sst_ids {
                let table = snapshot.sstables.get(sst).unwrap().clone();
                if key_maybe_within(&table, key) {
                    level_ssts.push(table);
                }
            }

            level_iters.push(Box::new(SstConcatIterator::create_and_seek_to_key(
                level_ssts,
                KeySlice::from_slice(key),
            )?));
        }

        let iter = MergeIterator::create(level_iters);

        if iter.is_valid() && iter.key().raw_ref() == key {
            return Ok(handle_tombstones(Bytes::copy_from_slice(iter.value())));
        }

        Ok(None)
    }

    /// Write a batch of data into the storage. Implement in week 2 day 7.
    pub fn write_batch<T: AsRef<[u8]>>(&self, _batch: &[WriteBatchRecord<T>]) -> Result<()> {
        unimplemented!()
    }

    /// Put a key-value pair into the storage by writing into the current memtable.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let res = self.state.read().memtable.put(key, value);

        let memtable_reached_capacity = || -> bool {
            self.state.read().memtable.approximate_size() > self.options.target_sst_size
        };

        if memtable_reached_capacity() {
            let state_lock = self.state_lock.lock();
            if memtable_reached_capacity() {
                self.force_freeze_memtable(&state_lock)?;
            }
        }

        res
    }

    /// Remove a key from the storage by writing an empty value.
    pub fn delete(&self, key: &[u8]) -> Result<()> {
        let res = self.state.read().memtable.put(key, &[]);

        let memtable_reached_capacity = || -> bool {
            self.state.read().memtable.approximate_size() > self.options.target_sst_size
        };

        if memtable_reached_capacity() {
            let state_lock = self.state_lock.lock();
            if memtable_reached_capacity() {
                self.force_freeze_memtable(&state_lock)?;
            }
        }

        res
    }

    pub(crate) fn path_of_sst_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.sst", id))
    }

    pub(crate) fn path_of_sst(&self, id: usize) -> PathBuf {
        Self::path_of_sst_static(&self.path, id)
    }

    pub(crate) fn path_of_wal_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.wal", id))
    }

    pub(crate) fn path_of_wal(&self, id: usize) -> PathBuf {
        Self::path_of_wal_static(&self.path, id)
    }

    pub(super) fn sync_dir(&self) -> Result<()> {
        File::open(&self.path)?.sync_all()?;

        Ok(())
    }

    fn freeze_memtable_with_memtable(&self, memtable: Arc<MemTable>) -> Result<()> {
        let mut guard = self.state.write();
        // Swap the current memtable with a new one.
        let mut snapshot = guard.as_ref().clone();
        let old_memtable = std::mem::replace(&mut snapshot.memtable, memtable);
        // Add the memtable to the immutable memtables.
        snapshot.imm_memtables.insert(0, old_memtable.clone());
        // Update the snapshot.
        *guard = Arc::new(snapshot);

        drop(guard);
        old_memtable.sync_wal()?;

        Ok(())
    }

    /// Force freeze the current memtable to an immutable memtable
    pub fn force_freeze_memtable(&self, _state_lock_observer: &MutexGuard<'_, ()>) -> Result<()> {
        let memtable_id = self.next_sst_id();
        let memtable = if self.options.enable_wal {
            let wal_path = self.path_of_wal(memtable_id);
            MemTable::create_with_wal(memtable_id, wal_path)?
        } else {
            MemTable::create(memtable_id)
        };

        {
            let mut state = self.state.write();
            let mut new_state = state.as_ref().clone();
            let old_memtable = std::mem::replace(&mut new_state.memtable, Arc::new(memtable));
            new_state.imm_memtables.insert(0, old_memtable);
            *state = Arc::new(new_state);
        }

        if let Some(manifest) = &self.manifest {
            manifest.add_record(
                _state_lock_observer,
                ManifestRecord::NewMemtable(memtable_id),
            )?;
        }

        self.sync_dir()?;

        Ok(())
    }

    /// Force flush the earliest-created immutable memtable to disk
    pub fn force_flush_next_imm_memtable(&self) -> Result<()> {
        let state_lock = self.state_lock.lock();

        let mem_table = {
            let guard = self.state.read();
            guard.imm_memtables.last().unwrap().clone()
        };

        let mut builder = SsTableBuilder::new(self.options.block_size);
        mem_table.flush(&mut builder)?;

        let id = mem_table.id();
        let path = self.path_of_sst(id);
        let ss_table = builder.build(id, Some(self.block_cache.clone()), path)?;

        {
            let mut guard = self.state.write();
            let mut state = guard.as_ref().clone();
            state.imm_memtables.pop();
            if self.compaction_controller.flush_to_l0() {
                state.l0_sstables.insert(0, id);
            } else {
                state.levels.insert(0, (id, vec![id]));
            }
            state.sstables.insert(id, Arc::new(ss_table));

            *guard = Arc::new(state);
        }

        if let Some(manifest) = &self.manifest {
            manifest.add_record(&state_lock, ManifestRecord::Flush(id))?;
        };
        self.sync_dir()?;

        Ok(())
    }

    pub fn new_txn(&self) -> Result<()> {
        // no-op
        Ok(())
    }

    /// Create an iterator over a range of keys.
    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        let snapshot = {
            let guard = self.state.read();
            guard.clone()
        };

        let mem_table_iterator = {
            let mut iters = Vec::with_capacity(snapshot.imm_memtables.len() + 1);
            iters.push(Box::new(snapshot.memtable.scan(lower, upper)));
            for t in snapshot.imm_memtables.iter() {
                iters.push(Box::new(t.scan(lower, upper)));
            }

            MergeIterator::create(iters)
        };
        let l0_iterator = {
            let mut iters = Vec::with_capacity(snapshot.l0_sstables.len());
            for l0_sstable in &snapshot.l0_sstables {
                let ss_table = snapshot.sstables.get(l0_sstable).unwrap().clone();
                if range_overlap(lower, upper, ss_table.first_key(), ss_table.last_key()) {
                    match lower {
                        Bound::Included(lower) => {
                            iters.push(Box::new(SsTableIterator::create_and_seek_to_key(
                                ss_table,
                                KeySlice::from_slice(lower),
                            )?))
                        }
                        Bound::Excluded(lower) => {
                            let mut iter = SsTableIterator::create_and_seek_to_key(
                                ss_table,
                                KeySlice::from_slice(lower),
                            )?;
                            if iter.is_valid() && iter.key().raw_ref() == lower {
                                iter.next()?;
                            };
                            iters.push(Box::new(iter))
                        }
                        Bound::Unbounded => iters.push(Box::new(
                            SsTableIterator::create_and_seek_to_first(ss_table)?,
                        )),
                    }
                }
            }
            MergeIterator::create(iters)
        };
        let levels_iterator = {
            let mut iters = Vec::new();

            for (_level, sstables) in &snapshot.levels {
                let mut tables = Vec::new();

                for sst_id in sstables {
                    let ss_table = snapshot.sstables.get(sst_id).unwrap().clone();
                    if range_overlap(lower, upper, ss_table.first_key(), ss_table.last_key()) {
                        tables.push(ss_table);
                    }
                }

                let iter = match lower {
                    Bound::Included(lower) => SstConcatIterator::create_and_seek_to_key(
                        tables,
                        KeySlice::from_slice(lower),
                    )?,
                    Bound::Excluded(lower) => {
                        let mut iter = SstConcatIterator::create_and_seek_to_key(
                            tables,
                            KeySlice::from_slice(lower),
                        )?;

                        if iter.is_valid() && iter.key().raw_ref() == lower {
                            iter.next()?;
                        };
                        iter
                    }
                    Bound::Unbounded => SstConcatIterator::create_and_seek_to_first(tables)?,
                };

                iters.push(Box::new(iter));
            }

            MergeIterator::create(iters)
        };

        let lsm_iterator = LsmIterator::new(
            TwoMergeIterator::create(
                TwoMergeIterator::create(mem_table_iterator, l0_iterator)?,
                levels_iterator,
            )?,
            map_bound(upper),
        )?;
        let fused_iterator = FusedIterator::new(lsm_iterator);

        Ok(fused_iterator)
    }
}
