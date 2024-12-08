#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

mod leveled;
mod simple_leveled;
mod tiered;

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
pub use leveled::{LeveledCompactionController, LeveledCompactionOptions, LeveledCompactionTask};
use serde::{Deserialize, Serialize};
pub use simple_leveled::{
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, SimpleLeveledCompactionTask,
};
pub use tiered::{TieredCompactionController, TieredCompactionOptions, TieredCompactionTask};

use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::key::KeySlice;
use crate::lsm_storage::{LsmStorageInner, LsmStorageState};
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};

#[derive(Debug, Serialize, Deserialize)]
pub enum CompactionTask {
    Leveled(LeveledCompactionTask),
    Tiered(TieredCompactionTask),
    Simple(SimpleLeveledCompactionTask),
    ForceFullCompaction {
        l0_sstables: Vec<usize>,
        l1_sstables: Vec<usize>,
    },
}

impl CompactionTask {
    fn compact_to_bottom_level(&self) -> bool {
        match self {
            CompactionTask::ForceFullCompaction { .. } => true,
            CompactionTask::Leveled(task) => task.is_lower_level_bottom_level,
            CompactionTask::Simple(task) => task.is_lower_level_bottom_level,
            CompactionTask::Tiered(task) => task.bottom_tier_included,
        }
    }
}

pub(crate) enum CompactionController {
    Leveled(LeveledCompactionController),
    Tiered(TieredCompactionController),
    Simple(SimpleLeveledCompactionController),
    NoCompaction,
}

impl CompactionController {
    pub fn generate_compaction_task(&self, snapshot: &LsmStorageState) -> Option<CompactionTask> {
        match self {
            CompactionController::Leveled(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Leveled),
            CompactionController::Simple(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Simple),
            CompactionController::Tiered(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Tiered),
            CompactionController::NoCompaction => unreachable!(),
        }
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &CompactionTask,
        output: &[usize],
        in_recovery: bool,
    ) -> (LsmStorageState, Vec<usize>) {
        match (self, task) {
            (CompactionController::Leveled(ctrl), CompactionTask::Leveled(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output, in_recovery)
            }
            (CompactionController::Simple(ctrl), CompactionTask::Simple(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            (CompactionController::Tiered(ctrl), CompactionTask::Tiered(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            _ => unreachable!(),
        }
    }
}

impl CompactionController {
    pub fn flush_to_l0(&self) -> bool {
        matches!(
            self,
            Self::Leveled(_) | Self::Simple(_) | Self::NoCompaction
        )
    }
}

#[derive(Debug, Clone)]
pub enum CompactionOptions {
    /// Leveled compaction with partial compaction + dynamic level support (= RocksDB's Leveled
    /// Compaction)
    Leveled(LeveledCompactionOptions),
    /// Tiered compaction (= RocksDB's universal compaction)
    Tiered(TieredCompactionOptions),
    /// Simple leveled compaction
    Simple(SimpleLeveledCompactionOptions),
    /// In no compaction mode (week 1), always flush to L0
    NoCompaction,
}

impl LsmStorageInner {
    fn generate_sst_from_iter(
        &self,
        mut iter: impl for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>,
    ) -> Result<Vec<Arc<SsTable>>> {
        let mut builder = None;
        let mut new_sstables = Vec::new();

        while iter.is_valid() {
            if builder.is_none() {
                builder = Some(SsTableBuilder::new(self.options.block_size));
            }
            let key = iter.key();
            let value = iter.value();
            let b = builder.as_mut().unwrap();

            if !value.is_empty() {
                b.add(key, value);
            }

            if b.estimated_size() >= self.options.target_sst_size {
                let id = self.next_sst_id();
                let path = self.path_of_sst(id);
                let sstable = b.build(id, Some(self.block_cache.clone()), path)?;
                new_sstables.push(Arc::new(sstable));
                builder = None;
            }
            iter.next()?;
        }

        // get the last one (if not empty)
        if let Some(mut b) = builder {
            let id = self.next_sst_id();
            let path = self.path_of_sst(id);
            let sstable = b.build(id, Some(self.block_cache.clone()), path)?;
            new_sstables.push(Arc::new(sstable));
        }

        Ok(new_sstables)
    }

    fn compact(&self, task: &CompactionTask) -> Result<Vec<Arc<SsTable>>> {
        let snapshot = {
            let state = self.state.read();
            state.clone()
        };
        match task {
            CompactionTask::ForceFullCompaction {
                l0_sstables,
                l1_sstables,
            } => {
                let l0_sstables = l0_sstables
                    .iter()
                    .map(|id| snapshot.sstables.get(id).unwrap().clone())
                    .collect::<Vec<_>>();
                let l1_sstables = l1_sstables
                    .iter()
                    .map(|id| snapshot.sstables.get(id).unwrap().clone())
                    .collect::<Vec<_>>();

                let l0_iter = {
                    let mut l0_iters = Vec::new();
                    for sstable in l0_sstables {
                        let iter = SsTableIterator::create_and_seek_to_first(sstable)?;
                        l0_iters.push(Box::new(iter));
                    }

                    MergeIterator::create(l0_iters)
                };

                let l1_iter = SstConcatIterator::create_and_seek_to_first(l1_sstables)?;

                let iter = TwoMergeIterator::create(l0_iter, l1_iter)?;
                self.generate_sst_from_iter(iter)
            }
            CompactionTask::Simple(SimpleLeveledCompactionTask {
                upper_level,
                upper_level_sst_ids,
                lower_level_sst_ids,
                ..
            }) => match upper_level {
                None => {
                    let mut l0_iters = Vec::new();
                    for sstable in upper_level_sst_ids {
                        let sst = snapshot.sstables.get(sstable).unwrap().clone();
                        let iter = SsTableIterator::create_and_seek_to_first(sst)?;
                        l0_iters.push(Box::new(iter));
                    }

                    let upper_iter = MergeIterator::create(l0_iters);
                    let lower_iter = {
                        let lower_sstables = lower_level_sst_ids
                            .iter()
                            .map(|sst_id| snapshot.sstables.get(sst_id).unwrap().clone())
                            .collect::<Vec<_>>();
                        SstConcatIterator::create_and_seek_to_first(lower_sstables)?
                    };

                    let iter = TwoMergeIterator::create(upper_iter, lower_iter)?;
                    self.generate_sst_from_iter(iter)
                }
                Some(_) => {
                    let upper_iter = {
                        let sstables = upper_level_sst_ids
                            .iter()
                            .map(|sst_id| snapshot.sstables.get(sst_id).unwrap().clone())
                            .collect::<Vec<_>>();
                        SstConcatIterator::create_and_seek_to_first(sstables)?
                    };
                    let lower_iter = {
                        let sstables = lower_level_sst_ids
                            .iter()
                            .map(|sst_id| snapshot.sstables.get(sst_id).unwrap().clone())
                            .collect::<Vec<_>>();
                        SstConcatIterator::create_and_seek_to_first(sstables)?
                    };

                    let iter = TwoMergeIterator::create(upper_iter, lower_iter)?;
                    self.generate_sst_from_iter(iter)
                }
            },
            _ => unimplemented!(),
        }
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        let snapshot = {
            let state = self.state.read();
            state.clone()
        };
        let l0_sstables = snapshot.l0_sstables.clone();
        let l1_sstables = snapshot.levels[0].1.clone();
        let compaction_task = CompactionTask::ForceFullCompaction {
            l0_sstables: l0_sstables.clone(),
            l1_sstables: l1_sstables.clone(),
        };

        let new_sstables = self.compact(&compaction_task)?;
        let new_l1_tables = Vec::from_iter(new_sstables.iter().map(|s| s.sst_id()));
        {
            let _state_lock = self.state_lock.lock();
            let mut new_state = self.state.read().as_ref().clone();

            new_state
                .l0_sstables
                .truncate(new_state.l0_sstables.len() - l0_sstables.len());
            for sstable in new_sstables {
                new_state.sstables.insert(sstable.sst_id(), sstable);
            }
            for old_table in l0_sstables.iter().chain(l1_sstables.iter()) {
                new_state.sstables.remove(old_table);
            }
            new_state.levels[0].1.clone_from(&new_l1_tables);
            *self.state.write() = Arc::new(new_state);
        }
        for sst in l0_sstables.iter().chain(l1_sstables.iter()) {
            std::fs::remove_file(self.path_of_sst(*sst))?;
        }

        Ok(())
    }

    fn trigger_compaction(&self) -> Result<()> {
        if let CompactionController::NoCompaction = self.compaction_controller {
            return Ok(());
        }

        let snapshot = self.state.read().clone();
        match self
            .compaction_controller
            .generate_compaction_task(snapshot.as_ref())
        {
            None => Ok(()),
            Some(task) => {
                println!("triggering compaction {:?}", &task);
                let output = self.compact(&task)?;
                let output_ssts = output.iter().map(|x| x.sst_id()).collect::<Vec<_>>();

                let files_to_delete = {
                    let _state_lock = self.state_lock.lock();
                    let snapshot = self.state.read().as_ref().clone();

                    let (mut snapshot, files_to_delete) = self
                        .compaction_controller
                        .apply_compaction_result(&snapshot, &task, &output_ssts, false);

                    for sst_to_add in &output {
                        let res = snapshot
                            .sstables
                            .insert(sst_to_add.sst_id(), sst_to_add.clone());
                        assert!(res.is_none())
                    }

                    for sst_id_to_delete in &files_to_delete {
                        let res = snapshot.sstables.remove(sst_id_to_delete);
                        assert!(res.is_some())
                    }

                    *self.state.write() = Arc::new(snapshot);

                    files_to_delete
                };

                for sst_id_to_delete in files_to_delete {
                    std::fs::remove_file(self.path_of_sst(sst_id_to_delete))?
                }
                Ok(())
            }
        }
    }

    pub(crate) fn spawn_compaction_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        if let CompactionOptions::Leveled(_)
        | CompactionOptions::Simple(_)
        | CompactionOptions::Tiered(_) = self.options.compaction_options
        {
            let this = self.clone();
            let handle = std::thread::spawn(move || {
                let ticker = crossbeam_channel::tick(Duration::from_millis(50));
                loop {
                    crossbeam_channel::select! {
                        recv(ticker) -> _ => if let Err(e) = this.trigger_compaction() {
                            eprintln!("compaction failed: {}", e);
                        },
                        recv(rx) -> _ => return
                    }
                }
            });
            return Ok(Some(handle));
        }
        Ok(None)
    }

    fn trigger_flush(&self) -> Result<()> {
        let num_imm_memtables = {
            let guard = self.state.read();
            guard.imm_memtables.len()
        };

        if num_imm_memtables >= self.options.num_memtable_limit {
            self.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub(crate) fn spawn_flush_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        let this = self.clone();
        let handle = std::thread::spawn(move || {
            let ticker = crossbeam_channel::tick(Duration::from_millis(50));
            loop {
                crossbeam_channel::select! {
                    recv(ticker) -> _ => if let Err(e) = this.trigger_flush() {
                        eprintln!("flush failed: {}", e);
                    },
                    recv(rx) -> _ => return
                }
            }
        });
        Ok(Some(handle))
    }
}
