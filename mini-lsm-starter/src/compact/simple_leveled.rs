use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Clone)]
pub struct SimpleLeveledCompactionOptions {
    pub size_ratio_percent: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SimpleLeveledCompactionTask {
    // if upper_level is `None`, then it is L0 compaction
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<usize>,
    pub lower_level: usize,
    pub lower_level_sst_ids: Vec<usize>,
    pub is_lower_level_bottom_level: bool,
}

pub struct SimpleLeveledCompactionController {
    options: SimpleLeveledCompactionOptions,
}

impl SimpleLeveledCompactionController {
    pub fn new(options: SimpleLeveledCompactionOptions) -> Self {
        Self { options }
    }

    /// Generates a compaction task.
    ///
    /// Returns `None` if no compaction needs to be scheduled. The order of SSTs in the compaction task id vector matters.
    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<SimpleLeveledCompactionTask> {
        if snapshot.l0_sstables.len() >= self.options.level0_file_num_compaction_trigger {
            return Some(SimpleLeveledCompactionTask {
                upper_level: None,
                upper_level_sst_ids: snapshot.l0_sstables.clone(),
                lower_level: 1,
                lower_level_sst_ids: snapshot.levels[0].1.clone(),
                is_lower_level_bottom_level: self.options.max_levels == 1,
            });
        }

        for i in 0..snapshot.levels.len() - 1 {
            if snapshot.levels[i].1.is_empty() {
                continue;
            }

            let ratio = 100 * snapshot.levels[i + 1].1.len() / snapshot.levels[i].1.len();
            if ratio < self.options.size_ratio_percent {
                return Some(SimpleLeveledCompactionTask {
                    upper_level: Some(i + 1),
                    upper_level_sst_ids: snapshot.levels[i].1.clone(),
                    lower_level: i + 2,
                    lower_level_sst_ids: snapshot.levels[i + 1].1.clone(),
                    is_lower_level_bottom_level: self.options.max_levels == i + 2,
                });
            }
        }

        None
    }

    /// Apply the compaction result.
    ///
    /// The compactor will call this function with the compaction task and the list of SST ids generated. This function applies the
    /// result and generates a new LSM state. The functions should only change `l0_sstables` and `levels` without changing memtables
    /// and `sstables` hash map. Though there should only be one thread running compaction jobs, you should think about the case
    /// where an L0 SST gets flushed while the compactor generates new SSTs, and with that in mind, you should do some sanity checks
    /// in your implementation.
    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &SimpleLeveledCompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        let mut snapshot = snapshot.clone();
        let files_to_remove = {
            task.upper_level_sst_ids
                .iter()
                .chain(task.lower_level_sst_ids.iter())
                .copied()
                .collect::<Vec<_>>()
        };

        match task.upper_level {
            None => {
                let l0_level = snapshot
                    .l0_sstables
                    .iter()
                    .copied()
                    .filter(|x| !task.upper_level_sst_ids.contains(x))
                    .collect::<Vec<_>>();
                let mut l1_level = snapshot.levels[0]
                    .1
                    .iter()
                    .copied()
                    .filter(|x| !task.lower_level_sst_ids.contains(x))
                    .collect::<Vec<_>>();

                assert!(l1_level.is_empty());
                l1_level.extend_from_slice(output);

                snapshot.l0_sstables = l0_level;
                snapshot.levels[0].1.clone_from(&l1_level);
            }
            Some(level) => {
                let idx = level - 1;
                let lower_idx = level;

                let upper_level = snapshot.levels[idx]
                    .1
                    .iter()
                    .copied()
                    .filter(|x| !task.upper_level_sst_ids.contains(x))
                    .collect::<Vec<_>>();
                let mut lower_level = snapshot.levels[lower_idx]
                    .1
                    .iter()
                    .copied()
                    .filter(|x| !task.lower_level_sst_ids.contains(x))
                    .collect::<Vec<_>>();

                assert!(upper_level.is_empty());
                assert!(lower_level.is_empty());
                lower_level.extend_from_slice(output);

                snapshot.levels[idx].1.clone_from(&upper_level);
                snapshot.levels[lower_idx].1.clone_from(&lower_level);
            }
        }

        (snapshot, files_to_remove)
    }
}
