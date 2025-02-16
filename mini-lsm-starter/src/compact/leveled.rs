use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Serialize, Deserialize)]
pub struct LeveledCompactionTask {
    // if upper_level is `None`, then it is L0 compaction
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<usize>,
    pub lower_level: usize,
    pub lower_level_sst_ids: Vec<usize>,
    pub is_lower_level_bottom_level: bool,
}

#[derive(Debug, Clone)]
pub struct LeveledCompactionOptions {
    pub level_size_multiplier: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
    pub base_level_size_mb: usize,
}

pub struct LeveledCompactionController {
    options: LeveledCompactionOptions,
}

impl LeveledCompactionController {
    pub fn new(options: LeveledCompactionOptions) -> Self {
        Self { options }
    }

    fn find_overlapping_ssts(
        &self,
        snapshot: &LsmStorageState,
        sst_ids: &[usize],
        in_level: usize,
    ) -> Vec<usize> {
        let first_key = sst_ids
            .iter()
            .map(|x| snapshot.sstables[x].first_key())
            .min()
            .cloned()
            .unwrap();
        let end_key = sst_ids
            .iter()
            .map(|x| snapshot.sstables[x].last_key())
            .max()
            .cloned()
            .unwrap();

        let mut res = Vec::new();

        for sst in &snapshot.levels[in_level - 1].1 {
            let sstable = &snapshot.sstables[sst];

            if !(sstable.first_key() > &end_key || sstable.last_key() < &first_key) {
                res.push(*sst)
            }
        }

        res
    }

    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<LeveledCompactionTask> {
        // compute target sizes
        let mut target_sizes = vec![0usize; self.options.max_levels];
        let min_level_size = self.options.base_level_size_mb * 1_000_000;

        for i in (0..self.options.max_levels).rev() {
            if i == self.options.max_levels - 1 {
                let mut base_level_size = 0usize;
                if snapshot.levels.len() == self.options.max_levels {
                    for sst in &snapshot.levels[i].1 {
                        base_level_size += snapshot.sstables[sst].table_size() as usize
                    }
                }

                target_sizes[i] = std::cmp::max(min_level_size, base_level_size)
            } else if target_sizes[i + 1] > min_level_size {
                target_sizes[i] = target_sizes[i + 1] / self.options.level_size_multiplier
            }
        }

        if snapshot.l0_sstables.len() >= self.options.level0_file_num_compaction_trigger {
            let first_nonzero_level = target_sizes
                .iter()
                .enumerate()
                .find(|x| *x.1 > 0)
                .unwrap()
                .0
                + 1;
            return Some(LeveledCompactionTask {
                upper_level: None,
                upper_level_sst_ids: snapshot.l0_sstables.clone(),
                lower_level: first_nonzero_level,
                lower_level_sst_ids: snapshot.levels[first_nonzero_level - 1].1.clone(),
                is_lower_level_bottom_level: first_nonzero_level == self.options.max_levels,
            });
        }

        // level priority
        let mut level_sizes = Vec::new();
        for level in &snapshot.levels {
            let mut size = 0;
            for sst in &level.1 {
                size += snapshot.sstables[sst].table_size() as usize
            }

            level_sizes.push(size);
        }

        let priority = level_sizes
            .iter()
            .zip(target_sizes)
            .map(|(size, target)| *size as f32 / target as f32)
            .collect::<Vec<_>>();

        let max_priority_level = priority
            .iter()
            .enumerate()
            .max_by(|(_, a), (_, b)| a.total_cmp(b))
            .unwrap();

        if *max_priority_level.1 <= 1.0 {
            return None;
        }
        let upper_level = max_priority_level.0 + 1;
        let lower_level = upper_level + 1;

        let upper_level_sst = *snapshot.levels[upper_level - 1].1.iter().min().unwrap();

        let lower_level_ssts =
            self.find_overlapping_ssts(snapshot, &[upper_level_sst], lower_level);

        Some(LeveledCompactionTask {
            upper_level: Some(upper_level),
            upper_level_sst_ids: vec![upper_level_sst],
            lower_level,
            lower_level_sst_ids: lower_level_ssts,
            is_lower_level_bottom_level: lower_level == self.options.max_levels,
        })
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &LeveledCompactionTask,
        output: &[usize],
        in_recovery: bool,
    ) -> (LsmStorageState, Vec<usize>) {
        let files_to_delete = task
            .upper_level_sst_ids
            .iter()
            .chain(task.lower_level_sst_ids.iter())
            .cloned()
            .collect::<Vec<_>>();

        let mut snapshot = snapshot.clone();

        match task.upper_level {
            None => {
                // l0 sstables
                let l0_sstables = snapshot
                    .l0_sstables
                    .iter()
                    .filter(|x| !task.upper_level_sst_ids.contains(x))
                    .cloned()
                    .collect::<Vec<_>>();
                snapshot.l0_sstables = l0_sstables
            }
            Some(upper_level) => {
                let level = snapshot.levels[upper_level - 1]
                    .1
                    .iter()
                    .filter(|x| !task.upper_level_sst_ids.contains(x))
                    .cloned()
                    .collect::<Vec<_>>();
                snapshot.levels[upper_level - 1].1 = level;
            }
        }

        let mut lower_level = output
            .iter()
            .chain(
                snapshot.levels[task.lower_level - 1]
                    .1
                    .iter()
                    .filter(|x| !task.lower_level_sst_ids.contains(x)),
            )
            .cloned()
            .collect::<Vec<_>>();

        if !in_recovery {
            lower_level.sort_by(|a, b| {
                snapshot.sstables[a]
                    .first_key()
                    .cmp(snapshot.sstables[b].first_key())
            });
        }
        snapshot.levels[task.lower_level - 1].1 = lower_level;

        (snapshot, files_to_delete)
    }
}
