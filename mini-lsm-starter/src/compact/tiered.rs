use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Serialize, Deserialize)]
pub struct TieredCompactionTask {
    pub tiers: Vec<(usize, Vec<usize>)>,
    pub bottom_tier_included: bool,
}

#[derive(Debug, Clone)]
pub struct TieredCompactionOptions {
    pub num_tiers: usize,
    pub max_size_amplification_percent: usize,
    pub size_ratio: usize,
    pub min_merge_width: usize,
}

pub struct TieredCompactionController {
    options: TieredCompactionOptions,
}

impl TieredCompactionController {
    pub fn new(options: TieredCompactionOptions) -> Self {
        Self { options }
    }

    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<TieredCompactionTask> {
        if snapshot.levels.len() < self.options.num_tiers {
            return None;
        }

        {
            // space amplification ratio
            let mut all_levels_except_last_size: usize = 0;
            let last_level_size = snapshot.levels.last().unwrap().1.len();
            for level in &snapshot.levels[..snapshot.levels.len() - 1] {
                all_levels_except_last_size += level.1.len()
            }

            if all_levels_except_last_size * 100
                >= self.options.max_size_amplification_percent * last_level_size
            {
                return Some(TieredCompactionTask {
                    tiers: snapshot.levels.clone(),
                    bottom_tier_included: true,
                });
            }
        }

        {
            // size ratio
            let mut sum_of_previous_tiers = snapshot.levels[0].1.len();
            for (i, level) in snapshot.levels.iter().enumerate().skip(1) {
                if sum_of_previous_tiers * 100 > level.1.len() * (100 + self.options.size_ratio)
                    && i + 1 >= self.options.min_merge_width
                {
                    return Some(TieredCompactionTask {
                        tiers: snapshot
                            .levels
                            .iter()
                            .take(i + 1)
                            .cloned()
                            .collect::<Vec<_>>(),
                        bottom_tier_included: i + 1 == snapshot.levels.len(),
                    });
                }

                sum_of_previous_tiers += level.1.len();
            }
        }

        {
            // reduce sorted run
            return Some(TieredCompactionTask {
                tiers: snapshot
                    .levels
                    .iter()
                    .take(self.options.num_tiers)
                    .cloned()
                    .collect::<Vec<_>>(),
                bottom_tier_included: false,
            });
        }
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &TieredCompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        let mut snapshot = snapshot.clone();

        let mut levels = Vec::new();
        let mut to_delete = Vec::new();

        let mut tiers = task.tiers.iter().cloned().collect::<HashMap<_, _>>();

        for level in &snapshot.levels {
            let removed = tiers.contains_key(&level.0);
            match tiers.remove(&level.0) {
                None => {
                    levels.push(level.clone());
                }
                Some(tier) => {
                    assert_eq!(tier.clone(), level.1);
                    to_delete.extend(tier);
                }
            }

            if removed && tiers.is_empty() {
                let mut new_tier = Vec::new();
                new_tier.extend_from_slice(output);
                levels.push((output[0], new_tier));
            }
        }

        snapshot.levels = levels;

        (snapshot, to_delete)
    }
}
