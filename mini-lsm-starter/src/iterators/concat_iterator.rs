use std::sync::Arc;

use anyhow::Result;

use super::StorageIterator;
use crate::{
    key::KeySlice,
    table::{SsTable, SsTableIterator},
};

/// Concat multiple iterators ordered in key order and their key ranges do not overlap. We do not want to create the
/// iterators when initializing this iterator to reduce the overhead of seeking.
pub struct SstConcatIterator {
    current: Option<SsTableIterator>,
    next_sst_idx: usize,
    sstables: Vec<Arc<SsTable>>,
}

impl SstConcatIterator {
    fn check_sst_valid(sstables: &[Arc<SsTable>]) {
        for sst in sstables {
            assert!(sst.first_key() <= sst.last_key());
        }
        if !sstables.is_empty() {
            for i in 0..(sstables.len() - 1) {
                assert!(sstables[i].last_key() < sstables[i + 1].first_key());
            }
        }
    }

    fn next_sst(&mut self) -> Result<()> {
        if self.next_sst_idx >= self.sstables.len() {
            self.current = None;
            Ok(())
        } else {
            let new_iter = SsTableIterator::create_and_seek_to_first(
                self.sstables[self.next_sst_idx].clone(),
            )?;
            self.current = Some(new_iter);
            self.next_sst_idx += 1;
            Ok(())
        }
    }

    fn _next(&mut self) -> Result<()> {
        if let Some(iter) = &mut self.current {
            iter.next()?;
            if !iter.is_valid() {
                self.next_sst()
            } else {
                Ok(())
            }
        } else {
            self.next_sst()
        }
    }

    pub fn create_and_seek_to_first(sstables: Vec<Arc<SsTable>>) -> Result<Self> {
        Self::check_sst_valid(&sstables);
        let mut ret = Self {
            next_sst_idx: 0,
            current: None,
            sstables,
        };

        // ignore if the first seek fails, since that means no elements
        let _ = ret._next();
        Ok(ret)
    }

    pub fn create_and_seek_to_key(sstables: Vec<Arc<SsTable>>, key: KeySlice) -> Result<Self> {
        Self::check_sst_valid(&sstables);
        for (idx, sstable) in sstables.iter().enumerate() {
            if sstable.last_key().as_key_slice() >= key {
                let iter = SsTableIterator::create_and_seek_to_key(sstable.clone(), key)?;
                let ret = Self {
                    current: Some(iter),
                    sstables,
                    next_sst_idx: idx + 1,
                };

                return Ok(ret);
            }
        }

        Ok(Self {
            current: None,
            next_sst_idx: sstables.len(),
            sstables,
        })
    }
}

impl StorageIterator for SstConcatIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        self.current.as_ref().unwrap().key()
    }

    fn value(&self) -> &[u8] {
        self.current.as_ref().unwrap().value()
    }

    fn is_valid(&self) -> bool {
        self.current
            .as_ref()
            .map(|i| i.is_valid())
            .unwrap_or_else(|| false)
    }

    fn next(&mut self) -> Result<()> {
        self._next()
    }

    fn num_active_iterators(&self) -> usize {
        1
    }
}
