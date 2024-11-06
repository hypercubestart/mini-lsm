#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::sync::Arc;

use anyhow::Result;

use super::SsTable;
use crate::{block::BlockIterator, iterators::StorageIterator, key::KeySlice};

/// An iterator over the contents of an SSTable.
pub struct SsTableIterator {
    table: Arc<SsTable>,
    blk_iter: BlockIterator,
    blk_idx: usize,
}

impl SsTableIterator {
    /// Create a new iterator and seek to the first key-value pair in the first data block.
    pub fn create_and_seek_to_first(table: Arc<SsTable>) -> Result<Self> {
        let blk_idx = 0;
        let block = table.read_block_cached(blk_idx)?;

        let res = Self {
            table,
            blk_iter: BlockIterator::create_and_seek_to_first(block),
            blk_idx,
        };

        Ok(res)
    }

    fn seek_to_idx(&mut self, blk_idx: usize) -> Result<()> {
        self.blk_idx = blk_idx;
        let block = self.table.read_block_cached(blk_idx)?;

        let blk_iter = BlockIterator::create_and_seek_to_first(block);

        self.blk_idx = blk_idx;
        self.blk_iter = blk_iter;

        Ok(())
    }

    /// Seek to the first key-value pair in the first data block.
    pub fn seek_to_first(&mut self) -> Result<()> {
        self.seek_to_idx(0)
    }

    /// Create a new iterator and seek to the first key-value pair which >= `key`.
    pub fn create_and_seek_to_key(table: Arc<SsTable>, key: KeySlice) -> Result<Self> {
        let mut res = Self::create_and_seek_to_first(table)?;

        let () = res.seek_to_key(key)?;
        Ok(res)
    }

    /// Seek to the first key-value pair which >= `key`.
    /// Note: You probably want to review the handout for detailed explanation when implementing
    /// this function.
    pub fn seek_to_key(&mut self, key: KeySlice) -> Result<()> {
        let mut lo = 0;
        let mut hi = self.table.block_meta.len();

        while lo < hi - 1 {
            let mid = (hi + lo) / 2;
            if key < self.table.block_meta[mid].first_key.as_key_slice() {
                hi = mid;
            } else {
                lo = mid;
            }
        }
        let mut idx = lo;

        if key > self.table.block_meta[lo].last_key.as_key_slice() {
            idx += 1;
        };
        if idx == self.table.block_meta.len() {
            idx -= 1;
        }
        let () = self.seek_to_idx(idx)?;
        self.blk_iter.seek_to_key(key);
        Ok(())
    }
}

impl StorageIterator for SsTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    /// Return the `key` that's held by the underlying block iterator.
    fn key(&self) -> KeySlice {
        self.blk_iter.key()
    }

    /// Return the `value` that's held by the underlying block iterator.
    fn value(&self) -> &[u8] {
        self.blk_iter.value()
    }

    /// Return whether the current block iterator is valid or not.
    fn is_valid(&self) -> bool {
        self.blk_iter.is_valid()
    }

    /// Move to the next `key` in the block.
    /// Note: You may want to check if the current block iterator is valid after the move.
    fn next(&mut self) -> Result<()> {
        self.blk_iter.next();

        if !self.blk_iter.is_valid() && self.blk_idx + 1 < self.table.block_meta.len() {
            self.seek_to_idx(self.blk_idx + 1)?;
        };

        Ok(())
    }
}
