#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use bytes::BufMut;

use super::{BlockMeta, FileObject, SsTable};
use crate::{
    block::BlockBuilder,
    key::{KeySlice, KeyVec},
    lsm_storage::BlockCache,
};

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    builder: BlockBuilder,
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    data: Vec<u8>,
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        Self {
            builder: BlockBuilder::new(block_size),
            first_key: Vec::new(),
            last_key: Vec::new(),
            data: Vec::new(),
            meta: Vec::new(),
            block_size,
        }
    }

    fn flush(&mut self) {
        if !self.builder.is_empty() {
            let first_key = std::mem::take(&mut self.first_key);
            let last_key = std::mem::take(&mut self.last_key);
            let builder = std::mem::replace(&mut self.builder, BlockBuilder::new(self.block_size));
            self.meta.push(BlockMeta {
                offset: self.data.len(),
                first_key: KeyVec::from_vec(first_key).into_key_bytes(),
                last_key: KeyVec::from_vec(last_key).into_key_bytes(),
            });

            self.data.extend(builder.build().encode());
        }
    }

    // helper function to add key-value pair. Returns true if succeeded, false if failed due to BlockBuilder being too full
    fn _add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        let added = self.builder.add(key, value);

        if added {
            let keyvec = key.to_key_vec().into_inner();
            if self.first_key.is_empty() {
                self.first_key = keyvec.clone();
            }
            self.last_key = keyvec.clone();
        };
        added
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        let added = self._add(key, value);
        if !added {
            self.flush();
            let added = self._add(key, value);
            if !added {
                panic!("failed to add key/value to SST")
            }
        }
    }

    /// Get the estimated size of the SSTable.
    ///
    /// Since the data blocks contain much more data than meta blocks, just return the size of data
    /// blocks here.
    pub fn estimated_size(&self) -> usize {
        self.data.len()
    }

    /// Builds the SSTable and writes it to the given path. Use the `FileObject` structure to manipulate the disk objects.
    pub fn build(
        &mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        self.flush();
        let metadata_offset = self.data.len();

        let mut data = self.data.clone();

        BlockMeta::encode_block_meta(&self.meta, &mut data);
        data.put_u32(metadata_offset as u32);

        let file = FileObject::create(path.as_ref(), data)?;

        SsTable::open(id, block_cache, file)
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(mut self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
