#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

pub(crate) mod bloom;
mod builder;
mod iterator;

use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use anyhow::{anyhow, Result};
pub use builder::SsTableBuilder;
use bytes::{Buf, BufMut};
pub use iterator::SsTableIterator;

use crate::block::Block;
use crate::key::{KeyBytes, KeySlice};
use crate::lsm_storage::BlockCache;

use self::bloom::Bloom;

const U16_SIZE: usize = std::mem::size_of::<u16>();
const U32_SIZE: usize = std::mem::size_of::<u32>();

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockMeta {
    /// Offset of this data block.
    pub offset: usize,
    /// The first key of the data block.
    pub first_key: KeyBytes,
    /// The last key of the data block.
    pub last_key: KeyBytes,
}

impl BlockMeta {
    /// Encode block meta to a buffer.
    /// You may add extra fields to the buffer,
    /// in order to help keep track of `first_key` when decoding from the same buffer in the future.
    pub fn encode_block_meta(block_meta: &[BlockMeta], buf: &mut Vec<u8>) {
        for meta in block_meta {
            buf.put_u32(meta.offset as u32);
            buf.put_u16(meta.first_key.key_len() as u16);
            buf.put(meta.first_key.key_ref());
            buf.put_u64(meta.first_key.ts());
            buf.put_u16(meta.last_key.key_len() as u16);
            buf.put(meta.last_key.key_ref());
            buf.put_u64(meta.last_key.ts());
        }
    }

    /// Decode block meta from a buffer.
    pub fn decode_block_meta(mut buf: &[u8]) -> Vec<BlockMeta> {
        let mut res = Vec::new();

        while buf.has_remaining() {
            let offset = buf.get_u32() as usize;
            let first_key_length = buf.get_u16() as usize;
            let first_key_bytes = buf.copy_to_bytes(first_key_length);
            let first_key_ts = buf.get_u64();
            let first_key = KeyBytes::from_bytes_with_ts(first_key_bytes, first_key_ts);
            let last_key_length = buf.get_u16() as usize;
            let last_key_bytes = buf.copy_to_bytes(last_key_length);
            let last_key_ts = buf.get_u64();
            let last_key = KeyBytes::from_bytes_with_ts(last_key_bytes, last_key_ts);

            res.push(BlockMeta {
                offset,
                first_key,
                last_key,
            });
        }

        res
    }
}

/// A file object.
pub struct FileObject(Option<File>, u64);

impl FileObject {
    pub fn read(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        use std::os::unix::fs::FileExt;
        let mut data = vec![0; len as usize];
        self.0
            .as_ref()
            .unwrap()
            .read_exact_at(&mut data[..], offset)?;
        Ok(data)
    }

    pub fn size(&self) -> u64 {
        self.1
    }

    /// Create a new file object (day 2) and write the file to the disk (day 4).
    pub fn create(path: &Path, data: Vec<u8>) -> Result<Self> {
        std::fs::write(path, &data)?;
        File::open(path)?.sync_all()?;
        Ok(FileObject(
            Some(File::options().read(true).write(false).open(path)?),
            data.len() as u64,
        ))
    }

    pub fn open(path: &Path) -> Result<Self> {
        let file = File::options().read(true).write(false).open(path)?;
        let size = file.metadata()?.len();
        Ok(FileObject(Some(file), size))
    }
}

/// An SSTable.
pub struct SsTable {
    /// The actual storage unit of SsTable, the format is as above.
    pub(crate) file: FileObject,
    /// The meta blocks that hold info for data blocks.
    pub(crate) block_meta: Vec<BlockMeta>,
    /// The offset that indicates the start point of meta blocks in `file`.
    pub(crate) block_meta_offset: usize,
    id: usize,
    block_cache: Option<Arc<BlockCache>>,
    first_key: KeyBytes,
    last_key: KeyBytes,
    pub(crate) bloom: Option<Bloom>,
    /// The maximum timestamp stored in this SST, implemented in week 3.
    max_ts: u64,
}

impl SsTable {
    #[cfg(test)]
    pub(crate) fn open_for_test(file: FileObject) -> Result<Self> {
        Self::open(0, None, file)
    }

    /// Open SSTable from a file.
    pub fn open(id: usize, block_cache: Option<Arc<BlockCache>>, file: FileObject) -> Result<Self> {
        let size = file.size();
        let bloom_block_end = size - (U32_SIZE as u64);
        let bloom_offset = {
            let data = file.read(bloom_block_end, U32_SIZE as u64)?;
            (&data[..]).get_u32() as u64
        };
        let bloom = {
            let data = file.read(bloom_offset, bloom_block_end - bloom_offset)?;
            Bloom::decode(&data)?
        };

        let meta_block_end = bloom_offset - (U32_SIZE as u64);
        let block_meta_offset = {
            let data = file.read(meta_block_end, U32_SIZE as u64)?;
            (&data[..]).get_u32() as u64
        };

        let block_meta = {
            let data = file.read(block_meta_offset, meta_block_end - block_meta_offset)?;
            BlockMeta::decode_block_meta(&data)
        };

        let first_key = block_meta.first().unwrap().first_key.clone();
        let last_key = block_meta.last().unwrap().last_key.clone();

        let res = Self {
            file,
            block_meta,
            block_meta_offset: block_meta_offset as usize,
            id,
            block_cache,
            first_key,
            last_key,
            bloom: Some(bloom),
            max_ts: 0,
        };

        Ok(res)
    }

    /// Create a mock SST with only first key + last key metadata
    pub fn create_meta_only(
        id: usize,
        file_size: u64,
        first_key: KeyBytes,
        last_key: KeyBytes,
    ) -> Self {
        Self {
            file: FileObject(None, file_size),
            block_meta: vec![],
            block_meta_offset: 0,
            id,
            block_cache: None,
            first_key,
            last_key,
            bloom: None,
            max_ts: 0,
        }
    }

    /// Read a block from the disk.
    pub fn read_block(&self, block_idx: usize) -> Result<Arc<Block>> {
        let meta = &self.block_meta[block_idx];
        let end_offset = {
            if block_idx == self.block_meta.len() - 1 {
                self.block_meta_offset
            } else {
                self.block_meta[block_idx + 1].offset
            }
        };

        let data = self
            .file
            .read(meta.offset as u64, (end_offset - meta.offset) as u64)?;
        let block = Block::decode(&data);
        Ok(Arc::new(block))
    }

    /// Read a block from disk, with block cache. (Day 4)
    pub fn read_block_cached(&self, block_idx: usize) -> Result<Arc<Block>> {
        match &self.block_cache {
            None => self.read_block(block_idx),
            Some(cache) => {
                let res = cache.try_get_with((self.id, block_idx), || self.read_block(block_idx));
                res.map_err(|e| anyhow!("{}", e))
            }
        }
    }

    /// Find the block that may contain `key`.
    /// Note: You may want to make use of the `first_key` stored in `BlockMeta`.
    /// You may also assume the key-value pairs stored in each consecutive block are sorted.
    pub fn find_block_idx(&self, key: KeySlice) -> usize {
        for (idx, meta) in self.block_meta.iter().enumerate() {
            if meta.first_key.as_key_slice() <= key && key <= meta.last_key.as_key_slice() {
                return idx;
            }
        }

        panic!("could not find block idx!")
    }

    /// Get number of data blocks.
    pub fn num_of_blocks(&self) -> usize {
        self.block_meta.len()
    }

    pub fn first_key(&self) -> &KeyBytes {
        &self.first_key
    }

    pub fn last_key(&self) -> &KeyBytes {
        &self.last_key
    }

    pub fn table_size(&self) -> u64 {
        self.file.1
    }

    pub fn sst_id(&self) -> usize {
        self.id
    }

    pub fn max_ts(&self) -> u64 {
        self.max_ts
    }
}
