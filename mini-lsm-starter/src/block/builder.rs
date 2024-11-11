#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use bytes::BufMut;

use crate::key::{Key, KeySlice, KeyVec};

use super::Block;

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The first key in the block
    first_key: KeyVec,
}

pub fn prefix(xs: &[u8], ys: &[u8]) -> usize {
    xs.iter().zip(ys).take_while(|(x, y)| x == y).count()
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        BlockBuilder {
            offsets: Vec::new(),
            data: Vec::new(),
            block_size,
            first_key: Key::from_vec(Vec::new()),
        }
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        let size_u16 = std::mem::size_of::<u16>();
        let byte_size_increase = size_u16 + key.len() + size_u16 + value.len() + size_u16;
        let cur_size = self.data.len() + self.offsets.len() * size_u16 + size_u16;

        let new_size = cur_size + byte_size_increase;

        if self.is_empty() || new_size <= self.block_size {
            let offset = self.data.len();
            if self.offsets.is_empty() {
                // first key encode as key_len (u16), key_data
                self.first_key = key.to_key_vec();
                self.data.put_u16(key.len() as u16);
                self.data.put(key.raw_ref());
            } else {
                // key_overlap_len (u16) | rest_key_len (u16) | key (rest_key_len)
                let overlap = prefix(self.first_key.raw_ref(), key.raw_ref());
                self.data.put_u16(overlap as u16);
                let rest_key_len = key.len() as u16 - overlap as u16;
                self.data.put_u16(rest_key_len);
                self.data.put(&key.raw_ref()[overlap..]);
            }

            self.offsets.push(offset as u16);

            self.data.put_u16(value.len() as u16);
            self.data.put(value);
            true
        } else {
            false
        }
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }
}
