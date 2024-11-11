#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::sync::Arc;

use nom::ToUsize;

use crate::key::{KeySlice, KeyVec};

use super::Block;

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    key: KeyVec,
    /// the current value range in the block.data, corresponds to the current key
    value_range: (usize, usize),
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,
    /// The first key in the block
    first_key: KeyVec,
}

const SIZE_U16: usize = std::mem::size_of::<u16>();

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        let first_key = {
            let first_key_length =
                u16::from_be_bytes(block.data[0..SIZE_U16].try_into().unwrap()) as usize;
            KeySlice::from_slice(&block.data[SIZE_U16..SIZE_U16 + first_key_length]).to_key_vec()
        };

        Self {
            block,
            key: KeyVec::new(),
            value_range: (0, 0),
            idx: 0,
            first_key,
        }
    }

    fn seek_to_idx(&mut self, idx: usize) {
        self.idx = idx;
        if self.idx == self.block.offsets.len() {
            self.key = KeyVec::new();
        } else {
            // start ... key_start .... value_length_start ...value_start ... end
            let start = self.block.offsets[self.idx] as usize;

            let (key, key_end) = {
                if idx == 0 {
                    (self.first_key.clone(), SIZE_U16 + self.first_key.len())
                } else {
                    let key_overlap_len = u16::from_be_bytes(
                        self.block.data[start..start + SIZE_U16].try_into().unwrap(),
                    ) as usize;

                    let start_key = start + SIZE_U16 + SIZE_U16;
                    let rest_key_len = u16::from_be_bytes(
                        self.block.data[start + SIZE_U16..start_key]
                            .try_into()
                            .unwrap(),
                    ) as usize;

                    let suffix_key = &self.block.data[start_key..start_key + rest_key_len];

                    let mut key =
                        KeySlice::from_slice(&self.first_key.raw_ref()[..key_overlap_len])
                            .to_key_vec();
                    key.append(suffix_key);

                    (key, start_key + rest_key_len)
                }
            };

            let value_length_start = key_end;
            let value_start = value_length_start + SIZE_U16;
            let value_length = u16::from_be_bytes(
                self.block.data[value_length_start..value_start]
                    .try_into()
                    .unwrap(),
            )
            .to_usize();
            let end = value_start + value_length;

            self.key = key;
            self.value_range = (value_start, end);
        }
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut iter = Self::new(block);
        iter.seek_to_first();

        iter
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        // TODO: implement this as binary search
        let mut iter = Self::new(block);
        iter.seek_to_key(key);

        iter
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> KeySlice {
        self.key.as_key_slice()
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        &self.block.data[self.value_range.0..self.value_range.1]
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        !self.key.is_empty()
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        self.seek_to_idx(0);
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        self.seek_to_idx(self.idx + 1);
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by
    /// callers.
    pub fn seek_to_key(&mut self, key: KeySlice) {
        for n in 0..self.block.offsets.len() {
            Self::seek_to_idx(self, n);
            if self.key.as_key_slice() >= key {
                break;
            }
        }
    }
}
