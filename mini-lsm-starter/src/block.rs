#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::{Buf, BufMut, Bytes};
pub use iterator::BlockIterator;

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the tutorial
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        let mut res = self.data.clone();

        for offset in &self.offsets {
            res.put_u16(*offset);
        }

        res.put_u16(self.offsets.len() as u16);
        Bytes::copy_from_slice(&res)
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        let size_u16 = std::mem::size_of::<u16>();
        let data_len = data.len();
        let num_elems = (&data[data_len - 2..]).get_u16() as usize;

        let mut offsets = Vec::with_capacity(num_elems);

        let offsets_byte_start = data_len - size_u16 - size_u16 * num_elems;

        for i in 0..num_elems {
            let o = offsets_byte_start + i * size_u16;
            let offset = u16::from_be_bytes(data[o..o + 2].try_into().unwrap());
            offsets.push(offset);
        }

        Block {
            data: data[..offsets_byte_start].to_vec(),
            offsets,
        }
    }
}
