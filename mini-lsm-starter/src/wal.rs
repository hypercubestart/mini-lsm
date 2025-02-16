#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Write};
use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use bytes::{Buf, Bytes};
use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;

pub struct Wal {
    file: Arc<Mutex<BufWriter<File>>>,
}

impl Wal {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .create_new(true)
            .write(true)
            .open(path)?;

        Ok(Wal {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    pub fn recover(path: impl AsRef<Path>, skiplist: &mut SkipMap<Bytes, Bytes>) -> Result<Self> {
        let mut file = OpenOptions::new()
            .read(true)
            .create_new(false)
            .write(true)
            .open(path)?;

        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;
        let mut buf_ptr = bytes.as_slice();

        while buf_ptr.has_remaining() {
            let key_len = buf_ptr.get_u64() as usize;
            let key = Bytes::copy_from_slice(&buf_ptr[..key_len]);
            buf_ptr.advance(key_len);

            let value_len = buf_ptr.get_u64() as usize;
            let value = Bytes::copy_from_slice(&buf_ptr[..value_len]);
            buf_ptr.advance(value_len);

            skiplist.insert(key, value);
        }

        Ok(Wal {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        {
            let mut guard = self.file.lock();
            guard.write_all(&(key.len() as u64).to_be_bytes())?;
            guard.write_all(key)?;
            guard.write_all(&(value.len() as u64).to_be_bytes())?;
            guard.write_all(value)?;
        }

        Ok(())
    }

    /// Implement this in week 3, day 5.
    pub fn put_batch(&self, _data: &[(&[u8], &[u8])]) -> Result<()> {
        unimplemented!()
    }

    pub fn sync(&self) -> Result<()> {
        {
            let mut guard = self.file.lock();
            guard.flush()?;
            guard.get_mut().sync_all()?;
        }

        Ok(())
    }
}
