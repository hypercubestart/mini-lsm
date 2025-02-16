#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;
use std::sync::Arc;
use std::{fs::File, io::Read};

use anyhow::Result;
use bytes::Buf;
use parking_lot::{Mutex, MutexGuard};
use serde::{Deserialize, Serialize};

use crate::compact::CompactionTask;

pub struct Manifest {
    file: Arc<Mutex<File>>,
}

#[derive(Serialize, Deserialize)]
pub enum ManifestRecord {
    Flush(usize),
    NewMemtable(usize),
    Compaction(CompactionTask, Vec<usize>),
}

impl Manifest {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .create_new(true)
            .write(true)
            .open(path)?;
        Ok(Manifest {
            file: Arc::new(Mutex::new(file)),
        })
    }

    pub fn recover(path: impl AsRef<Path>) -> Result<(Self, Vec<ManifestRecord>)> {
        let mut file = OpenOptions::new()
            .read(true)
            .create_new(false)
            .write(true)
            .open(path)?;

        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;
        let mut buf_ptr = bytes.as_slice();

        let mut records = Vec::new();

        let manifest = Manifest {
            file: Arc::new(Mutex::new(file)),
        };

        while buf_ptr.has_remaining() {
            let len = buf_ptr.get_u64();
            let slice = &buf_ptr[..len as usize];
            records.push(serde_json::from_slice::<ManifestRecord>(slice)?);
            buf_ptr.advance(len as usize);
        }

        Ok((manifest, records))
    }

    pub fn add_record(
        &self,
        _state_lock_observer: &MutexGuard<()>,
        record: ManifestRecord,
    ) -> Result<()> {
        self.add_record_when_init(record)
    }

    pub fn add_record_when_init(&self, record: ManifestRecord) -> Result<()> {
        let bytes = serde_json::to_vec(&record)?;
        let len = bytes.len() as u64;

        let mut guard = self.file.lock();
        guard.write_all(&len.to_be_bytes())?;
        guard.write_all(&bytes)?;
        guard.sync_all()?;

        Ok(())
    }
}
