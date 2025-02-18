#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

use std::ops::Bound;
use std::path::Path;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use crossbeam_skiplist::map::Entry;
use crossbeam_skiplist::SkipMap;
use ouroboros::self_referencing;

use crate::iterators::StorageIterator;
use crate::key::{KeyBytes, KeySlice, TS_DEFAULT};
use crate::table::SsTableBuilder;
use crate::wal::Wal;

/// A basic mem-table based on crossbeam-skiplist.
///
/// An initial implementation of memtable is part of week 1, day 1. It will be incrementally implemented in other
/// chapters of week 1 and week 2.
pub struct MemTable {
    map: Arc<SkipMap<KeyBytes, Bytes>>,
    wal: Option<Wal>,
    id: usize,
    approximate_size: Arc<AtomicUsize>,
}

fn key_slice_to_key_bytes(key: KeySlice) -> KeyBytes {
    let key_bytes = Bytes::copy_from_slice(key.key_ref());
    KeyBytes::from_bytes_with_ts(key_bytes, key.ts())
}

/// Create a bound of `Bytes` from a bound of `&[u8]`.
pub(crate) fn map_bound(bound: Bound<KeySlice>) -> Bound<KeyBytes> {
    match bound {
        Bound::Included(x) => Bound::Included(key_slice_to_key_bytes(x)),
        Bound::Excluded(x) => Bound::Excluded(key_slice_to_key_bytes(x)),
        Bound::Unbounded => Bound::Unbounded,
    }
}

pub(crate) fn map_bound_with_ts(bound: Bound<&[u8]>, ts: u64) -> Bound<KeySlice> {
    match bound {
        Bound::Included(x) => Bound::Included(KeySlice::from_slice(x, ts)),
        Bound::Excluded(x) => Bound::Excluded(KeySlice::from_slice(x, ts)),
        Bound::Unbounded => Bound::Unbounded,
    }
}

impl MemTable {
    /// Create a new mem-table.
    pub fn create(id: usize) -> Self {
        MemTable {
            map: Arc::new(SkipMap::new()),
            wal: None,
            id,
            approximate_size: Arc::new(AtomicUsize::new(0usize)),
        }
    }

    /// Create a new mem-table with WAL
    pub fn create_with_wal(id: usize, path: impl AsRef<Path>) -> Result<Self> {
        let wal = Wal::create(path)?;
        Ok(MemTable {
            map: Arc::new(SkipMap::new()),
            wal: Some(wal),
            id,
            approximate_size: Arc::new(AtomicUsize::new(0usize)),
        })
    }

    /// Create a memtable from WAL
    pub fn recover_from_wal(id: usize, path: impl AsRef<Path>) -> Result<Self> {
        let mut skiplist = SkipMap::new();
        let wal = Wal::recover(path, &mut skiplist)?;
        Ok(MemTable {
            map: Arc::new(skiplist),
            wal: Some(wal),
            id,
            approximate_size: Arc::new(AtomicUsize::new(0usize)),
        })
    }

    pub fn for_testing_put_slice(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.put(KeySlice::from_slice(key, TS_DEFAULT), value)
    }

    pub fn for_testing_get_slice(&self, key: &[u8]) -> Option<Bytes> {
        self.get(KeySlice::from_slice(key, TS_DEFAULT))
    }

    pub fn for_testing_scan_slice(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> MemTableIterator {
        self.scan(
            map_bound_with_ts(lower, TS_DEFAULT),
            map_bound_with_ts(upper, TS_DEFAULT),
        )
    }

    /// Get a value by key.
    pub fn get(&self, key: KeySlice) -> Option<Bytes> {
        let bytes =
            Bytes::from_static(unsafe { std::mem::transmute::<&[u8], &[u8]>(key.key_ref()) });

        let key = KeyBytes::from_bytes_with_ts(bytes, key.ts());

        self.map.get(&key).map(|entry| entry.value().clone())
    }

    /// Put a key-value pair into the mem-table.
    ///
    /// In week 1, day 1, simply put the key-value pair into the skipmap.
    /// In week 2, day 6, also flush the data to WAL.
    /// In week 3, day 5, modify the function to use the batch API.
    pub fn put(&self, key: KeySlice, value: &[u8]) -> Result<()> {
        let value_bytes = Bytes::copy_from_slice(value);
        let key_bytes = key_slice_to_key_bytes(key);
        self.approximate_size.fetch_add(
            key.key_len() + value.len(),
            std::sync::atomic::Ordering::SeqCst,
        );
        self.map.insert(key_bytes, value_bytes);

        if let Some(wal) = &self.wal {
            wal.put(key, value)?;
        }
        Ok(())
    }

    /// Implement this in week 3, day 5.
    pub fn put_batch(&self, _data: &[(KeySlice, &[u8])]) -> Result<()> {
        unimplemented!()
    }

    pub fn sync_wal(&self) -> Result<()> {
        if let Some(ref wal) = self.wal {
            wal.sync()?;
        }
        Ok(())
    }

    /// Get an iterator over a range of keys.
    pub fn scan(&self, lower: Bound<KeySlice>, upper: Bound<KeySlice>) -> MemTableIterator {
        let (lower, upper) = (map_bound(lower), map_bound(upper));
        let mut iter = MemTableIteratorBuilder {
            map: self.map.clone(),
            iter_builder: |map| map.range((lower, upper)),
            item: (KeyBytes::new(), Bytes::new()),
        }
        .build();

        iter.next().unwrap();
        iter
    }

    /// Flush the mem-table to SSTable. Implement in week 1 day 6.
    pub fn flush(&self, builder: &mut SsTableBuilder) -> Result<()> {
        for entry in self.map.iter() {
            builder.add(entry.key().as_key_slice(), entry.value());
        }

        Ok(())
    }

    pub fn id(&self) -> usize {
        self.id
    }

    pub fn approximate_size(&self) -> usize {
        self.approximate_size
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Only use this function when closing the database
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

type SkipMapRangeIter<'a> = crossbeam_skiplist::map::Range<
    'a,
    KeyBytes,
    (Bound<KeyBytes>, Bound<KeyBytes>),
    KeyBytes,
    Bytes,
>;

/// An iterator over a range of `SkipMap`. This is a self-referential structure and please refer to week 1, day 2
/// chapter for more information.
///
/// This is part of week 1, day 2.
#[self_referencing]
pub struct MemTableIterator {
    /// Stores a reference to the skipmap.
    map: Arc<SkipMap<KeyBytes, Bytes>>,
    /// Stores a skipmap iterator that refers to the lifetime of `MemTableIterator` itself.
    #[borrows(map)]
    #[not_covariant]
    iter: SkipMapRangeIter<'this>,
    /// Stores the current key-value pair.
    item: (KeyBytes, Bytes),
}

impl MemTableIterator {
    fn entry_to_item(entry: Option<Entry<'_, KeyBytes, Bytes>>) -> (KeyBytes, Bytes) {
        entry
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .unwrap_or_else(|| (KeyBytes::new(), Bytes::from_static(&[])))
    }
}

impl StorageIterator for MemTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn value(&self) -> &[u8] {
        &self.borrow_item().1[..]
    }

    fn key(&self) -> KeySlice {
        self.borrow_item().0.as_key_slice()
    }

    fn is_valid(&self) -> bool {
        !self.borrow_item().0.is_empty()
    }

    fn next(&mut self) -> Result<()> {
        let entry = self.with_iter_mut(|iter| MemTableIterator::entry_to_item(iter.next()));
        self.with_item_mut(|x| *x = entry);
        Ok(())
    }
}
