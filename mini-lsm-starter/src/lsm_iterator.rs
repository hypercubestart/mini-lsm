use std::ops::Bound;

use anyhow::{Error, Result};
use bytes::Bytes;

use crate::{
    iterators::{
        concat_iterator::SstConcatIterator, merge_iterator::MergeIterator,
        two_merge_iterator::TwoMergeIterator, StorageIterator,
    },
    mem_table::MemTableIterator,
    table::SsTableIterator,
};

/// Represents the internal type for an LSM iterator. This type will be changed across the tutorial for multiple times.
type LsmIteratorInner = TwoMergeIterator<
    TwoMergeIterator<MergeIterator<MemTableIterator>, MergeIterator<SsTableIterator>>,
    MergeIterator<SstConcatIterator>,
>;

pub struct LsmIterator {
    inner: LsmIteratorInner,
    end_bound: Bound<Bytes>,
    is_valid: bool,
    prev_key: Vec<u8>,
}

impl LsmIterator {
    pub(crate) fn new(iter: LsmIteratorInner, end_bound: Bound<Bytes>) -> Result<Self> {
        let mut iter = Self {
            is_valid: iter.is_valid(),
            inner: iter,
            end_bound,
            prev_key: Vec::new(),
        };

        iter.move_to_non_delete()?;

        Ok(iter)
    }

    fn next_inner(&mut self) -> Result<()> {
        self.prev_key = self.key().to_vec();
        self.inner.next()?;

        if !self.inner.is_valid() {
            self.is_valid = false;
            return Ok(());
        }

        match self.end_bound.as_ref() {
            Bound::Included(end_bound) => self.is_valid = self.inner.key().key_ref() <= end_bound,

            Bound::Excluded(end_bound) => self.is_valid = self.inner.key().key_ref() < end_bound,

            Bound::Unbounded => (),
        };
        Ok(())
    }

    fn move_to_non_delete(&mut self) -> Result<()> {
        while self.is_valid() && (self.value().is_empty() || self.key() == self.prev_key) {
            self.next_inner()?;
        }

        Ok(())
    }
}

impl StorageIterator for LsmIterator {
    type KeyType<'a> = &'a [u8];

    fn is_valid(&self) -> bool {
        self.is_valid
    }

    fn key(&self) -> &[u8] {
        self.inner.key().key_ref()
    }

    fn value(&self) -> &[u8] {
        self.inner.value()
    }

    fn next(&mut self) -> Result<()> {
        self.next_inner()?;
        self.move_to_non_delete()
    }

    fn num_active_iterators(&self) -> usize {
        self.inner.num_active_iterators()
    }
}

/// A wrapper around existing iterator, will prevent users from calling `next` when the iterator is
/// invalid. If an iterator is already invalid, `next` does not do anything. If `next` returns an error,
/// `is_valid` should return false, and `next` should always return an error.
pub struct FusedIterator<I: StorageIterator> {
    iter: I,
    has_errored: bool,
}

impl<I: StorageIterator> FusedIterator<I> {
    pub fn new(iter: I) -> Self {
        Self {
            iter,
            has_errored: false,
        }
    }
}

impl<I: StorageIterator> StorageIterator for FusedIterator<I> {
    type KeyType<'a> = I::KeyType<'a> where Self: 'a;

    fn is_valid(&self) -> bool {
        !self.has_errored && self.iter.is_valid()
    }

    fn key(&self) -> Self::KeyType<'_> {
        if !self.is_valid() {
            panic!("invalid access to the underlying iterator")
        }
        self.iter.key()
    }

    fn value(&self) -> &[u8] {
        if !self.is_valid() {
            panic!("invalid access to the underlying iterator")
        }
        self.iter.value()
    }

    fn next(&mut self) -> Result<()> {
        if self.has_errored {
            Err(Error::msg("called next() on FusedIterator"))
        } else if self.iter.is_valid() {
            match self.iter.next() {
                Ok(()) => Ok(()),
                Err(e) => {
                    self.has_errored = true;
                    Err(e)
                }
            }
        } else {
            Ok(())
        }
    }

    fn num_active_iterators(&self) -> usize {
        self.iter.num_active_iterators()
    }
}
