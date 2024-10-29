#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::cmp::{self};
use std::collections::binary_heap::PeekMut;
use std::collections::BinaryHeap;

use anyhow::{Ok, Result};

use crate::key::KeySlice;

use super::StorageIterator;

struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>);

impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.1
            .key()
            .cmp(&other.1.key())
            .then(self.0.cmp(&other.0))
            .reverse()
    }
}

/// Merge multiple iterators of the same type. If the same key occurs multiple times in some
/// iterators, prefer the one with smaller index.
pub struct MergeIterator<I: StorageIterator> {
    iters: BinaryHeap<HeapWrapper<I>>,
    current: Option<HeapWrapper<I>>,
}

impl<I: StorageIterator> MergeIterator<I> {
    pub fn create(iters: Vec<Box<I>>) -> Self {
        let mut heap = BinaryHeap::from_iter(
            iters
                .into_iter()
                .enumerate()
                .filter(|val| val.1.is_valid())
                .map(|val| HeapWrapper(val.0, val.1)),
        );

        let current = heap.pop();

        MergeIterator {
            iters: heap,
            current: current,
        }
    }
}

impl<I: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>> StorageIterator
    for MergeIterator<I>
{
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        self.current.as_ref().unwrap().1.key()
    }

    fn value(&self) -> &[u8] {
        self.current.as_ref().unwrap().1.value()
    }

    fn is_valid(&self) -> bool {
        self.current.is_some()
    }

    fn next(&mut self) -> Result<()> {
        // call next() on entries with the same key
        let cur = self.current.as_ref().unwrap();

        loop {
            let mut cont = false;
            if let Some(mut inner) = self.iters.peek_mut() {
                if inner.1.key() == cur.1.key() {
                    cont = true;
                    if let Err(e) = inner.1.next() {
                        PeekMut::pop(inner);
                        break Err(e);
                    }

                    if !inner.1.is_valid() {
                        PeekMut::pop(inner);
                    }
                }
            };

            if !cont {
                break (Ok(()));
            }
        }?;

        let mut next: Option<HeapWrapper<I>> = None;

        std::mem::swap(&mut self.current, &mut next);
        if let Some(mut next) = next {
            next.1.next()?;

            if next.1.is_valid() {
                self.iters.push(next);
            }
        };

        self.current = self.iters.pop();

        Ok(())
    }
}
