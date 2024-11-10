#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use anyhow::Result;

use super::StorageIterator;

/// Merges two iterators of different types into one. If the two iterators have the same key, only
/// produce the key once and prefer the entry from A.

enum WhichIterator {
    First,
    Second,
    Invalid,
}
pub struct TwoMergeIterator<A: StorageIterator, B: StorageIterator> {
    a: A,
    b: B,
    cur: WhichIterator,
}

impl<
        A: 'static + StorageIterator,
        B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
    > TwoMergeIterator<A, B>
{
    fn which_iterator(a: &A, b: &B) -> WhichIterator {
        if a.is_valid() && b.is_valid() {
            if a.key() <= b.key() {
                WhichIterator::First
            } else {
                WhichIterator::Second
            }
        } else if a.is_valid() {
            WhichIterator::First
        } else if b.is_valid() {
            WhichIterator::Second
        } else {
            WhichIterator::Invalid
        }
    }

    pub fn create(a: A, b: B) -> Result<Self> {
        let cur = TwoMergeIterator::which_iterator(&a, &b);

        let res = Self { a, b, cur };
        Ok(res)
    }
}

impl<
        A: 'static + StorageIterator,
        B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
    > StorageIterator for TwoMergeIterator<A, B>
{
    type KeyType<'a> = A::KeyType<'a>;

    fn key(&self) -> Self::KeyType<'_> {
        match self.cur {
            WhichIterator::First => self.a.key(),
            WhichIterator::Second => self.b.key(),
            WhichIterator::Invalid => panic!("key() called on invalid iterator"),
        }
    }

    fn value(&self) -> &[u8] {
        match self.cur {
            WhichIterator::First => self.a.value(),
            WhichIterator::Second => self.b.value(),
            WhichIterator::Invalid => panic!("key() called on invalid iterator"),
        }
    }

    fn is_valid(&self) -> bool {
        !matches!(self.cur, WhichIterator::Invalid)
    }

    fn next(&mut self) -> Result<()> {
        match self.cur {
            WhichIterator::First => {
                while self.b.is_valid() && self.b.key() == self.a.key() {
                    self.b.next()?;
                }
                self.a.next()?;
            }
            WhichIterator::Second => {
                self.b.next()?;
            }
            WhichIterator::Invalid => (),
        }

        let cur = TwoMergeIterator::which_iterator(&self.a, &self.b);
        self.cur = cur;
        Ok(())
    }
}
