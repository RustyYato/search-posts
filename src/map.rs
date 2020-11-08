use ahash::RandomState;
use hashbrown::{hash_map::RawEntryMut, HashMap};
use std::iter::Flatten;
use std::vec::IntoIter;

use std::hash::{BuildHasher, Hash, Hasher};

#[derive(Clone, Copy)]
pub struct NoHash;

pub struct Map {
    inner: Box<[HashMap<crate::PhraseBuf, u32, NoHash>]>,
    hasher: RandomState,
}

impl Map {
    pub fn new() -> Self {
        Self {
            hasher: RandomState::new(),
            inner: vec![HashMap::with_hasher(NoHash); 64].into_boxed_slice(),
        }
    }

    fn hash<H: Hash>(hasher: &RandomState, value: &H) -> (u64, usize) {
        let mut hasher = hasher.build_hasher();
        value.hash(&mut hasher);
        let hash = hasher.finish();
        let index = (hash & (64 - 1)) as usize;
        (hash, index)
    }

    pub fn add(&mut self, phrase: crate::Phrase, value: u32) {
        let hasher = &self.hasher;
        let (hash, index) = Self::hash(hasher, &phrase);

        match self.inner[index].raw_entry_mut().from_hash(hash, |key| {
            key.iter()
                .map(AsRef::<str>::as_ref)
                .eq(phrase.iter().copied())
        }) {
            RawEntryMut::Occupied(entry) => *entry.into_mut() += value,
            RawEntryMut::Vacant(entry) => {
                entry.insert_with_hasher(hash, crate::config::to_owned(phrase), value, |x| {
                    Self::hash(hasher, x).0
                });
            }
        }
    }

    pub fn add_owned(&mut self, phrase: crate::PhraseBuf, value: u32) {
        let hasher = &self.hasher;
        let (hash, index) = Self::hash(hasher, &phrase);

        match self.inner[index]
            .raw_entry_mut()
            .from_hash(hash, |key| *key == phrase)
        {
            RawEntryMut::Occupied(entry) => *entry.into_mut() += value,
            RawEntryMut::Vacant(entry) => {
                entry.insert_with_hasher(hash, phrase, value, |x| Self::hash(hasher, x).0);
            }
        }
    }
}

impl IntoIterator for Map {
    type IntoIter = Flatten<IntoIter<HashMap<crate::PhraseBuf, u32, NoHash>>>;
    type Item = (crate::PhraseBuf, u32);

    fn into_iter(self) -> Self::IntoIter {
        self.inner.into_vec().into_iter().flatten()
    }
}
