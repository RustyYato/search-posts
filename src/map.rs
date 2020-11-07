use ahash::RandomState;
use hashbrown::HashMap;
use std::iter::Flatten;
use std::vec::IntoIter;

use std::hash::{BuildHasher, Hash, Hasher};

pub struct Map {
    inner: Box<[HashMap<crate::PhraseBuf, u32>]>,
    hasher: RandomState,
}

impl Map {
    pub fn new() -> Self {
        Self {
            hasher: RandomState::new(),
            inner: vec![HashMap::new(); 64].into_boxed_slice(),
        }
    }

    fn hash<H: Hash>(&self, value: &H) -> (u64, usize) {
        let mut hasher = self.hasher.build_hasher();
        value.hash(&mut hasher);
        let hash = hasher.finish();
        let index = (hash & (64 - 1)) as usize;
        (hash, index)
    }

    pub fn add(&mut self, phrase: crate::Phrase, value: u32) {
        let (hash, index) = self.hash(&phrase);

        *self.inner[index]
            .raw_entry_mut()
            .from_hash(hash, |key| {
                key.iter()
                    .map(AsRef::<str>::as_ref)
                    .eq(phrase.iter().copied())
            })
            .or_insert_with(|| (crate::config::to_owned(phrase), 0))
            .1 += value;
    }

    pub fn add_owned(&mut self, phrase: crate::PhraseBuf, value: u32) {
        let (hash, index) = self.hash(&phrase);

        *self.inner[index]
            .raw_entry_mut()
            .from_hash(hash, |key| *key == phrase)
            .or_insert_with(|| (phrase, 0))
            .1 += value;
    }
}

impl IntoIterator for Map {
    type IntoIter = Flatten<IntoIter<HashMap<crate::PhraseBuf, u32>>>;
    type Item = (crate::PhraseBuf, u32);

    fn into_iter(self) -> Self::IntoIter {
        self.inner.into_vec().into_iter().flatten()
    }
}
