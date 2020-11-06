use super::config::WORD_COUNT;
use hashbrown::HashMap;
use serde::Deserializer;

use std::hash::{BuildHasher, Hash, Hasher};

pub struct DesCollect<'a>(pub &'a mut HashMap<[Box<str>; WORD_COUNT], u32>);

impl<'de> serde::de::DeserializeSeed<'de> for DesCollect<'_> {
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(self)
    }
}

impl<'de> serde::de::Visitor<'de> for DesCollect<'_> {
    type Value = ();

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(formatter, "a map")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::MapAccess<'de>,
    {
        let search = &mut *self.0;
        while let Some((word, value)) = map.next_entry()? {
            let _: [&str; WORD_COUNT] = word;
            let _: u32 = value;

            let mut hasher = search.hasher().build_hasher();
            word.hash(&mut hasher);
            let hash = hasher.finish();

            *search
                .raw_entry_mut()
                .from_hash(hash, |item| {
                    item.iter()
                        .map(AsRef::<str>::as_ref)
                        .eq(word.iter().copied())
                })
                .or_insert_with(|| (crate::config::to_owned(word), 0))
                .1 += value;
        }

        Ok(())
    }
}
