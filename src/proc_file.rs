use serde::de::*;
use std::borrow::Cow;
use std::fmt;
use unicode_segmentation::UnicodeSegmentation;

impl<'a> ProcFile<'a> {
    pub fn new(phrase_counts: &'a mut crate::Map) -> Self {
        Self { phrase_counts }
    }
}

pub struct ProcFile<'a> {
    phrase_counts: &'a mut crate::Map,
}

#[derive(Clone, Copy)]
enum Location {
    Outside,
    User,
    Post,
    Unknown,
}

struct ProcFileValue<'a> {
    phrase_counts: &'a mut crate::Map,
    location: Location,
}

impl<'de> DeserializeSeed<'de> for ProcFile<'_> {
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer
            .deserialize_map(ProcFileValue {
                phrase_counts: self.phrase_counts,
                location: Location::Outside,
            })
            .map(drop)
    }
}

impl<'de> DeserializeSeed<'de> for ProcFileValue<'_> {
    type Value = Option<Cow<'de, str>>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(self)
    }
}

impl<'de> Visitor<'de> for ProcFileValue<'_> {
    type Value = Option<Cow<'de, str>>;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("any valid JSON value")
    }

    #[inline]
    fn visit_bool<E>(self, _: bool) -> Result<Self::Value, E> {
        Ok(None)
    }

    #[inline]
    fn visit_i64<E>(self, _: i64) -> Result<Self::Value, E> {
        Ok(None)
    }

    #[inline]
    fn visit_u64<E>(self, _: u64) -> Result<Self::Value, E> {
        Ok(None)
    }

    #[inline]
    fn visit_f64<E>(self, _: f64) -> Result<Self::Value, E> {
        Ok(None)
    }

    fn visit_borrowed_str<E>(self, value: &'de str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(Some(value.into()))
    }

    #[inline]
    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        self.visit_string(value.into())
    }

    #[inline]
    fn visit_string<E>(self, value: String) -> Result<Self::Value, E> {
        Ok(Some(value.into()))
    }

    #[inline]
    fn visit_none<E>(self) -> Result<Self::Value, E> {
        Ok(None)
    }

    #[inline]
    fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Deserialize::deserialize(deserializer)
    }

    #[inline]
    fn visit_unit<E>(self) -> Result<Self::Value, E> {
        Ok(None)
    }

    #[inline]
    fn visit_seq<V>(self, mut visitor: V) -> Result<Self::Value, V::Error>
    where
        V: SeqAccess<'de>,
    {
        while let Some(_) = visitor.next_element_seed(ProcFileValue {
            phrase_counts: self.phrase_counts,
            location: self.location,
        })? {}

        Ok(None)
    }

    fn visit_map<V>(self, mut visitor: V) -> Result<Self::Value, V::Error>
    where
        V: MapAccess<'de>,
    {
        while let Some(key) = visitor.next_key()? {
            let _: Cow<str> = key;
            let key = key.as_ref();

            let location = match (self.location, key) {
                (Location::Outside, "users") => Location::User,
                (Location::User, "posts") => Location::Post,
                (Location::Outside, _) | (Location::User, _) => Location::Unknown,
                (location, _) => location,
            };

            if let Location::Unknown = location {
                let _: IgnoredAny = visitor.next_value()?;
                continue;
            }

            let value = visitor.next_value_seed(ProcFileValue {
                phrase_counts: self.phrase_counts,
                location,
            })?;

            match location {
                Location::Post => (),
                _ => continue,
            }

            if key == "text" || key == "description" {
                value
                    .unwrap()
                    .as_ref()
                    .unicode_sentences()
                    .map(UnicodeSegmentation::unicode_words)
                    .flat_map(itertools::Itertools::tuple_windows)
                    .map(crate::config::get_chunks)
                    .for_each(|chunk| crate::insert_value(chunk, 1, self.phrase_counts));
            }
        }

        Ok(None)
    }
}
