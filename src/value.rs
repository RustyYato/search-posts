use serde::de::*;
use std::fmt;

use hashbrown::HashMap;
use std::borrow::Cow;

pub enum Value<'de> {
    Ignored,
    String(Cow<'de, str>),
    Array(Vec<Value<'de>>),
    Object(HashMap<Cow<'de, str>, Value<'de>>),
}

impl Value<'_> {
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::String(s) => Some(s),
            _ => None,
        }
    }
}

impl<'de: 'a, 'a> Deserialize<'de> for Value<'a> {
    #[inline]
    fn deserialize<D>(deserializer: D) -> Result<Value<'a>, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct ValueVisitor;

        impl<'de> Visitor<'de> for ValueVisitor {
            type Value = Value<'de>;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("any valid JSON value")
            }

            #[inline]
            fn visit_bool<E>(self, _: bool) -> Result<Value<'de>, E> {
                Ok(Value::Ignored)
            }

            #[inline]
            fn visit_i64<E>(self, _: i64) -> Result<Value<'de>, E> {
                Ok(Value::Ignored)
            }

            #[inline]
            fn visit_u64<E>(self, _: u64) -> Result<Value<'de>, E> {
                Ok(Value::Ignored)
            }

            #[inline]
            fn visit_f64<E>(self, _: f64) -> Result<Value<'de>, E> {
                Ok(Value::Ignored)
            }

            fn visit_borrowed_str<E>(self, value: &'de str) -> Result<Value<'de>, E>
            where
                E: serde::de::Error,
            {
                Ok(Value::String(value.into()))
            }

            #[inline]
            fn visit_str<E>(self, value: &str) -> Result<Value<'de>, E>
            where
                E: serde::de::Error,
            {
                self.visit_string(value.into())
            }

            #[inline]
            fn visit_string<E>(self, value: String) -> Result<Value<'de>, E> {
                Ok(Value::String(value.into()))
            }

            #[inline]
            fn visit_none<E>(self) -> Result<Value<'de>, E> {
                Ok(Value::Ignored)
            }

            #[inline]
            fn visit_some<D>(self, deserializer: D) -> Result<Value<'de>, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                Deserialize::deserialize(deserializer)
            }

            #[inline]
            fn visit_unit<E>(self) -> Result<Value<'de>, E> {
                Ok(Value::Ignored)
            }

            #[inline]
            fn visit_seq<V>(self, mut visitor: V) -> Result<Value<'de>, V::Error>
            where
                V: SeqAccess<'de>,
            {
                let mut vec = Vec::new();

                while let Some(elem) = visitor.next_element()? {
                    vec.push(elem);
                }

                Ok(Value::Array(vec))
            }

            fn visit_map<V>(self, mut visitor: V) -> Result<Value<'de>, V::Error>
            where
                V: MapAccess<'de>,
            {
                let mut values = HashMap::new();

                while let Some((key, value)) = visitor.next_entry()? {
                    values.insert(key, value);
                }

                Ok(Value::Object(values))
            }
        }

        deserializer.deserialize_any(ValueVisitor)
    }
}
