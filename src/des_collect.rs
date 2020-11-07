use serde::Deserializer;

pub struct DesCollect<'a>(pub &'a mut super::map::Map);

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
        let phrase_counts = &mut *self.0;
        while let Some((phrase, count)) = map.next_entry()? {
            phrase_counts.add(phrase, count);
        }

        Ok(())
    }
}
