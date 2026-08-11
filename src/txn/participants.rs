use std::collections::BTreeSet;

/// A deterministic participant set shared by transaction prepare and finalize.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct ParticipantSet<P: Ord>(BTreeSet<P>);

impl<P: Ord> ParticipantSet<P> {
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = &P> {
        self.0.iter()
    }

    pub fn insert(&mut self, participant: P) {
        self.0.insert(participant);
    }

    pub fn contains(&self, participant: &P) -> bool {
        self.0.contains(participant)
    }

    pub fn retain_unresolved(&mut self, delivered: &Self) {
        self.0
            .retain(|participant| !delivered.0.contains(participant));
    }
}

impl<P: Ord> FromIterator<P> for ParticipantSet<P> {
    fn from_iter<T: IntoIterator<Item = P>>(iter: T) -> Self {
        Self(iter.into_iter().collect())
    }
}

impl<P: Ord> IntoIterator for ParticipantSet<P> {
    type Item = P;
    type IntoIter = std::collections::btree_set::IntoIter<P>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

#[cfg(test)]
mod tests {
    use super::ParticipantSet;

    #[test]
    fn participants_are_unique_ordered_and_retry_only_unresolved() {
        let mut participants = ["b", "a", "b"]
            .into_iter()
            .map(str::to_string)
            .collect::<ParticipantSet<_>>();
        let delivered = ["a".to_string()].into_iter().collect();
        participants.retain_unresolved(&delivered);
        assert_eq!(participants.into_iter().collect::<Vec<_>>(), ["b"]);
    }
}
