use fxhash::FxHashMap as HashMap;

use std::cmp::Ordering;
use std::hash::Hash;

use super::MessageId;

/// An ordered set of messages
pub(crate) struct MessageSet {
    messages: Vec<MessageId>,
    sorted: bool,
}

impl MessageSet {
    /// Create a new, empty message set
    pub(crate) fn new() -> MessageSet {
        MessageSet {
            messages: Vec::new(),
            sorted: true,
        }
    }

    /// Insert a message
    pub(crate) fn insert(&mut self, message: MessageId) {
        if let Some(last) = self.messages.last() {
            if *last > message {
                self.sorted = false;
            }
        }
        self.messages.push(message);
    }

    /// Sort the data if necessary
    fn sort(&mut self) {
        if !self.sorted {
            self.messages.sort_unstable();
        }
        self.sorted = true;
    }

    /// Convert this into a regular Vec
    fn into_vec(self) -> Vec<MessageId> {
        self.messages
    }

    /// Split by some function into a hash map of grouped valuex
    pub(crate) fn split_by<G>(self, indicator: impl Fn(&MessageId) -> G) -> HashMap<G, MessageSet>
    where
        G: Eq + Hash,
    {
        let mut result: HashMap<G, MessageSet> = HashMap::default();

        for val in self.messages {
            let key = indicator(&val);
            let entry = result.entry(key).or_default();
            entry.insert(val);
        }

        for set in result.values_mut() {
            set.sort()
        }

        result
    }

    /// Get the number of contained messages
    pub(crate) fn len(&self) -> usize {
        self.messages.len()
    }

    /// Compute the relative set distance (added, overlap) from this set to `other`
    pub(crate) fn distance(&self, other: &MessageSet) -> (usize, usize) {
        assert!(self.sorted);
        assert!(other.sorted);

        let mut added: usize = 0;
        let mut overlap: usize = 0;

        let mut left_iter = self.iter().into_iter();
        let mut left_exhausted = false;

        'outer: for right_val in other.iter() {
            // have to check if this is also present in left side
            if left_exhausted {
                added += 1;
                continue;
            }

            'inner: loop {
                match left_iter.next() {
                    None => {
                        // left finished
                        added += 1;
                        left_exhausted = true;
                        continue 'outer;
                    }
                    Some(left_val) => match left_val.cmp(right_val) {
                        Ordering::Less => {
                            continue 'inner;
                        }
                        Ordering::Equal => {
                            overlap += 1;
                            continue 'outer;
                        }
                        Ordering::Greater => {
                            added += 1;
                            continue 'outer;
                        }
                    },
                }
            }
        }

        (added, overlap)
    }

    pub(crate) fn iter(&self) -> impl IntoIterator<Item = &MessageId> {
        self.messages.iter()
    }
}

impl Default for MessageSet {
    fn default() -> Self {
        MessageSet::new()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    mod common {
        use super::*;

        pub fn test_case(
            set_1: impl IntoIterator<Item = u64>,
            set_2: impl IntoIterator<Item = u64>,
            result: (usize, usize),
        ) {
            let set_1 = {
                let mut set = MessageSet::new();
                for msg in set_1 {
                    set.insert(MessageId::new(msg));
                }
                set
            };
            let set_2 = {
                let mut set = MessageSet::new();
                for msg in set_2 {
                    set.insert(MessageId::new(msg));
                }
                set
            };

            assert_eq!(set_1.distance(&set_2), result);
        }
    }

    #[test]
    fn messageset_distance_1() {
        common::test_case([1, 2, 3, 4, 5], [2, 5, 6], (1, 2));
    }

    #[test]
    fn messageset_distance_2() {
        common::test_case([1, 2, 3, 4, 5], [6, 7], (2, 0));
    }

    #[test]
    fn messageset_distance_3() {
        common::test_case([1, 2, 3, 4, 5], [1, 2, 3, 4, 5], (0, 5));
    }

    #[test]
    fn messageset_distance_4() {
        common::test_case([], [2, 5, 6], (3, 0));
    }

    #[test]
    fn messageset_distance_5() {
        common::test_case([], [], (0, 0));
    }

    #[test]
    fn messageset_distance_6() {
        common::test_case([2, 4, 5], [], (0, 0));
    }
}
