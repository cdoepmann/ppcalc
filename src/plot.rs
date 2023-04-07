use std::collections::{BTreeMap, HashMap};

pub struct PlotFormat {
    pub source_message_anonymity_sets: HashMap<u64, Vec<String>>,
    pub source_message_map: HashMap<String, Vec<u64>>,
}

impl PlotFormat {
    pub fn new(
        source_relationship_anonymity_sets: HashMap<String, Vec<(u64, Vec<String>)>>,
    ) -> Self {
        let mut source_message_map: HashMap<String, Vec<u64>> = HashMap::new();
        let mut source_message_anonymity_sets: HashMap<u64, Vec<String>> = HashMap::new();

        for (source, mas) in source_relationship_anonymity_sets.into_iter() {
            let mut message_list: Vec<u64> = vec![];
            for (id, destinations) in mas.into_iter() {
                message_list.push(id);
                source_message_anonymity_sets.insert(id, destinations);
            }
            source_message_map.insert(source.to_string(), message_list);
        }
        PlotFormat {
            source_message_anonymity_sets,
            source_message_map,
        }
    }

    pub fn deanonymized_users_over_time(self: &Self) {
        let mut deanonymization_map: HashMap<u64, String> = HashMap::new();
        for (source, messages) in self.source_message_map.iter() {
            let mut message_number = 1;
            for message_id in messages {
                let anonymity_set = self.source_message_anonymity_sets.get(message_id).unwrap();
                if anonymity_set.len() == 1 {
                    deanonymization_map.insert(*message_id, source.clone());
                    println!(
                        "Deanonymized : {source} at {message_id} after {message_number} messages"
                    );
                    break;
                }
                message_number += 1;
            }
        }
        println!("Deanomized in total: {}", deanonymization_map.len());
    }

    pub fn anonymity_set_size_over_time(self: &Self) {
        let mut anonymity_set_difference_map: BTreeMap<u64, usize> = BTreeMap::new();
        for (source, messages) in self.source_message_map.iter() {
            let mut anonymity_set_size = self.source_message_anonymity_sets.len();
            for message_id in messages {
                let anonymity_set = self.source_message_anonymity_sets.get(message_id).unwrap();
                let diff = anonymity_set_size - anonymity_set.len();
                anonymity_set_difference_map.insert(*message_id, diff);
                anonymity_set_size = anonymity_set.len();
            }
        }
        let mut anonymity_set_size_map: BTreeMap<u64, usize> = BTreeMap::new();
        let mut total_anonymity_set_size =
            self.source_message_anonymity_sets.len() * self.source_message_anonymity_sets.len();

        for (id, difference) in anonymity_set_difference_map {
            total_anonymity_set_size -= difference;
            anonymity_set_size_map.insert(id, total_anonymity_set_size);
            println!("{total_anonymity_set_size}");
        }
    }
}
