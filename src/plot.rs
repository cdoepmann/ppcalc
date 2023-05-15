use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};

#[derive(Serialize, Deserialize)]
pub struct PlotFormat {
    pub source_message_anonymity_sets: HashMap<u64, Vec<String>>,
    pub source_message_map: HashMap<String, Vec<u64>>,
}

#[derive(Serialize, Deserialize)]
pub struct DeanomizationEntry {
    source: String,
    remaining_anonymity_set: u64,
    messages: u64,
    deanomized_at: Option<u64>,
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

    pub fn deanonymized_users_over_time(self: &Self) -> Vec<DeanomizationEntry> {
        let mut deanonymization_vec: Vec<DeanomizationEntry> = vec![];
        for (source, messages) in self.source_message_map.iter() {
            let mut message_number = 1;
            let mut anonymity_set;
            let num_messages = messages.len();
            let last_message = messages.last();
            let Some(last_message) = last_message else {
                println!("skipped.");
                continue;
            };

            anonymity_set = self
                .source_message_anonymity_sets
                .get(&last_message)
                .unwrap();
            if anonymity_set.len() != 1 {
                deanonymization_vec.push(DeanomizationEntry {
                    source: source.clone(),
                    remaining_anonymity_set: anonymity_set.len() as u64,
                    messages: messages.len() as u64,
                    deanomized_at: None,
                });
            } else {
                let mut message_number = messages.len() as u64;
                for message_id in messages.iter().rev() {
                    anonymity_set = self.source_message_anonymity_sets.get(message_id).unwrap();
                    if anonymity_set.len() != 1 {
                        break;
                    }
                    message_number -= 1;
                }
                deanonymization_vec.push(DeanomizationEntry {
                    source: source.clone(),
                    remaining_anonymity_set: 1,
                    messages: messages.len() as u64,
                    deanomized_at: Some(message_number),
                });
            }
        }

        println!("Deanomized in total: {}", deanonymization_vec.len());
        deanonymization_vec
    }

    pub fn anonymity_set_size_over_time(self: &Self) -> BTreeMap<u64, usize> {
        let mut anonymity_set_difference_map: BTreeMap<u64, usize> = BTreeMap::new();
        let mut total_anonymity_set_size = self
            .source_message_map
            .iter()
            .map(|(_source, messages)| {
                self.source_message_anonymity_sets
                    .get(messages.first().unwrap())
                    .unwrap()
                    .len()
            })
            .reduce(|first_message, sum| first_message + sum)
            .unwrap();
        println!("TOtal anonymity set size: {total_anonymity_set_size}");

        for (source, messages) in self.source_message_map.iter() {
            messages
                .iter()
                .map(|m| (*m, self.source_message_anonymity_sets.get(m).unwrap().len()))
                .collect::<Vec<(u64, usize)>>()
                .windows(2)
                .map(|a| (a[0].0, a[0].1 - a[1].1))
                .for_each(|(id, diff)| {
                    anonymity_set_difference_map.insert(id, diff);
                });
            println!("Processed source: {source}");
        }
        let mut anonymity_set_size_map: BTreeMap<u64, usize> = BTreeMap::new();
        for (id, difference) in anonymity_set_difference_map {
            total_anonymity_set_size -= difference;
            anonymity_set_size_map.insert(id, total_anonymity_set_size);
        }

        anonymity_set_size_map
    }
    pub fn write_plot(self: &Self, path: String) {
        std::fs::write(path, serde_json::to_string_pretty(&self).unwrap()).unwrap();
    }
}