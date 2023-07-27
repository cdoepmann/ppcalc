use serde::{Deserialize, Serialize};
use std::{collections::HashMap, hash::BuildHasherDefault, path::Path};

use ppcalc_metric::{DestinationId, MessageId, SourceId, Trace};

#[derive(Serialize, Deserialize)]
pub struct DeanomizationEntry {
    source: SourceId,
    destination: DestinationId,
    remaining_anonymity_set: u64,
    messages: u64,
    deanomized_at: Option<u64>,
}

pub fn deanonymized_users_over_time(
    source_relationship_anonymity_sets: &HashMap<
        SourceId,
        Vec<(MessageId, Vec<DestinationId>)>,
        BuildHasherDefault<fxhash::FxHasher>,
    >,
    net_trace: &Trace,
) -> Vec<DeanomizationEntry> {
    let mut deanonymization_vec: Vec<DeanomizationEntry> = vec![];
    for (source, messages) in source_relationship_anonymity_sets.iter() {
        let mut remaining_anonymity_set;
        let num_messages = messages.len();
        let last_message = messages.last();
        let Some(last_message) = last_message else {
                println!("skipped.");
                continue;
            };

        remaining_anonymity_set = last_message.1.len();
        if let Some(destination_id) = net_trace.get_destination_mapping().get(&last_message.0) {
            if remaining_anonymity_set != 1 {
                deanonymization_vec.push(DeanomizationEntry {
                    destination: *destination_id,
                    source: source.clone(),
                    remaining_anonymity_set: remaining_anonymity_set as u64,
                    messages: messages.len() as u64,
                    deanomized_at: None,
                });
            } else {
                let mut message_number = messages.len() as u64;
                for (message_id, destinations) in messages.iter().rev() {
                    if destinations.len() == 1 {
                        break;
                    }
                    message_number -= 1;
                }
                deanonymization_vec.push(DeanomizationEntry {
                    source: source.clone(),
                    destination: *destination_id,
                    remaining_anonymity_set: 1,
                    messages: messages.len() as u64,
                    deanomized_at: Some(message_number),
                });
            }
        }
    }

    println!("Deanomized in total: {}", deanonymization_vec.len());
    deanonymization_vec
}
