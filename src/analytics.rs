use std::{
    collections::{hash_map::Entry, HashMap},
    fmt::Display,
    ops::Add,
    vec,
};

use time::PrimitiveDateTime;

use crate::trace;

#[derive(PartialEq, PartialOrd, Debug)]
enum EventType {
    AddSourceMessage,
    RemoveSourceMessage,
    AddDestinationMessage,
}
impl Display for EventType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EventType::AddDestinationMessage => f.write_str("AddDestinationMessage"),
            EventType::AddSourceMessage => f.write_str("AddSourceMessage"),
            EventType::RemoveSourceMessage => f.write_str("RemoveSourceMessage"),
        }
    }
}
#[derive(Debug)]
struct ProcessingEvent {
    event_type: EventType,
    ts: PrimitiveDateTime,
    m_id: u64,
    name: String,
}
impl Display for ProcessingEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} {} {} {}",
            self.event_type, self.ts, self.m_id, self.name
        )
    }
}
pub fn compute_message_anonymity_sets(
    trace: &trace::Trace,
    min_delay: i64,
    max_delay: i64,
) -> Result<HashMap<u64, Vec<u64>>, Box<dyn std::error::Error>> {
    let event_queue = compute_event_queue(trace, min_delay, max_delay)?;
    let mut current_message_set: Vec<u64> = vec![];
    let mut anonymity_sets: HashMap<u64, Vec<u64>> = HashMap::new();

    for event in event_queue {
        match event.event_type {
            EventType::AddSourceMessage => current_message_set.push(event.m_id),
            EventType::RemoveSourceMessage => current_message_set.retain(|x| *x != event.m_id),
            EventType::AddDestinationMessage => {
                for m_id in current_message_set.iter() {
                    match anonymity_sets.get_mut(&m_id) {
                        Some(set) => set.push(event.m_id),
                        None => {
                            anonymity_sets.insert(*m_id, vec![event.m_id]);
                        }
                    };
                }
            }
        };
        print!("Current Message set: ");
        for id in current_message_set.iter() {
            print!("{} ", id);
        }
        println!("");
    }
    Ok(anonymity_sets)
}

fn compute_destination_mapping(trace: &trace::Trace) -> HashMap<u64, String> {
    let mut destination_mapping = HashMap::new();
    for entry in trace.entries.iter() {
        destination_mapping.insert(entry.m_id, entry.destination_name.clone());
    }
    destination_mapping
}

fn compute_source_message_mapping(trace: &trace::Trace) -> HashMap<String, Vec<u64>> {
    let mut source_message_mapping = HashMap::new();
    for trace_entry in trace.entries.iter() {
        match source_message_mapping.entry(trace_entry.source_name.clone()) {
            Entry::Vacant(e) => {
                e.insert(vec![trace_entry.m_id]);
            }
            Entry::Occupied(mut e) => e.get_mut().push(trace_entry.m_id),
        }
    }
    source_message_mapping
}

pub fn compute_relationship_anonymity(
    trace: &trace::Trace,
    min_delay: i64,
    max_delay: i64,
) -> Result<HashMap<String, Vec<(u64, Vec<String>)>>, Box<dyn std::error::Error>> {
    let source_message_mapping = compute_source_message_mapping(&trace);
    let destination_mapping = compute_destination_mapping(&trace);
    let message_anonymity_set =
        compute_message_anonymity_sets(&trace, min_delay, max_delay).unwrap();
    let mut relationship_anonymity_sets = HashMap::new();
    for (source, source_messages) in source_message_mapping {
        let mut anonymity_sets: Vec<(u64, Vec<String>)> = vec![];
        let mut selected_messages: Vec<u64> = vec![];
        let mut current_relationship_anonymity_set = vec![];
        if let Some((first_message, remaining_messages)) = source_messages.split_first() {
            /* Get Message Anonymity Set */
            let mas = message_anonymity_set.get(first_message).unwrap();
            for received_message in mas {
                println!(
                    "Processing received message: {} for {}",
                    received_message, first_message
                );
                // Determine Destination
                let destination = destination_mapping
                    .get(received_message)
                    .ok_or("Destination not found")?;
                // Check if Destination is already in current set
                if current_relationship_anonymity_set.contains(destination) {
                    println!(
                        "Current relationship anonymity set already contains this destination"
                    );
                    println!("{:?}", current_relationship_anonymity_set);
                    continue;
                }
                // Check if this message has already been "used" in a previous round
                if !selected_messages.contains(received_message) {
                    println!("Added unselected message");
                    selected_messages.push(*received_message);
                    current_relationship_anonymity_set.push(destination.clone());
                }
            }
            anonymity_sets.push((*first_message, current_relationship_anonymity_set));

            for message in remaining_messages {
                let mut current_relationship_anonymity_set = vec![];
                let mas = message_anonymity_set.get(message).unwrap();
                for received_message in mas {
                    // Determine Destination
                    let destination = destination_mapping
                        .get(received_message)
                        .ok_or("Destination not found")?;
                    // Check if Destination is in privous set
                    let (_, previous_relationship_anonymity_set) = anonymity_sets
                        .last()
                        .ok_or("There is no last anonymity set")?;
                    if !previous_relationship_anonymity_set.contains(destination) {
                        continue;
                    }
                    // Check if Destination is already in current set
                    if current_relationship_anonymity_set.contains(destination) {
                        continue;
                    }
                    // Check if this message has already been "used" in a previous round
                    if !selected_messages.contains(received_message) {
                        selected_messages.push(*received_message);
                        current_relationship_anonymity_set.push(destination.clone());
                    }
                }
                anonymity_sets.push((*message, current_relationship_anonymity_set));
            }
            relationship_anonymity_sets.insert(source, anonymity_sets);
        }
    }
    Ok(relationship_anonymity_sets)
}
fn compute_event_queue(
    trace: &trace::Trace,
    min_delay: i64,
    max_delay: i64,
) -> Result<Vec<ProcessingEvent>, Box<dyn std::error::Error>> {
    let min_delay = time::Duration::milliseconds(min_delay);
    let max_delay = time::Duration::milliseconds(max_delay) + time::Duration::nanoseconds(1);
    let mut event_queue: Vec<ProcessingEvent> = vec![];
    for entry in trace.entries.iter() {
        event_queue.push(ProcessingEvent {
            event_type: EventType::AddSourceMessage,
            ts: entry.source_timestamp.add(min_delay),
            m_id: entry.m_id,
            name: entry.source_name.clone(),
        });
        event_queue.push(ProcessingEvent {
            event_type: EventType::RemoveSourceMessage,
            ts: entry.source_timestamp.add(max_delay),
            m_id: entry.m_id,
            name: entry.source_name.clone(),
        });
        event_queue.push(ProcessingEvent {
            event_type: EventType::AddDestinationMessage,
            ts: entry.destination_timestamp,
            m_id: entry.m_id,
            name: entry.destination_name.clone(),
        });
    }
    event_queue.sort_by(|a, b| a.ts.cmp(&b.ts));
    for event in event_queue.iter() {
        println!("{}", event);
    }
    Ok(event_queue)
}
