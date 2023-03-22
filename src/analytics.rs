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
) -> Result<(HashMap<u64, Vec<u64>>, HashMap<u64, Vec<u64>>), Box<dyn std::error::Error>> {
    let event_queue = compute_event_queue(trace, min_delay, max_delay)?;
    let mut current_source_message_set: Vec<u64> = vec![];
    let mut source_message_anonymity_sets: HashMap<u64, Vec<u64>> = HashMap::new();
    let mut destination_message_anonymity_sets: HashMap<u64, Vec<u64>> = HashMap::new();

    for event in event_queue {
        match event.event_type {
            EventType::AddSourceMessage => current_source_message_set.push(event.m_id),
            EventType::RemoveSourceMessage => {
                current_source_message_set.retain(|x| *x != event.m_id)
            }
            EventType::AddDestinationMessage => {
                for m_id in current_source_message_set.iter() {
                    match source_message_anonymity_sets.get_mut(&m_id) {
                        Some(set) => set.push(event.m_id),
                        None => {
                            source_message_anonymity_sets.insert(*m_id, vec![event.m_id]);
                        }
                    };
                }
                destination_message_anonymity_sets
                    .insert(event.m_id, current_source_message_set.clone());
            }
        };
    }
    Ok((
        source_message_anonymity_sets,
        destination_message_anonymity_sets,
    ))
}

fn compute_destination_mapping(trace: &trace::Trace) -> HashMap<u64, String> {
    let mut destination_mapping = HashMap::new();
    for entry in trace.entries.iter() {
        destination_mapping.insert(entry.m_id, entry.destination_name.clone());
    }
    destination_mapping
}

fn compute_source_mapping(trace: &trace::Trace) -> HashMap<u64, String> {
    let mut source_mapping = HashMap::new();
    for entry in trace.entries.iter() {
        source_mapping.insert(entry.m_id, entry.source_name.clone());
    }
    source_mapping
}

fn compute_source_and_destination_mapping(
    trace: &trace::Trace,
) -> (HashMap<u64, String>, HashMap<u64, String>) {
    let mut source_mapping = HashMap::new();
    let mut destination_mapping = HashMap::new();
    for entry in trace.entries.iter() {
        source_mapping.insert(entry.m_id, entry.source_name.clone());
        destination_mapping.insert(entry.m_id, entry.destination_name.clone());
    }
    (source_mapping, destination_mapping)
}

fn compute_source_and_destination_message_mapping(
    trace: &trace::Trace,
) -> (HashMap<String, Vec<u64>>, HashMap<String, Vec<u64>>) {
    let mut source_message_mapping = HashMap::new();
    let mut destination_message_mapping = HashMap::new();
    for trace_entry in trace.entries.iter() {
        match source_message_mapping.entry(trace_entry.source_name.clone()) {
            Entry::Vacant(e) => {
                e.insert(vec![trace_entry.m_id]);
            }
            Entry::Occupied(mut e) => e.get_mut().push(trace_entry.m_id),
        }
        match destination_message_mapping.entry(trace_entry.destination_name.clone()) {
            Entry::Vacant(e) => {
                e.insert(vec![trace_entry.m_id]);
            }
            Entry::Occupied(mut e) => e.get_mut().push(trace_entry.m_id),
        }
    }
    (source_message_mapping, destination_message_mapping)
}

/* Currently computes this completely from source perspective:
  for each message sent we consider all destinations that received a message in the timeframe (mindelay - maxdelay)
  we should also compute this from the destinations point of view and then intersect those sets.
*/

pub fn compute_relationship_anonymity(
    trace: &trace::Trace,
    min_delay: i64,
    max_delay: i64,
) -> Result<
    (
        HashMap<String, Vec<(u64, Vec<String>)>>,
        HashMap<String, Vec<(u64, Vec<String>)>>,
    ),
    Box<dyn std::error::Error>,
> {
    let (source_message_mapping, destination_message_mapping) =
        compute_source_and_destination_message_mapping(&trace);
    let (source_mapping, destination_mapping) = compute_source_and_destination_mapping(&trace);
    let (source_message_anonymity_sets, destination_message_anonymity_sets) =
        compute_message_anonymity_sets(&trace, min_delay, max_delay).unwrap();
    let source_relationship_anonymity_sets = compute_relation_ship_anonymity_sets(
        source_message_mapping,
        destination_mapping,
        source_message_anonymity_sets,
    )?;
    let destination_relationship_anonymity_sets = compute_relation_ship_anonymity_sets(
        destination_message_mapping,
        source_mapping,
        destination_message_anonymity_sets,
    )?;
    Ok((
        source_relationship_anonymity_sets,
        destination_relationship_anonymity_sets,
    ))
}

pub fn compute_relation_ship_anonymity_sets(
    message_collection_a: HashMap<String, Vec<u64>>,
    message_to_name_mapping_b: HashMap<u64, String>,
    message_anonymity_sets_a: HashMap<u64, Vec<u64>>,
) -> Result<HashMap<String, Vec<(u64, Vec<String>)>>, Box<dyn std::error::Error>> {
    let mut relationship_anonymity_sets = HashMap::new();
    for (name_a, messages_a) in message_collection_a {
        let mut anonymity_sets: Vec<(u64, Vec<String>)> = vec![];
        let mut selected_messages: Vec<u64> = vec![];
        let mut current_relationship_anonymity_set = vec![];
        if let Some((first_message, remaining_messages)) = messages_a.split_first() {
            /* Get Message Anonymity Set */
            let mas = message_anonymity_sets_a.get(first_message).unwrap();
            for message_b in mas {
                // Determine Destination
                let name_b = message_to_name_mapping_b
                    .get(message_b)
                    .ok_or("Name not found")?;
                // Check if Destination is already in current set
                if current_relationship_anonymity_set.contains(name_b) {
                    println!(
                        "Current relationship anonymity set already contains this name: {}",
                        name_b
                    );
                    println!("{:?}", current_relationship_anonymity_set);
                    continue;
                }
                // Check if this message has already been "used" in a previous round
                if !selected_messages.contains(message_b) {
                    println!("Added unselected message");
                    selected_messages.push(*message_b);
                    current_relationship_anonymity_set.push(name_b.clone());
                }
            }
            anonymity_sets.push((*first_message, current_relationship_anonymity_set));

            for message_a in remaining_messages {
                let mut current_relationship_anonymity_set = vec![];
                let mas = message_anonymity_sets_a.get(message_a).unwrap();
                for message_b in mas {
                    // Determine Destination
                    let name_b = message_to_name_mapping_b
                        .get(message_b)
                        .ok_or("name not found")?;
                    // Check if name is in previous set
                    let (_, previous_relationship_anonymity_set) = anonymity_sets
                        .last()
                        .ok_or("There is no last anonymity set")?;
                    if !previous_relationship_anonymity_set.contains(name_b) {
                        continue;
                    }
                    // Check if name is already in current set
                    if current_relationship_anonymity_set.contains(name_b) {
                        continue;
                    }
                    // Check if this message has already been "used" in a previous round
                    if !selected_messages.contains(message_b) {
                        selected_messages.push(*message_b);
                        current_relationship_anonymity_set.push(name_b.clone());
                    }
                }
                anonymity_sets.push((*message_a, current_relationship_anonymity_set));
            }
            relationship_anonymity_sets.insert(name_a, anonymity_sets);
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
