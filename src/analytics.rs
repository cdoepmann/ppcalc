use std::{
    collections::{hash_map::Entry, HashMap},
    fmt::Display,
    hash::Hash,
    ops::Add,
    vec,
};

use time::PrimitiveDateTime;

use crate::{network, trace};

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
    trace: trace::Trace,
    min_delay: i64,
    max_delay: i64,
) -> Result<HashMap<u64, Vec<u64>>, Box<dyn std::error::Error>> {
    let event_queue = compute_event_queue(trace, min_delay, max_delay)?;
    let mut current_message_set: Vec<u64> = vec![];
    let mut current_destination_set: Vec<String> = vec![];
    let mut anonymity_sets: HashMap<u64, Vec<u64>> = HashMap::new();
    let previous_event = EventType::AddSourceMessage;

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

fn compute_destination_mapping(trace: trace::Trace) -> HashMap<u64, String> {
    let mut destination_mapping = HashMap::new();
    for entry in trace.entries {
        destination_mapping.insert(entry.m_id, entry.destination_name);
    }
    destination_mapping
}

fn compute_source_message_mapping(trace: trace::Trace) -> HashMap<String, Vec<u64>> {
    let mut source_message_mapping = HashMap::new();
    for trace_entry in trace.entries {
        match source_message_mapping.entry(trace_entry.source_name) {
            Entry::Vacant(e) => {
                e.insert(vec![trace_entry.m_id]);
            }
            Entry::Occupied(mut e) => e.get_mut().push(trace_entry.m_id),
        }
    }
    source_message_mapping
}

fn compute_event_queue(
    trace: trace::Trace,
    min_delay: i64,
    max_delay: i64,
) -> Result<Vec<ProcessingEvent>, Box<dyn std::error::Error>> {
    let min_delay = time::Duration::milliseconds(min_delay);
    let max_delay = time::Duration::milliseconds(max_delay) + time::Duration::nanoseconds(1);
    let mut event_queue: Vec<ProcessingEvent> = vec![];
    for entry in trace.entries {
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
            name: entry.source_name,
        });
        event_queue.push(ProcessingEvent {
            event_type: EventType::AddDestinationMessage,
            ts: entry.destination_timestamp,
            m_id: entry.m_id,
            name: entry.destination_name,
        });
    }
    event_queue.sort_by(|a, b| a.ts.cmp(&b.ts));
    for event in event_queue.iter() {
        println!("{}", event);
    }
    Ok(event_queue)
}
