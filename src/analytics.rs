use std::cmp::Ordering;
use std::{
    collections::{hash_map::Entry, HashMap},
    fmt::Display,
    ops::Add,
    vec,
};

use time::PrimitiveDateTime;

use crate::trace;

#[derive(Debug, PartialEq, PartialOrd, Eq, Ord)]
enum EventType {
    AddSourceMessage,
    AddDestinationMessage,
    RemoveSourceMessage,
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

#[derive(Debug, PartialEq, Eq, Ord)]
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

impl PartialOrd for ProcessingEvent {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        if self.ts.eq(&other.ts) {
            return self.event_type.partial_cmp(&other.event_type);
        }
        Some(self.ts.cmp(&other.ts))
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
    event_queue.sort();
    for event in event_queue.iter() {
        println!("{}", event);
    }
    Ok(event_queue)
}

#[cfg(test)]
mod tests {
    use crate::analytics::*;
    #[test]
    fn simple_example_validation() {
        let network_trace =
            crate::trace::read_network_trace_from_file("./test/simple_network_trace.csv").unwrap();
        let (source_anonymity_sets, destination_anonymity_sets) =
            compute_message_anonymity_sets(&network_trace, 1, 100).unwrap();

        let mut expected_source_message_anonymity_set: HashMap<u64, Vec<u64>> = HashMap::new();
        expected_source_message_anonymity_set.insert(0, vec![0, 1, 2, 3, 4, 5, 6, 7]);
        expected_source_message_anonymity_set.insert(1, vec![0, 1, 2, 3, 4, 5, 6, 7]);
        expected_source_message_anonymity_set.insert(2, vec![1, 2, 3, 4, 5, 6, 7, 8]);
        expected_source_message_anonymity_set.insert(3, vec![3, 4, 5, 6, 7, 8, 9, 10]);
        expected_source_message_anonymity_set.insert(4, vec![4, 5, 6, 7, 8, 9, 10]);
        expected_source_message_anonymity_set.insert(5, vec![4, 5, 6, 7, 8, 9, 10]);
        expected_source_message_anonymity_set.insert(6, vec![4, 5, 6, 7, 8, 9, 10, 11]);
        expected_source_message_anonymity_set.insert(7, vec![6, 7, 8, 9, 10, 11, 12]);
        expected_source_message_anonymity_set.insert(8, vec![8, 9, 10, 11, 12, 13]);
        expected_source_message_anonymity_set.insert(9, vec![8, 9, 10, 11, 12, 13, 14]);
        expected_source_message_anonymity_set.insert(10, vec![8, 9, 10, 11, 12, 13, 14, 15]);
        expected_source_message_anonymity_set.insert(11, vec![11, 12, 13, 14, 15]);
        expected_source_message_anonymity_set.insert(12, vec![11, 12, 13, 14, 15]);
        expected_source_message_anonymity_set.insert(13, vec![13, 14, 15]);
        expected_source_message_anonymity_set.insert(14, vec![14, 15]);
        expected_source_message_anonymity_set.insert(15, vec![14, 15]);

        for id in 0..16 {
            assert_eq!(
                *source_anonymity_sets.get(&id).unwrap(),
                *expected_source_message_anonymity_set.get(&id).unwrap(),
                "Source message anonymity set differed from expectation at id: {}",
                id
            );
        }

        let mut expected_destination_message_anonymity_set: HashMap<u64, Vec<u64>> = HashMap::new();
        expected_destination_message_anonymity_set.insert(0, vec![0, 1]);
        expected_destination_message_anonymity_set.insert(1, vec![0, 1, 2]);
        expected_destination_message_anonymity_set.insert(2, vec![0, 1, 2]);
        expected_destination_message_anonymity_set.insert(3, vec![0, 1, 2, 3]);
        expected_destination_message_anonymity_set.insert(4, vec![0, 1, 2, 3, 4, 5, 6]);
        expected_destination_message_anonymity_set.insert(5, vec![0, 1, 2, 3, 4, 5, 6]);
        expected_destination_message_anonymity_set.insert(6, vec![0, 1, 2, 3, 4, 5, 6, 7]);
        expected_destination_message_anonymity_set.insert(7, vec![0, 1, 2, 3, 4, 5, 6, 7]);
        expected_destination_message_anonymity_set.insert(8, vec![2, 3, 4, 5, 6, 7, 8, 9, 10]);
        expected_destination_message_anonymity_set.insert(9, vec![3, 4, 5, 6, 7, 8, 9, 10]);
        expected_destination_message_anonymity_set.insert(10, vec![3, 4, 5, 6, 7, 8, 9, 10]);
        expected_destination_message_anonymity_set.insert(11, vec![6, 7, 8, 9, 10, 11, 12]);
        expected_destination_message_anonymity_set.insert(12, vec![7, 8, 9, 10, 11, 12]);
        expected_destination_message_anonymity_set.insert(13, vec![8, 9, 10, 11, 12, 13]);
        expected_destination_message_anonymity_set.insert(14, vec![9, 10, 11, 12, 13, 14, 15]);
        expected_destination_message_anonymity_set.insert(15, vec![10, 11, 12, 13, 14, 15]);

        for id in 0..16 {
            assert_eq!(
                *destination_anonymity_sets.get(&id).unwrap(),
                *expected_destination_message_anonymity_set.get(&id).unwrap(),
                "Destination message anonymity set differed from expectation at id: {}",
                id
            );
        }

        let (source_relationship_anonymity_sets, destination_relationship_anonymity_sets) =
            compute_relationship_anonymity(&network_trace, 1, 100).unwrap();

        let mut source_relationship_anonymity_sets_s1 = vec![];
        let d1_s = String::from("d1");
        let d2_s = String::from("d2");
        source_relationship_anonymity_sets_s1.push((0, vec![d1_s.clone(), d2_s.clone()]));
        source_relationship_anonymity_sets_s1.push((2, vec![d1_s.clone(), d2_s.clone()]));
        source_relationship_anonymity_sets_s1.push((3, vec![d1_s.clone(), d2_s.clone()]));
        source_relationship_anonymity_sets_s1.push((5, vec![d1_s.clone(), d2_s.clone()]));
        source_relationship_anonymity_sets_s1.push((6, vec![d1_s.clone(), d2_s.clone()]));
        source_relationship_anonymity_sets_s1.push((9, vec![d1_s.clone(), d2_s.clone()]));
        source_relationship_anonymity_sets_s1.push((10, vec![d1_s.clone(), d2_s.clone()]));
        source_relationship_anonymity_sets_s1.push((12, vec![d1_s.clone()]));
        source_relationship_anonymity_sets_s1.push((14, vec![d1_s.clone()]));

        let sras_s1 = source_relationship_anonymity_sets
            .get("s1")
            .unwrap()
            .clone();
        assert_eq!(
            sras_s1.len(),
            source_relationship_anonymity_sets_s1.len(),
            "The length of the anonymity sets for sender s1 differ"
        );
        let mut r_iter = sras_s1.into_iter();
        let mut e_iter = source_relationship_anonymity_sets_s1.into_iter();
        loop {
            let (r_id, mut r_as) = match r_iter.next() {
                Some(item) => item,
                None => break,
            };
            let (e_id, mut e_as) = match e_iter.next() {
                Some(item) => item,
                None => {
                    panic!("Real has entries left, expected doesn't. This should fail earlier.");
                }
            };
            r_as.sort();
            e_as.sort();
            assert_eq!(r_id, e_id, "Real id, was not the same as expected id");
            assert_eq!(r_as, e_as, "Anonymity sets differ at id: {}", e_id);
        }

        let mut source_relationship_anonymity_sets_s2 = vec![];
        let d1_s = String::from("d1");
        let d2_s = String::from("d2");
        source_relationship_anonymity_sets_s2.push((1, vec![d1_s.clone(), d2_s.clone()]));
        source_relationship_anonymity_sets_s2.push((4, vec![d1_s.clone(), d2_s.clone()]));
        source_relationship_anonymity_sets_s2.push((7, vec![d1_s.clone(), d2_s.clone()]));
        source_relationship_anonymity_sets_s2.push((8, vec![d1_s.clone(), d2_s.clone()]));
        source_relationship_anonymity_sets_s2.push((11, vec![d1_s.clone(), d2_s.clone()]));
        source_relationship_anonymity_sets_s2.push((13, vec![d1_s.clone(), d2_s.clone()]));
        source_relationship_anonymity_sets_s2.push((15, vec![d2_s.clone()]));

        let sras_s2 = source_relationship_anonymity_sets
            .get("s2")
            .unwrap()
            .clone();
        assert_eq!(
            sras_s2.len(),
            source_relationship_anonymity_sets_s2.len(),
            "The length of the anonymity sets for sender s1 differ"
        );
        let mut r_iter = sras_s2.into_iter();
        let mut e_iter = source_relationship_anonymity_sets_s2.into_iter();
        loop {
            let (r_id, mut r_as) = match r_iter.next() {
                Some(item) => item,
                None => break,
            };
            let (e_id, mut e_as) = match e_iter.next() {
                Some(item) => item,
                None => {
                    panic!("Real has entries left, expected doesn't. This should fail earlier.");
                }
            };
            r_as.sort();
            e_as.sort();
            assert_eq!(r_id, e_id, "Real id, was not the same as expected id");
            assert_eq!(r_as, e_as, "Anonymity sets differ at id: {}", e_id);
        }

        let mut destination_relationship_anonymity_sets_d1 = vec![];
        let s1_s = String::from("s1");
        let s2_s = String::from("s2");
        destination_relationship_anonymity_sets_d1.push((0, vec![s1_s.clone(), s2_s.clone()]));
        destination_relationship_anonymity_sets_d1.push((2, vec![s1_s.clone()]));
        destination_relationship_anonymity_sets_d1.push((3, vec![s1_s.clone()]));
        destination_relationship_anonymity_sets_d1.push((5, vec![s1_s.clone()]));
        destination_relationship_anonymity_sets_d1.push((6, vec![s1_s.clone()]));
        destination_relationship_anonymity_sets_d1.push((9, vec![s1_s.clone()]));
        destination_relationship_anonymity_sets_d1.push((10, vec![s1_s.clone()]));
        destination_relationship_anonymity_sets_d1.push((12, vec![s1_s.clone()]));
        destination_relationship_anonymity_sets_d1.push((14, vec![s1_s.clone()]));

        let dras_d1 = destination_relationship_anonymity_sets
            .get("d1")
            .unwrap()
            .clone();
        assert_eq!(
            dras_d1.len(),
            destination_relationship_anonymity_sets_d1.len(),
            "The length of the anonymity sets for sender s1 differ"
        );
        let mut r_iter = dras_d1.into_iter();
        let mut e_iter = destination_relationship_anonymity_sets_d1.into_iter();
        loop {
            let (r_id, mut r_as) = match r_iter.next() {
                Some(item) => item,
                None => break,
            };
            let (e_id, mut e_as) = match e_iter.next() {
                Some(item) => item,
                None => {
                    panic!("Real has entries left, expected doesn't. This should fail earlier.");
                }
            };
            r_as.sort();
            e_as.sort();
            assert_eq!(r_id, e_id, "Real id, was not the same as expected id");
            assert_eq!(r_as, e_as, "Anonymity sets differ at id: {}", e_id);
        }

        let mut destination_relationship_anonymity_sets_d2 = vec![];
        let s1_s = String::from("s1");
        let s2_s = String::from("s2");
        destination_relationship_anonymity_sets_d2.push((1, vec![s1_s.clone(), s2_s.clone()]));
        destination_relationship_anonymity_sets_d2.push((4, vec![s1_s.clone(), s2_s.clone()]));
        destination_relationship_anonymity_sets_d2.push((7, vec![s1_s.clone(), s2_s.clone()]));
        destination_relationship_anonymity_sets_d2.push((8, vec![s1_s.clone(), s2_s.clone()]));
        destination_relationship_anonymity_sets_d2.push((11, vec![s1_s.clone(), s2_s.clone()]));
        destination_relationship_anonymity_sets_d2.push((13, vec![s1_s.clone(), s2_s.clone()]));
        destination_relationship_anonymity_sets_d2.push((15, vec![s1_s.clone(), s2_s.clone()]));

        let dras_d2 = destination_relationship_anonymity_sets
            .get("d2")
            .unwrap()
            .clone();
        assert_eq!(
            dras_d2.len(),
            destination_relationship_anonymity_sets_d2.len(),
            "The length of the anonymity sets for sender s1 differ"
        );
        let mut r_iter = dras_d2.into_iter();
        let mut e_iter = destination_relationship_anonymity_sets_d2.into_iter();
        loop {
            let (r_id, mut r_as) = match r_iter.next() {
                Some(item) => item,
                None => break,
            };
            let (e_id, mut e_as) = match e_iter.next() {
                Some(item) => item,
                None => {
                    panic!("Real has entries left, expected doesn't. This should fail earlier.");
                }
            };
            r_as.sort();
            e_as.sort();
            assert_eq!(r_id, e_id, "Real id, was not the same as expected id");
            assert_eq!(r_as, e_as, "Anonymity sets differ at id: {}", e_id);
        }
    }
}
