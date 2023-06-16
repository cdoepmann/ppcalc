use rayon::prelude::*;
use std::cmp::Ordering;
use std::{
    collections::{hash_map::Entry, HashMap, HashSet},
    fmt::Display,
    ops::Add,
    vec,
};

use time::PrimitiveDateTime;

use crate::bench;
use crate::trace::{DestinationId, MessageId, SourceId, Trace};

#[derive(Debug, PartialEq, PartialOrd, Eq, Ord)]
enum EventTypeAndId {
    AddSourceMessage(SourceId),
    AddDestinationMessage(DestinationId),
    RemoveSourceMessage(SourceId),
}

impl EventTypeAndId {
    fn id_to_string(&self) -> String {
        match self {
            EventTypeAndId::AddDestinationMessage(dest_id) => dest_id.to_string(),
            EventTypeAndId::AddSourceMessage(source_id) => source_id.to_string(),
            EventTypeAndId::RemoveSourceMessage(source_id) => source_id.to_string(),
        }
    }
}

impl Display for EventTypeAndId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EventTypeAndId::AddDestinationMessage(_) => f.write_str("AddDestinationMessage"),
            EventTypeAndId::AddSourceMessage(_) => f.write_str("AddSourceMessage"),
            EventTypeAndId::RemoveSourceMessage(_) => f.write_str("RemoveSourceMessage"),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Ord)]
struct ProcessingEvent {
    event_type: EventTypeAndId,
    ts: PrimitiveDateTime,
    m_id: MessageId,
}
impl Display for ProcessingEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} {} {} {}",
            self.event_type,
            self.ts,
            self.m_id,
            self.event_type.id_to_string()
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
    trace: &Trace,
    min_delay: i64,
    max_delay: i64,
) -> Result<
    (
        HashMap<MessageId, Vec<MessageId>>,
        HashMap<MessageId, Vec<MessageId>>,
    ),
    Box<dyn std::error::Error>,
> {
    let event_queue = compute_event_queue(trace, min_delay, max_delay)?;
    let mut current_source_message_set: Vec<MessageId> = vec![];
    let mut message_receiver_anonymity_sets: HashMap<MessageId, Vec<MessageId>> = HashMap::new();
    let mut message_sender_anonymity_sets: HashMap<MessageId, Vec<MessageId>> = HashMap::new();

    for event in event_queue {
        match event.event_type {
            EventTypeAndId::AddSourceMessage(_) => current_source_message_set.push(event.m_id),
            EventTypeAndId::RemoveSourceMessage(_) => {
                current_source_message_set.retain(|x| *x != event.m_id)
            }
            EventTypeAndId::AddDestinationMessage(_) => {
                for m_id in current_source_message_set.iter() {
                    match message_receiver_anonymity_sets.get_mut(&m_id) {
                        Some(set) => set.push(event.m_id),
                        None => {
                            message_receiver_anonymity_sets.insert(*m_id, vec![event.m_id]);
                        }
                    };
                }
                message_sender_anonymity_sets
                    .insert(event.m_id, current_source_message_set.clone());
            }
        };
    }
    Ok((
        message_receiver_anonymity_sets,
        message_sender_anonymity_sets,
    ))
}

fn compute_source_and_destination_mapping(
    trace: &Trace,
) -> (
    HashMap<MessageId, SourceId>,
    HashMap<MessageId, DestinationId>,
) {
    let mut source_mapping = HashMap::new();
    let mut destination_mapping = HashMap::new();
    for entry in trace.entries.iter() {
        source_mapping.insert(entry.m_id, entry.source_id.clone());
        destination_mapping.insert(entry.m_id, entry.destination_id.clone());
    }
    (source_mapping, destination_mapping)
}

fn compute_source_and_destination_message_mapping(
    trace: &Trace,
) -> (
    HashMap<SourceId, Vec<MessageId>>,
    HashMap<DestinationId, Vec<MessageId>>,
) {
    let mut source_message_mapping = HashMap::new();
    let mut destination_message_mapping = HashMap::new();
    for trace_entry in trace.entries.iter() {
        match source_message_mapping.entry(trace_entry.source_id.clone()) {
            Entry::Vacant(e) => {
                e.insert(vec![trace_entry.m_id]);
            }
            Entry::Occupied(mut e) => e.get_mut().push(trace_entry.m_id),
        }
        match destination_message_mapping.entry(trace_entry.destination_id.clone()) {
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
    trace: &Trace,
    min_delay: i64,
    max_delay: i64,
) -> Result<
    (
        HashMap<SourceId, Vec<(MessageId, HashSet<DestinationId>)>>,
        HashMap<SourceId, Vec<(MessageId, Vec<DestinationId>)>>,
    ),
    Box<dyn std::error::Error>,
> {
    let mut bench = bench::Bench::new();
    let BENCH_ENABLED = true;

    bench.measure("preliminaries for metric calculation", BENCH_ENABLED);
    let (source_message_mapping, destination_message_mapping) =
        compute_source_and_destination_message_mapping(&trace);
    let (source_mapping, destination_mapping) = compute_source_and_destination_mapping(&trace);

    bench.measure("source anonymity sets", BENCH_ENABLED);
    let (source_message_anonymity_sets, destination_message_anonymity_sets) =
        compute_message_anonymity_sets(&trace, min_delay, max_delay).unwrap();

    bench.measure("source relationship anonymity sets", BENCH_ENABLED);
    let source_relationship_anonymity_sets: HashMap<
        SourceId,
        Vec<(MessageId, HashSet<DestinationId>)>,
    > = compute_relation_ship_anonymity_sets(
        source_message_mapping,
        destination_mapping,
        source_message_anonymity_sets,
    )?;
    /* Be wary that this yields only useful results if there is just one source per destination */
    let destination_relationship_anonymity_sets = HashMap::new();
    /*compute_relation_ship_anonymity_sets(
        destination_message_mapping,
        source_mapping,
        destination_message_anonymity_sets,
    )?;*/
    Ok((
        source_relationship_anonymity_sets,
        destination_relationship_anonymity_sets,
    ))
}

pub fn compute_relation_ship_anonymity_sets(
    source_message_mapping: HashMap<SourceId, Vec<MessageId>>,
    destination_mapping: HashMap<MessageId, DestinationId>,
    message_anonymity_sets: HashMap<MessageId, Vec<MessageId>>,
) -> Result<HashMap<SourceId, Vec<(MessageId, HashSet<DestinationId>)>>, Box<dyn std::error::Error>>
{
    //TODO rayon
    // Bitvektoren für Anonymity sets
    let relationship_anonymity_sets = source_message_mapping
        .par_iter()
        .map(|(name_a, messages_a)| {
            let mut anonymity_sets: Vec<(MessageId, HashSet<DestinationId>)> = Vec::new();
            let mut selected_messages: HashSet<MessageId> = HashSet::new();

            for source_msg in messages_a {
                let mut current_relationship_anonymity_set = HashSet::new();
                let previous_dest_anonymity_set = anonymity_sets.last().map(|(_, x)| x);

                let msg_anon_set = message_anonymity_sets.get(source_msg).unwrap();

                // "candidate" messages in this source_msg's anonymity set.
                // We here calculate the anonymity set of *destinations* for source_msg
                for candidate in msg_anon_set {
                    let dest = destination_mapping.get(candidate).expect("name not found");

                    // check if name is in previous set (we do not want to grow the set)
                    if let Some(previous_set) = previous_dest_anonymity_set {
                        if !previous_set.contains(dest) {
                            continue;
                        }
                    }

                    // check if destination is already in current anonymity set
                    if current_relationship_anonymity_set.contains(dest) {
                        continue;
                    }

                    // check if this message has already been "used" in a previous round
                    if selected_messages.contains(candidate) {
                        continue;
                    }

                    // remember the destination and that we used the message
                    selected_messages.insert(*candidate);
                    current_relationship_anonymity_set.insert(dest.clone());
                }

                // remember this message's anonymity set of destinations
                anonymity_sets.push((*source_msg, current_relationship_anonymity_set));
            }

            (*name_a, anonymity_sets)
        })
        .collect();
    Ok(relationship_anonymity_sets)
}

/*
fn compute_relationship_anonymity_intersection(
    source_relationship_anonymity_sets: HashMap<String, Vec<(u64, Vec<String>)>>,
    destination_relationship_anonymity_sets: HashMap<String, Vec<(u64, Vec<String>)>>,
) -> HashMap<String, Vec<(u64, Vec<String>)>> {
    let mut relationship_anonymity_sets: HashMap<u64, Vec<String>> = HashMap::new();
    let mut source_message_map: HashMap<String, Vec<u64>> = HashMap::new();
    let mut source_message_anonymity_sets: HashMap<u64, Vec<String>> = HashMap::new();
    let mut destination_message_map: HashMap<String, Vec<u64>> = HashMap::new();
    let mut destination_message_anonymity_sets: HashMap<u64, Vec<String>> = HashMap::new();

    for (source, mas) in source_relationship_anonymity_sets.into_iter() {
        let mut message_list: Vec<u64> = vec![];
        for (id, destinations) in mas.into_iter() {
            message_list.push(id);
            source_message_anonymity_sets.insert(id, destinations);
        }
        source_message_map.insert(source.to_string(), message_list);
    }

    for (destination, mas) in destination_relationship_anonymity_sets.into_iter() {
        let mut message_list: Vec<u64> = vec![];
        for (id, sources) in mas.into_iter() {
            message_list.push(id);
            destination_message_anonymity_sets.insert(id, sources);
        }
        destination_message_map.insert(destination.to_string(), message_list);
    }

    for (id, sources) in destination_message_anonymity_sets.iter() {
        if let Some(destinations) = source_message_anonymity_sets.get(id) {
            for source in sources
        }
    }
    relationship_anonymity_sets
}
*/
fn compute_event_queue(
    trace: &Trace,
    min_delay: i64,
    max_delay: i64,
) -> Result<Vec<ProcessingEvent>, Box<dyn std::error::Error>> {
    let min_delay = time::Duration::milliseconds(min_delay);
    let max_delay = time::Duration::milliseconds(max_delay) + time::Duration::nanoseconds(1);
    let mut event_queue: Vec<ProcessingEvent> = vec![];
    for entry in trace.entries.iter() {
        event_queue.push(ProcessingEvent {
            event_type: EventTypeAndId::AddSourceMessage(entry.source_id),
            ts: entry.source_timestamp.add(min_delay),
            m_id: entry.m_id,
        });
        event_queue.push(ProcessingEvent {
            event_type: EventTypeAndId::RemoveSourceMessage(entry.source_id),
            ts: entry.source_timestamp.add(max_delay),
            m_id: entry.m_id,
        });
        event_queue.push(ProcessingEvent {
            event_type: EventTypeAndId::AddDestinationMessage(entry.destination_id),
            ts: entry.destination_timestamp,
            m_id: entry.m_id,
        });
    }
    event_queue.sort();
    Ok(event_queue)
}

#[cfg(test)]
mod tests {
    use crate::metric::*;
    #[test]
    fn simple_example_validation() {
        let network_trace = Trace::from_csv("./test/simple_network_trace.csv").unwrap();
        let (source_anonymity_sets, destination_anonymity_sets) =
            compute_message_anonymity_sets(&network_trace, 1, 100).unwrap();

        let mut expected_source_message_anonymity_set: HashMap<MessageId, Vec<MessageId>> =
            HashMap::new();

        for (message, anon_set_messages) in [
            (0, vec![0, 1, 2, 3, 4, 5, 6, 7]),
            (1, vec![0, 1, 2, 3, 4, 5, 6, 7]),
            (2, vec![1, 2, 3, 4, 5, 6, 7, 8]),
            (3, vec![3, 4, 5, 6, 7, 8, 9, 10]),
            (4, vec![4, 5, 6, 7, 8, 9, 10]),
            (5, vec![4, 5, 6, 7, 8, 9, 10]),
            (6, vec![4, 5, 6, 7, 8, 9, 10, 11]),
            (7, vec![6, 7, 8, 9, 10, 11, 12]),
            (8, vec![8, 9, 10, 11, 12, 13]),
            (9, vec![8, 9, 10, 11, 12, 13, 14]),
            (10, vec![8, 9, 10, 11, 12, 13, 14, 15]),
            (11, vec![11, 12, 13, 14, 15]),
            (12, vec![11, 12, 13, 14, 15]),
            (13, vec![13, 14, 15]),
            (14, vec![14, 15]),
            (15, vec![14, 15]),
        ] {
            expected_source_message_anonymity_set.insert(
                MessageId::new(message),
                anon_set_messages
                    .into_iter()
                    .map(|x| MessageId::new(x))
                    .collect(),
            );
        }

        for id in 0..16 {
            assert_eq!(
                *source_anonymity_sets.get(&MessageId::new(id)).unwrap(),
                *expected_source_message_anonymity_set
                    .get(&MessageId::new(id))
                    .unwrap(),
                "Source message anonymity set differed from expectation at id: {}",
                id
            );
        }

        let mut expected_destination_message_anonymity_set: HashMap<MessageId, Vec<MessageId>> =
            HashMap::new();

        for (message, anon_set_messages) in [
            (0, vec![0, 1]),
            (1, vec![0, 1, 2]),
            (2, vec![0, 1, 2]),
            (3, vec![0, 1, 2, 3]),
            (4, vec![0, 1, 2, 3, 4, 5, 6]),
            (5, vec![0, 1, 2, 3, 4, 5, 6]),
            (6, vec![0, 1, 2, 3, 4, 5, 6, 7]),
            (7, vec![0, 1, 2, 3, 4, 5, 6, 7]),
            (8, vec![2, 3, 4, 5, 6, 7, 8, 9, 10]),
            (9, vec![3, 4, 5, 6, 7, 8, 9, 10]),
            (10, vec![3, 4, 5, 6, 7, 8, 9, 10]),
            (11, vec![6, 7, 8, 9, 10, 11, 12]),
            (12, vec![7, 8, 9, 10, 11, 12]),
            (13, vec![8, 9, 10, 11, 12, 13]),
            (14, vec![9, 10, 11, 12, 13, 14, 15]),
            (15, vec![10, 11, 12, 13, 14, 15]),
        ] {
            expected_destination_message_anonymity_set.insert(
                MessageId::new(message),
                anon_set_messages
                    .into_iter()
                    .map(|x| MessageId::new(x))
                    .collect(),
            );
        }

        for id in 0..16 {
            assert_eq!(
                *destination_anonymity_sets.get(&MessageId::new(id)).unwrap(),
                *expected_destination_message_anonymity_set
                    .get(&MessageId::new(id))
                    .unwrap(),
                "Destination message anonymity set differed from expectation at id: {}",
                id
            );
        }

        let (source_relationship_anonymity_sets, destination_relationship_anonymity_sets) =
            compute_relationship_anonymity(&network_trace, 1, 100).unwrap();

        let mut source_relationship_anonymity_sets_s1 = vec![];
        let d1_s = DestinationId::new(1);
        let d2_s = DestinationId::new(2);
        source_relationship_anonymity_sets_s1
            .push((MessageId::new(0), vec![d1_s.clone(), d2_s.clone()]));
        source_relationship_anonymity_sets_s1
            .push((MessageId::new(2), vec![d1_s.clone(), d2_s.clone()]));
        source_relationship_anonymity_sets_s1
            .push((MessageId::new(3), vec![d1_s.clone(), d2_s.clone()]));
        source_relationship_anonymity_sets_s1
            .push((MessageId::new(5), vec![d1_s.clone(), d2_s.clone()]));
        source_relationship_anonymity_sets_s1
            .push((MessageId::new(6), vec![d1_s.clone(), d2_s.clone()]));
        source_relationship_anonymity_sets_s1
            .push((MessageId::new(9), vec![d1_s.clone(), d2_s.clone()]));
        source_relationship_anonymity_sets_s1
            .push((MessageId::new(10), vec![d1_s.clone(), d2_s.clone()]));
        source_relationship_anonymity_sets_s1.push((MessageId::new(12), vec![d1_s.clone()]));
        source_relationship_anonymity_sets_s1.push((MessageId::new(14), vec![d1_s.clone()]));

        let sras_s1 = source_relationship_anonymity_sets
            .get(&SourceId::new(1))
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
            let (r_id, r_as) = match r_iter.next() {
                Some(item) => item,
                None => break,
            };
            let (e_id, mut e_as) = match e_iter.next() {
                Some(item) => item,
                None => {
                    panic!("Real has entries left, expected doesn't. This should fail earlier.");
                }
            };
            let mut r_as: Vec<_> = r_as.into_iter().collect();
            r_as.sort();
            e_as.sort();
            assert_eq!(r_id, e_id, "Real id, was not the same as expected id");
            assert_eq!(r_as, e_as, "Anonymity sets differ at id: {}", e_id);
        }

        let mut source_relationship_anonymity_sets_s2 = vec![];
        let d1_s = DestinationId::new(1);
        let d2_s = DestinationId::new(2);
        source_relationship_anonymity_sets_s2
            .push((MessageId::new(1), vec![d1_s.clone(), d2_s.clone()]));
        source_relationship_anonymity_sets_s2
            .push((MessageId::new(4), vec![d1_s.clone(), d2_s.clone()]));
        source_relationship_anonymity_sets_s2
            .push((MessageId::new(7), vec![d1_s.clone(), d2_s.clone()]));
        source_relationship_anonymity_sets_s2
            .push((MessageId::new(8), vec![d1_s.clone(), d2_s.clone()]));
        source_relationship_anonymity_sets_s2
            .push((MessageId::new(11), vec![d1_s.clone(), d2_s.clone()]));
        source_relationship_anonymity_sets_s2
            .push((MessageId::new(13), vec![d1_s.clone(), d2_s.clone()]));
        source_relationship_anonymity_sets_s2.push((MessageId::new(15), vec![d2_s.clone()]));

        let sras_s2 = source_relationship_anonymity_sets
            .get(&SourceId::new(2))
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
            let (r_id, r_as) = match r_iter.next() {
                Some(item) => item,
                None => break,
            };
            let (e_id, mut e_as) = match e_iter.next() {
                Some(item) => item,
                None => {
                    panic!("Real has entries left, expected doesn't. This should fail earlier.");
                }
            };
            let mut r_as: Vec<_> = r_as.into_iter().collect();
            r_as.sort();
            e_as.sort();
            assert_eq!(r_id, e_id, "Real id, was not the same as expected id");
            assert_eq!(r_as, e_as, "Anonymity sets differ at id: {}", e_id);
        }

        //     let mut destination_relationship_anonymity_sets_d1 = vec![];
        //     let s1_s = 1;
        //     let s2_s = 2;
        //     destination_relationship_anonymity_sets_d1.push((0, vec![s1_s.clone(), s2_s.clone()]));
        //     destination_relationship_anonymity_sets_d1.push((2, vec![s1_s.clone()]));
        //     destination_relationship_anonymity_sets_d1.push((3, vec![s1_s.clone()]));
        //     destination_relationship_anonymity_sets_d1.push((5, vec![s1_s.clone()]));
        //     destination_relationship_anonymity_sets_d1.push((6, vec![s1_s.clone()]));
        //     destination_relationship_anonymity_sets_d1.push((9, vec![s1_s.clone()]));
        //     destination_relationship_anonymity_sets_d1.push((10, vec![s1_s.clone()]));
        //     destination_relationship_anonymity_sets_d1.push((12, vec![s1_s.clone()]));
        //     destination_relationship_anonymity_sets_d1.push((14, vec![s1_s.clone()]));

        //     let dras_d1 = destination_relationship_anonymity_sets
        //         .get(&1)
        //         .unwrap()
        //         .clone();
        //     assert_eq!(
        //         dras_d1.len(),
        //         destination_relationship_anonymity_sets_d1.len(),
        //         "The length of the anonymity sets for sender s1 differ"
        //     );
        //     let mut r_iter = dras_d1.into_iter();
        //     let mut e_iter = destination_relationship_anonymity_sets_d1.into_iter();
        //     loop {
        //         let (r_id, mut r_as) = match r_iter.next() {
        //             Some(item) => item,
        //             None => break,
        //         };
        //         let (e_id, mut e_as) = match e_iter.next() {
        //             Some(item) => item,
        //             None => {
        //                 panic!("Real has entries left, expected doesn't. This should fail earlier.");
        //             }
        //         };
        //         r_as.sort();
        //         e_as.sort();
        //         assert_eq!(r_id, e_id, "Real id, was not the same as expected id");
        //         assert_eq!(r_as, e_as, "Anonymity sets differ at id: {}", e_id);
        //     }

        //     let mut destination_relationship_anonymity_sets_d2 = vec![];
        //     let s1_s = 1;
        //     let s2_s = 2;
        //     destination_relationship_anonymity_sets_d2.push((1, vec![s1_s.clone(), s2_s.clone()]));
        //     destination_relationship_anonymity_sets_d2.push((4, vec![s1_s.clone(), s2_s.clone()]));
        //     destination_relationship_anonymity_sets_d2.push((7, vec![s1_s.clone(), s2_s.clone()]));
        //     destination_relationship_anonymity_sets_d2.push((8, vec![s1_s.clone(), s2_s.clone()]));
        //     destination_relationship_anonymity_sets_d2.push((11, vec![s1_s.clone(), s2_s.clone()]));
        //     destination_relationship_anonymity_sets_d2.push((13, vec![s1_s.clone(), s2_s.clone()]));
        //     destination_relationship_anonymity_sets_d2.push((15, vec![s1_s.clone(), s2_s.clone()]));

        //     let dras_d2 = destination_relationship_anonymity_sets
        //         .get(&2)
        //         .unwrap()
        //         .clone();
        //     assert_eq!(
        //         dras_d2.len(),
        //         destination_relationship_anonymity_sets_d2.len(),
        //         "The length of the anonymity sets for sender s1 differ"
        //     );
        //     let mut r_iter = dras_d2.into_iter();
        //     let mut e_iter = destination_relationship_anonymity_sets_d2.into_iter();
        //     loop {
        //         let (r_id, mut r_as) = match r_iter.next() {
        //             Some(item) => item,
        //             None => break,
        //         };
        //         let (e_id, mut e_as) = match e_iter.next() {
        //             Some(item) => item,
        //             None => {
        //                 panic!("Real has entries left, expected doesn't. This should fail earlier.");
        //             }
        //         };
        //         r_as.sort();
        //         e_as.sort();
        //         assert_eq!(r_id, e_id, "Real id, was not the same as expected id");
        //         assert_eq!(r_as, e_as, "Anonymity sets differ at id: {}", e_id);
        //     }
    }
}
