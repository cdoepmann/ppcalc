use rayon::prelude::*;
use std::cmp::Ordering;
use std::{
    collections::{hash_map::Entry, HashMap},
    fmt::Display,
    ops::Add,
    vec,
};

use fxhash::FxHashSet as HashSet;
use time::{Duration, PrimitiveDateTime};

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

/// Compute the relative difference between two message anonymity sets.
///
/// Returns a pair of `usize`s, with the following meaning:
/// 1. the number of _newly added_ messages from `set1` to `set2`
/// 2. the number of _shared_ messages (overlap) between the two sets
fn relative_set_distance(set1: &HashSet<MessageId>, set2: &HashSet<MessageId>) -> (usize, usize) {
    (
        set2.difference(&set1).count(),
        set2.intersection(&set1).count(),
    )
}

/// Split an anonymity set by the destination of its messages
fn split_by_destination(
    set: HashSet<MessageId>,
    destination_mapping: &HashMap<MessageId, DestinationId>,
) -> HashMap<DestinationId, HashSet<MessageId>> {
    let mut res: HashMap<DestinationId, HashSet<MessageId>> = HashMap::default();

    for message in set {
        let destination = destination_mapping.get(&message).unwrap();
        res.entry(*destination).or_default().insert(message);
    }

    res
}

pub fn compute_message_anonymity_sets(
    trace: &Trace,
    min_delay: Duration,
    max_delay: Duration,
    destination_mapping: &HashMap<MessageId, DestinationId>,
) -> Vec<(
    SourceId,
    Vec<(MessageId, HashMap<DestinationId, (usize, usize)>)>,
)> {
    let mut result = Vec::new();

    // for each source, compute its queue of events (messages entering and leaving the network)
    let event_queues = compute_event_queues(trace, min_delay, max_delay);

    for (source_id, events) in event_queues {
        // process this source's event queue, keeping track of which messages
        // are in the window (and thus, the anonymity set)

        // we collect the messages and their per-destination anonymity set descriptors
        // in the following variable.
        // Each (usize,usize) tuple describes:
        // 1. the number of _new_ messages in this anonymity set compared to the
        //    previous one in this source-destination pair.
        // 2. the number of _overlapping_ messages between this anonymity set and the
        //    previous one in this source-destination pair.
        let mut message_anon_sets: Vec<(MessageId, HashMap<DestinationId, (usize, usize)>)> =
            Vec::new();

        // the anonymity set of each source message (that is still in the window)
        let mut current_message_anon_sets: HashMap<MessageId, HashSet<MessageId>> =
            HashMap::default();

        // also remember the last one, so we can compute the relative difference
        // (the previous one is already split by destination)
        let mut last_message_anon_set: Option<HashMap<DestinationId, HashSet<MessageId>>> = None;

        // // keep track of the source messages in the current window
        // let mut current_source_messages: HashSet<MessageId> = HashSet::default();

        for event in events {
            match event.event_type {
                EventTypeAndId::AddSourceMessage(_) => {
                    // A source message was first observed
                    current_message_anon_sets.insert(event.m_id, HashSet::default());
                }
                EventTypeAndId::RemoveSourceMessage(_) => {
                    // The "window" of a source message has expired. Consequently,
                    // it can be differentiated from messages arriving later than
                    // this at their destination.
                    let this_message_anon_set =
                        current_message_anon_sets.remove(&event.m_id).unwrap();

                    // We now compute this anonymity set's delta from the last anonymity set
                    // (per destination) and aggregate it to the numbers of added and shared messages.

                    // split the anonymity set by destination
                    let this_message_anon_set =
                        split_by_destination(this_message_anon_set, destination_mapping);

                    // compute the relative difference (per destination) of the new anonymity set
                    let relative_difference: HashMap<DestinationId, (usize, usize)> =
                        match last_message_anon_set {
                            None => {
                                // all messages are new
                                this_message_anon_set
                                    .iter()
                                    .map(|(dest, messages)| (dest.clone(), (messages.len(), 0)))
                                    .collect()
                            }
                            Some(previous) => {
                                // compute the difference per destination.
                                // Destinations that aren't present anymore are left out (would be (0,0) anyway).
                                this_message_anon_set
                                    .iter()
                                    .map(|(dest, messages)| {
                                        (
                                            dest.clone(),
                                            match previous.get(&dest) {
                                                None => (messages.len(), 0),
                                                Some(previous_messages) => relative_set_distance(
                                                    previous_messages,
                                                    &messages,
                                                ),
                                            },
                                        )
                                    })
                                    .collect()
                            }
                        };

                    // save the aggregated anonymity set delta as the next result
                    message_anon_sets.push((event.m_id, relative_difference));

                    // remember the original (but split by destination) anonymity set for next iteration
                    last_message_anon_set = Some(this_message_anon_set);
                }
                EventTypeAndId::AddDestinationMessage(_) => {
                    // A message arrived at its destination. It is therefore
                    // part of the anonymity set of each source message that
                    // we haven't removed from the source set yet.
                    for anonymity_set in current_message_anon_sets.values_mut() {
                        anonymity_set.insert(event.m_id);
                    }
                }
            };
        }

        result.push((source_id, message_anon_sets));
    }

    result
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
    min_delay: Duration,
    max_delay: Duration,
) -> Result<
    (
        HashMap<SourceId, Vec<(MessageId, usize)>>,
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
    let source_message_anonymity_sets =
        compute_message_anonymity_sets(&trace, min_delay, max_delay, &destination_mapping);

    bench.measure("source relationship anonymity sets", BENCH_ENABLED);
    let source_relationship_anonymity_sets: HashMap<SourceId, Vec<(MessageId, usize)>> =
        compute_relation_ship_anonymity_sets(source_message_anonymity_sets);
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
    message_anonymity_sets: Vec<(
        SourceId,
        Vec<(MessageId, HashMap<DestinationId, (usize, usize)>)>,
    )>,
) -> HashMap<SourceId, Vec<(MessageId, usize)>> {
    //TODO rayon
    // Bitvektoren für Anonymity sets
    let relationship_anonymity_sets = message_anonymity_sets
        .into_par_iter()
        .map(|(source, messages)| {
            // size of the destination anonymity set after each message
            let mut anon_set_sizes = Vec::new();

            // number of candidate messages per destination after the previous message
            let mut prev_destination_candidates: HashMap<DestinationId, usize> = {
                // For the very first  message of this source), pretent all its destinations
                // were seen before (so we do not exclude them now), but there was no
                // candidate messages left. This way, we will just use the first candidate
                // set as-is.
                if let Some((_first_message, first_destinations)) = messages.first() {
                    first_destinations
                        .keys()
                        .cloned()
                        .map(|dest| (dest, 0))
                        .collect()
                } else {
                    HashMap::default()
                }
            };

            // go through all messages and check their potential destinations
            for (source_message, anonymity_set_sizes) in messages {
                // number of candidate messages per destination for this source message
                let mut destination_candidates: HashMap<DestinationId, usize> = HashMap::default();

                for (destination, (added, overlap)) in anonymity_set_sizes {
                    // calculate the number of candidate messages for this destination
                    let from_previous_message = match prev_destination_candidates.get(&destination)
                    {
                        None => {
                            // this destination wasn't a candidate previously, so we don't add it
                            continue;
                        }
                        Some(previous_candidates) => previous_candidates,
                    };

                    let candidates = added + from_previous_message;

                    // For this destination to remain a candidate, it must have at least one message
                    if candidates == 0 {
                        // Do not keep/make this destination a candidate. This means that our source
                        // was sending more messages than the destination potentially received
                        // from this source.
                        continue;
                    }

                    // This destination is (still) a candidate for our source after this message.
                    // For the next source_message, reduce our candidate message count by one
                    // because we have "used" or "assigned" one of the messages
                    destination_candidates.insert(destination.clone(), candidates - 1);
                }

                // The destination anonymity set after this message is now ready.
                // For now, we only output its size.
                anon_set_sizes.push((source_message, destination_candidates.len()));

                // remember the remaining number of message candidates for each destination
                prev_destination_candidates = destination_candidates;
            }

            (source, anon_set_sizes)
        })
        .collect();

    relationship_anonymity_sets
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

/// Compute the event queues from messages entering and leaving the network, per source.
fn compute_event_queues(
    trace: &Trace,
    min_delay: Duration,
    max_delay: Duration,
) -> HashMap<SourceId, Vec<ProcessingEvent>> {
    // collect all the sources first and create a hash map entry for each
    // we do this separately first in order to have a complete list of sources
    // later when creating the destination message events
    let mut result: HashMap<SourceId, Vec<ProcessingEvent>> = trace
        .entries
        .iter()
        .map(|entry| (entry.source_id, Vec::new()))
        .collect();

    let max_delay = max_delay + time::Duration::nanoseconds(1); // TODO

    for entry in trace.entries.iter() {
        let event_queue = result.entry(entry.source_id).or_default();

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

        // The occurence of a destination messages is relevant for all sources.
        for event_queue in result.values_mut() {
            event_queue.push(ProcessingEvent {
                event_type: EventTypeAndId::AddDestinationMessage(entry.destination_id),
                ts: entry.destination_timestamp,
                m_id: entry.m_id,
            });
        }
    }

    for events in result.values_mut() {
        events.sort();
    }
    result
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
            assert_eq!(r_id, e_id, "Real id, was not the same as expected id");
            assert_eq!(
                r_as,
                e_as.len(),
                "Anonymity sets differ in size at id: {}",
                e_id
            );
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
            assert_eq!(r_id, e_id, "Real id, was not the same as expected id");
            assert_eq!(
                r_as,
                e_as.len(),
                "Anonymity sets differ in size at id: {}",
                e_id
            );
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
