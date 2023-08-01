use std::cmp::Ordering;
use std::path::Path;

use fxhash::FxHashSet as HashSet;
use serde::{Deserialize, Serialize};
use time::PrimitiveDateTime;

/// A single entry within a provided [Trace].
///
/// It contains the real information when a message was sent and received,
/// and by whom.
#[derive(Serialize, Deserialize)]
pub struct TraceEntry {
    pub m_id: MessageId,
    pub source_id: SourceId,
    pub source_timestamp: PrimitiveDateTime,
    pub destination_id: DestinationId,
    pub destination_timestamp: PrimitiveDateTime,
}

/// A builder for a network trace.
pub struct TraceBuilder {
    entries: Vec<TraceEntry>,
}

impl TraceBuilder {
    /// Construct a new trace builder
    pub fn new() -> TraceBuilder {
        TraceBuilder {
            entries: Vec::new(),
        }
    }

    /// Add a new entry to the trace builder
    pub fn add_entry(&mut self, entry: TraceEntry) {
        self.entries.push(entry);
    }

    /// Load a full trace from a CSV file, given its file path
    pub fn from_csv(
        path: impl AsRef<Path>,
    ) -> Result<TraceBuilder, Box<dyn std::error::Error + Send + Sync>> {
        let path = path.as_ref();

        let mut rdr = csv::ReaderBuilder::new().from_path(path)?;
        let mut iter = rdr.deserialize();

        let mut trace = TraceBuilder::new();
        while let Some(result) = iter.next() {
            let entry: TraceEntry = result?;
            trace.add_entry(entry);
        }
        Ok(trace)
    }

    /// Fix the contained entries so they fulfil the trace requirements.
    /// This primarily renames the message IDs.
    pub fn fix(&mut self) {
        self.entries
            .sort_unstable_by_key(|e| e.destination_timestamp);
        for (i, entry) in self.entries.iter_mut().enumerate() {
            entry.m_id = MessageId::new(i as u64);
        }
    }

    /// Construct a trace object from the loaded entries
    pub fn build(mut self) -> Result<Trace, TraceBuildError> {
        // check message IDs first
        self.entries.sort_unstable_by_key(|e| e.m_id);

        if self.entries.len() == 0 {
            return Err(TraceBuildError::EmptyTrace);
        }

        let mut next_msg: u64 = 0;
        for msg in self.entries.iter() {
            match msg.m_id.to_num().cmp(&next_msg) {
                Ordering::Equal => {
                    next_msg += 1;
                    continue;
                }
                Ordering::Greater => {
                    // we incremented next_msg but the entries' IDs didn't grow
                    return Err(TraceBuildError::MessageIdsNotUnique(msg.m_id));
                }
                Ordering::Less => {
                    return Err(TraceBuildError::MessageIdsHaveGaps(msg.m_id));
                }
            }
        }

        // check arrival times
        let mut previous_time: Option<PrimitiveDateTime> = None;
        for entry in self.entries.iter() {
            match previous_time {
                None => {}
                Some(prev) => {
                    if prev > entry.destination_timestamp {
                        return Err(TraceBuildError::NotSortedByArrival(entry.m_id));
                    }
                }
            }
            previous_time = Some(entry.destination_timestamp);
        }

        // check source IDs
        let sources: HashSet<SourceId> = self.entries.iter().map(|e| e.source_id).collect();
        let sources = {
            let mut v: Vec<_> = sources.into_iter().collect();
            v.sort();
            v
        };
        for (i, s) in sources.iter().enumerate() {
            if i as u64 != s.to_num() {
                return Err(TraceBuildError::SourceIdsHaveGaps(*s));
            }
        }

        let (source_mapping, destination_mapping) = self.source_and_destination_mappings();

        Ok(Trace {
            entries: self.entries,
            max_msgid: MessageId::new(next_msg - 1),
            source_mapping,
            destination_mapping,
            max_sourceid: sources.last().unwrap().clone(),
        })
    }

    /// Compute mappings (Vecs) that map each message ID to their respective
    /// source and destination.
    pub(crate) fn source_and_destination_mappings(&self) -> (SourceMapping, DestinationMapping) {
        let mut source_mapping = SourceMapping {
            data: Vec::with_capacity(self.entries.len()),
        };
        let mut dest_mapping = DestinationMapping {
            data: Vec::with_capacity(self.entries.len()),
        };

        // checked before
        // let mut next_msg_id: u64 = 0;
        for entry in self.entries.iter() {
            // if entry.m_id.to_num() != next_msg_id {
            //     panic!("Message IDs need to be sequential, starting from 0. Found messge ID {} but expected {}", entry.m_id, next_msg_id);
            // }
            source_mapping.data.push(entry.source_id);
            dest_mapping.data.push(entry.destination_id);
            // next_msg_id += 1;
        }

        (source_mapping, dest_mapping)
    }
}

/// A network trace containing the ground truth of an ACN run.
///
/// [Trace]s are meant as the "ground truth" in the way that they contain the
/// real mapping of messages at the source and destination. They are usually
/// the product of some sort of simulation in a controlled environment.
pub struct Trace {
    entries: Vec<TraceEntry>,
    max_msgid: MessageId,
    source_mapping: SourceMapping,
    destination_mapping: DestinationMapping,
    max_sourceid: SourceId,
}

impl Trace {
    /// Serialize to a CSV file
    pub fn write_to_file(
        &self,
        path: impl AsRef<Path>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let path = path.as_ref();

        let mut wtr = csv::WriterBuilder::new().from_path(path)?;
        for entry in self.entries.iter() {
            wtr.serialize(entry)?;
        }
        Ok(())
    }

    /// Get an iterator over the entries in this trace
    pub fn entries(&self) -> impl Iterator<Item = &TraceEntry> {
        self.entries.iter()
    }

    /// Get the source mapping
    pub fn get_source_mapping(&self) -> &SourceMapping {
        &self.source_mapping
    }

    /// Get the destination mapping
    pub fn get_destination_mapping(&self) -> &DestinationMapping {
        &self.destination_mapping
    }

    /// Get the maximum message ID
    pub fn max_message_id(&self) -> MessageId {
        self.max_msgid
    }

    /// Get the maximum source ID
    pub fn max_source_id(&self) -> SourceId {
        self.max_sourceid
    }

    /// Get the "sent" timestamp of a message, if the provided message ID is present in the trace.
    pub fn message_sent(&self, message_id: &MessageId) -> Option<PrimitiveDateTime> {
        // message IDs are equivalent to the index in the entries Vec
        self.entries
            .get(message_id.to_num() as usize)
            .map(|entry| entry.source_timestamp)
    }

    pub fn entries_vec(&self) -> &Vec<TraceEntry> {
        &self.entries
    }
}

/// An error that can occur when building a trace
#[derive(Debug, thiserror::Error)]
pub enum TraceBuildError {
    #[error("There are no messages in the trace.")]
    EmptyTrace,
    #[error("Destination arrival times do not monotonically increase with message IDs. Observed at message {0}.")]
    NotSortedByArrival(MessageId),
    #[error("Message IDs have gaps, but need to be sequential. Observed at message {0}.")]
    MessageIdsHaveGaps(MessageId),
    #[error("Message ID used multiple times: {0}")]
    MessageIdsNotUnique(MessageId),
    #[error("Source IDs have gaps, but need to be sequential. Observed at source {0}.")]
    SourceIdsHaveGaps(SourceId),
}

pub struct DestinationMapping {
    data: Vec<DestinationId>,
}

impl DestinationMapping {
    pub fn get(&self, msg: &MessageId) -> Option<&DestinationId> {
        self.data.get(msg.to_num() as usize)
    }

    pub(crate) fn len(&self) -> usize {
        self.data.len()
    }
}

pub struct SourceMapping {
    data: Vec<SourceId>,
}

impl SourceMapping {
    pub(crate) fn get(&self, msg: &MessageId) -> Option<&SourceId> {
        self.data.get(msg.to_num() as usize)
    }

    pub(crate) fn len(&self) -> usize {
        self.data.len()
    }
}

macro_rules! implement_display {
    ($t:ident) => {
        impl ::std::fmt::Display for $t {
            fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                write!(f, "{}", self.0)
            }
        }
    };
}

macro_rules! implement_conversions {
    ($t:ident,$b:ident) => {
        impl $t {
            pub fn new(other: $b) -> Self {
                Self(other)
            }

            pub fn to_num(self) -> $b {
                self.0
            }
        }
    };
}

/// The ID of a message in a [Trace].
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct MessageId(u64);
implement_display!(MessageId);
implement_conversions!(MessageId, u64);

/// The ID of a source entity in a [Trace].
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct SourceId(u64);
implement_display!(SourceId);
implement_conversions!(SourceId, u64);

/// The ID of a destination entity in a [Trace].
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct DestinationId(u64);
implement_display!(DestinationId);
implement_conversions!(DestinationId, u64);
