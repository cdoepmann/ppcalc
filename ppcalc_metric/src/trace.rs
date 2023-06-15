use std::path::Path;

use serde::{Deserialize, Serialize};
use time::PrimitiveDateTime;

/// A network trace containing the ground truth of an ACN run.
///
/// [Trace]s are meant as the "ground truth" in the way that they contain the
/// real mapping of messages at the source and destination. They are usually
/// the product of some sort of simulation in a controlled environment.
pub struct Trace {
    pub entries: Vec<TraceEntry>,
}

/// A single entry within a provided [Trace].
///
/// It contains the real information when a message was sent and received,
/// and by whom.
#[derive(Serialize, Deserialize)]
pub struct TraceEntry {
    pub m_id: u64,
    pub source_id: u64,
    pub source_timestamp: PrimitiveDateTime,
    pub destination_id: u64,
    pub destination_timestamp: PrimitiveDateTime,
}

impl Trace {
    /// Construct a new, empty trace object
    pub fn new() -> Trace {
        Trace {
            entries: Vec::new(),
        }
    }

    /// Load a full trace from a CSV file, given its file path
    pub fn from_csv(path: impl AsRef<Path>) -> Result<Trace, Box<dyn std::error::Error>> {
        let path = path.as_ref();

        let mut rdr = csv::ReaderBuilder::new().from_path(path)?;
        let mut iter = rdr.deserialize();
        let mut entries: Vec<TraceEntry> = vec![];

        while let Some(result) = iter.next() {
            let entry: TraceEntry = result?;
            entries.push(entry);
        }
        Ok(Trace { entries })
    }

    // pub fn write_to_file(&self, path: &str) -> Result<(), Box<dyn std::error::Error>> {
    //     let mut wtr = WriterBuilder::new().from_path(path)?;
    //     for entry in self.entries.iter() {
    //         wtr.serialize(entry)?;
    //     }
    //     Ok(())
    // }
}
