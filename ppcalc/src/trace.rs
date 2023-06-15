use csv::WriterBuilder;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fs::File, io::BufReader};
use time::PrimitiveDateTime;

use ppcalc_metric::{DestinationId, SourceId};

#[derive(Serialize, Deserialize)]
pub struct SourceTrace {
    pub source_id: SourceId,
    pub timestamps: Vec<PrimitiveDateTime>,
}

#[derive(Serialize, Deserialize)]
pub struct SourceDestinationMapEntry {
    source: SourceId,
    destination: DestinationId,
}

#[derive(Serialize, Deserialize)]
pub struct PreNetworkTraceEntry {
    pub source_id: SourceId,
    pub source_timestamp: PrimitiveDateTime,
    pub destination_id: DestinationId,
}

// impl SourceTrace {
//     pub fn write_to_file(&self, path: &str) -> Result<(), Box<dyn std::error::Error>> {
//         let mut wtr = WriterBuilder::new().has_headers(false).from_path(path)?;
//         wtr.serialize(self)?;
//         Ok(())
//     }
// }

pub fn write_sources(
    path: &str,
    traces: &Vec<SourceTrace>,
) -> Result<(), Box<dyn std::error::Error>> {
    let wtr = std::fs::File::create(path)?;

    serde_json::to_writer(&wtr, traces)?;
    Ok(())
}
pub fn read_source_trace_from_file(
    path: &str,
) -> Result<Vec<SourceTrace>, Box<dyn std::error::Error>> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);

    // Read the JSON contents of the file as an instance of `User`.
    let source_trace = serde_json::from_reader(reader)?;
    Ok(source_trace)
}

pub fn write_source_destination_map(
    map: &HashMap<SourceId, DestinationId>,
    path: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut wtr = WriterBuilder::new().from_path(path)?;
    for (key, value) in map.iter() {
        wtr.serialize(SourceDestinationMapEntry {
            source: *key,
            destination: *value,
        })?;
    }
    Ok(())
}
