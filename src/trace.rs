use csv::{ReaderBuilder, WriterBuilder};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, hash::Hash, vec};
use time::PrimitiveDateTime;

#[derive(Serialize, Deserialize)]
pub struct SourceTrace {
    pub source_name: String,
    pub timestamps: Vec<PrimitiveDateTime>,
}

#[derive(Serialize, Deserialize)]
pub struct SourceDestinationMapEntry {
    source: String,
    destination: String,
}

pub struct Trace {
    pub entries: Vec<TraceEntry>,
}

#[derive(Serialize, Deserialize)]
pub struct PreNetworkTraceEntry {
    pub source_name: String,
    pub source_timestamp: PrimitiveDateTime,
    pub destination_name: String,
}

#[derive(Serialize, Deserialize)]
pub struct TraceEntry {
    pub source_name: String,
    pub source_timestamp: PrimitiveDateTime,
    pub destination_name: String,
    pub destination_timestamp: PrimitiveDateTime,
}

impl SourceTrace {
    pub fn write_to_file(&self, path: &str) -> Result<(), Box<dyn std::error::Error>> {
        let mut wtr = WriterBuilder::new().has_headers(false).from_path(path)?;
        wtr.serialize(self)?;
        Ok(())
    }
}
pub fn read_source_trace_from_file(path: &str) -> Result<SourceTrace, Box<dyn std::error::Error>> {
    let mut rdr = ReaderBuilder::new().has_headers(false).from_path(path)?;
    let mut iter = rdr.deserialize();

    if let Some(result) = iter.next() {
        let source_trace: SourceTrace = result?;
        Ok(source_trace)
    } else {
        Err(From::from("expected at least one record but got none"))
    }
}

pub fn write_source_destination_map(
    map: HashMap<String, String>,
    path: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut wtr = WriterBuilder::new().from_path(path)?;
    for (key, value) in map.iter() {
        wtr.serialize(SourceDestinationMapEntry {
            source: key.to_string(),
            destination: value.to_string(),
        })?;
    }
    Ok(())
}

pub fn read_source_destination_map_from_file(
    path: &str,
) -> Result<HashMap<String, String>, Box<dyn std::error::Error>> {
    let mut rdr = ReaderBuilder::new().from_path(path)?;
    let mut iter = rdr.deserialize();
    let mut map: HashMap<String, String> = HashMap::new();

    while let Some(result) = iter.next() {
        let entry: SourceDestinationMapEntry = result?;
        map.insert(entry.source, entry.destination);
    }
    Ok(map)
}

pub fn read_network_trace_from_file(path: &str) -> Result<Trace, Box<dyn std::error::Error>> {
    let mut rdr = ReaderBuilder::new().from_path(path)?;
    let mut iter = rdr.deserialize();
    let mut entries: Vec<TraceEntry> = vec![];

    while let Some(result) = iter.next() {
        let entry: TraceEntry = result?;
        entries.push(entry);
    }
    Ok(Trace { entries })
}
impl Trace {
    pub fn write_to_file(&self, path: &str) -> Result<(), Box<dyn std::error::Error>> {
        let mut wtr = WriterBuilder::new().has_headers(false).from_path(path)?;
        for entry in self.entries.iter() {
            wtr.serialize(entry)?;
        }
        Ok(())
    }
}
