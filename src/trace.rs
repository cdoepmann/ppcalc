use csv::{ReaderBuilder, WriterBuilder};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fs::File, io::BufReader, vec};
use time::PrimitiveDateTime;

#[derive(Serialize, Deserialize)]
pub struct SourceTrace {
    pub source_id: u64,
    pub timestamps: Vec<PrimitiveDateTime>,
}

#[derive(Serialize, Deserialize)]
pub struct SourceDestinationMapEntry {
    source: u64,
    destination: u64,
}

pub struct Trace {
    pub entries: Vec<TraceEntry>,
}

#[derive(Serialize, Deserialize)]
pub struct PreNetworkTraceEntry {
    pub source_id: u64,
    pub source_timestamp: PrimitiveDateTime,
    pub destination_id: u64,
}

#[derive(Serialize, Deserialize)]
pub struct TraceEntry {
    pub m_id: u64,
    pub source_id: u64,
    pub source_timestamp: PrimitiveDateTime,
    pub destination_id: u64,
    pub destination_timestamp: PrimitiveDateTime,
}

impl SourceTrace {
    pub fn write_to_file(&self, path: &str) -> Result<(), Box<dyn std::error::Error>> {
        let mut wtr = WriterBuilder::new().has_headers(false).from_path(path)?;
        wtr.serialize(self)?;
        Ok(())
    }
}
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
    map: &HashMap<u64, u64>,
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

pub fn read_source_destination_map_from_file(
    path: &str,
) -> Result<HashMap<u64, u64>, Box<dyn std::error::Error>> {
    let mut rdr = ReaderBuilder::new().from_path(path)?;
    let mut iter = rdr.deserialize();
    let mut map: HashMap<u64, u64> = HashMap::new();

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
        let mut wtr = WriterBuilder::new().from_path(path)?;
        for entry in self.entries.iter() {
            wtr.serialize(entry)?;
        }
        Ok(())
    }
}
