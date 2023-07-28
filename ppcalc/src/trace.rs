use std::path::Path;

use serde::{Deserialize, Serialize};
use time::PrimitiveDateTime;

use ppcalc_metric::{DestinationId, SourceId, TraceBuilder};

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

/// Reconstruct sources and their behavior from a network trace file
pub fn read_sources_from_trace(
    path: impl AsRef<Path>,
) -> Result<Vec<SourceTrace>, Box<dyn std::error::Error + Send + Sync>> {
    let path = path.as_ref();

    // load the trace
    let trace = TraceBuilder::from_csv(path)?.build()?;

    // create a SourceTrace per source
    let mut result: Vec<SourceTrace> = (0..=trace.max_source_id().to_num())
        .map(|source_id| SourceTrace {
            source_id: SourceId::new(source_id),
            timestamps: Vec::new(),
        })
        .collect();

    // Collect send times per source
    for entry in trace.entries() {
        result[entry.source_id.to_num() as usize]
            .timestamps
            .push(entry.source_timestamp);
    }

    Ok(result)
}
