//! A crate for analyzing anonymity properties of traces from anonymous communication networks (ACNs).

mod trace;
pub use trace::{DestinationId, MessageId, SourceId};
pub use trace::{Trace, TraceBuilder, TraceEntry};

mod containers;

mod metric;
pub use metric::{
    compute_relationship_anonymity, compute_relationship_anonymity_sizes, simple_example_generator,
};

mod bench;
