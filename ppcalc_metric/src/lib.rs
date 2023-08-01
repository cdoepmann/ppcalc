//! A crate for analyzing anonymity properties of traces from anonymous communication networks (ACNs).

mod trace;
pub use trace::{DestinationId, MessageId, SourceId};
pub use trace::{Trace, TraceBuilder, TraceEntry};

mod containers;

mod metric;
pub use metric::{
    compute_message_anonymity_sets, compute_relationship_anonymity, simple_example_generator,
};

mod bench;
