//! A crate for analyzing anonymity properties of traces from anonymous communication networks (ACNs).

mod trace;
pub use trace::{DestinationId, MessageId, SourceId};
pub use trace::{Trace, TraceEntry};

mod metric;
pub use metric::{
    compute_message_anonymity_sets, compute_relation_ship_anonymity_sets,
    compute_relationship_anonymity,
};
