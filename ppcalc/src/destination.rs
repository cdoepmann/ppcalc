use std::collections::HashMap;

use rand::{distributions::Uniform, prelude::Distribution};
use serde::{Deserialize, Serialize};

use ppcalc_metric::{DestinationId, SourceId};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum DestinationSelectionType {
    Uniform,
    RoundRobin,
    Normal,
}

pub fn destination_selection(
    selection_type: &DestinationSelectionType,
    number_of_destinations: u64,
    source_id_list: Vec<SourceId>,
) -> HashMap<SourceId, DestinationId> {
    match selection_type {
        DestinationSelectionType::Uniform => {
            uniform_destination_selection(number_of_destinations, source_id_list)
        }
        DestinationSelectionType::RoundRobin => {
            round_robin_destination_selection(number_of_destinations, source_id_list)
        }
        DestinationSelectionType::Normal => {
            normal_destination_selection(number_of_destinations, source_id_list)
        }
    }
}

pub fn uniform_destination_selection(
    number_of_destinations: u64,
    source_id_list: Vec<SourceId>,
) -> HashMap<SourceId, DestinationId> {
    let mut map = HashMap::new();
    let distr = Uniform::from(0..number_of_destinations);
    let mut rng = rand::thread_rng();
    for source_id in source_id_list {
        map.insert(source_id, DestinationId::new(distr.sample(&mut rng)));
    }
    map
}

pub fn round_robin_destination_selection(
    number_of_destinations: u64,
    source_id_list: Vec<SourceId>,
) -> HashMap<SourceId, DestinationId> {
    let mut map = HashMap::new();
    for (i, source_id) in source_id_list.into_iter().enumerate() {
        map.insert(
            source_id,
            DestinationId::new((i % number_of_destinations as usize) as u64),
        );
    }
    map
}

// TODO
pub fn normal_destination_selection(
    _number_of_destinations: u64,
    _source_id_list: Vec<SourceId>,
) -> HashMap<SourceId, DestinationId> {
    unimplemented!("Choosing destinations based on a normal distribution isn't implemented yet.")
}
