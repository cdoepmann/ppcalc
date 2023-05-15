use std::collections::HashMap;

use rand::{distributions::Uniform, prelude::Distribution};
use statrs::distribution::Normal;

pub enum DestinationSelectionType {
    Uniform,
    RoundRobin,
    SmallWorld,
    Normal,
}

pub fn destination_selection(
    selection_type: DestinationSelectionType,
    number_of_destinations: u64,
    source_name_list: Vec<String>,
) -> HashMap<String, String> {
    match selection_type {
        DestinationSelectionType::Uniform => {
            uniform_destination_selection(number_of_destinations, source_name_list)
        }
        DestinationSelectionType::RoundRobin => {
            round_robin_destination_selection(number_of_destinations, source_name_list)
        }
        DestinationSelectionType::SmallWorld => {
            small_world_destination_selection(number_of_destinations, source_name_list)
        }
        DestinationSelectionType::Normal => {
            normal_destination_selection(number_of_destinations, source_name_list)
        }
    }
}

pub fn uniform_destination_selection(
    number_of_destinations: u64,
    source_name_list: Vec<String>,
) -> HashMap<String, String> {
    let mut map: HashMap<String, String> = HashMap::new();
    let distr = Uniform::from(0..number_of_destinations);
    let mut rng = rand::thread_rng();
    for source_name in source_name_list {
        map.insert(source_name, distr.sample(&mut rng).to_string());
    }
    map
}

pub fn round_robin_destination_selection(
    number_of_destinations: u64,
    source_name_list: Vec<String>,
) -> HashMap<String, String> {
    let mut map: HashMap<String, String> = HashMap::new();
    let distr = Uniform::from(0..number_of_destinations);
    let mut rng = rand::thread_rng();
    for (i, source_name) in source_name_list.into_iter().enumerate() {
        map.insert(
            source_name,
            (i % number_of_destinations as usize).to_string(),
        );
    }
    map
}

pub fn small_world_destination_selection(
    number_of_destinations: u64,
    source_name_list: Vec<String>,
) -> HashMap<String, String> {
    /* TODO */
    let mut map: HashMap<String, String> = HashMap::new();
    panic!("Small world destination selection is not implemented yet");
    map
}

pub fn normal_destination_selection(
    number_of_destinations: u64,
    source_name_list: Vec<String>,
) -> HashMap<String, String> {
    let mut map: HashMap<String, String> = HashMap::new();
    let distr = Normal::new(100.0, 10.0).unwrap();
    let mut rng = rand::thread_rng();
    for source_name in source_name_list {
        map.insert(source_name, distr.sample(&mut rng).to_string());
    }
    map
}
