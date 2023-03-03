use std::collections::HashMap;

use rand::{distributions::Uniform, prelude::Distribution};

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
