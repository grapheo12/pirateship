fn round_robin_rotation(num_nodes: u64, view: u64) -> usize {
    ((view - 1) % num_nodes) as usize
}

pub fn get_current_leader(num_nodes: u64, view: u64) -> usize {
    #[cfg(feature = "round_robin_leader")]
    {
        return round_robin_rotation(num_nodes, view);
    }

    // This default should never be used.
    0 as usize
}
