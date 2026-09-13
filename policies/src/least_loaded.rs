use simulator::{request::Request, worker::Worker};

use crate::RoutingPolicy;

pub struct LeastLoaded;

impl RoutingPolicy for LeastLoaded {
    fn choose_worker(
        &mut self,
        _request: &Request,
        workers: &mut [Worker],
    ) -> usize {
        workers
            .iter()
            .enumerate()
            .min_by(|(_, a), (_, b)| {
                a.load()
                    .partial_cmp(&b.load())
                    .unwrap()
            })
            .map(|(id, _)| id)
            .unwrap()
    }
}