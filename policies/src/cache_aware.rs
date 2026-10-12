use simulator::{request::Request, worker::Worker};

use crate::RoutingPolicy;

pub struct CacheAware;

impl RoutingPolicy for CacheAware {
    fn choose_worker(&mut self, request: &Request, workers: &mut [Worker]) -> usize {
        workers
            .iter_mut()
            .enumerate()
            .map(|(id, worker)| {
                let cache_hit = worker.cache.lookup(&request.tokens);

                (id, cache_hit)
            })
            .max_by_key(|(_, cache_hit)| *cache_hit)
            .map(|(id, _)| id)
            .unwrap()
    }
}
