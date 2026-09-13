use simulator::{request::Request, worker::Worker};

use crate::RoutingPolicy;



pub struct Hybrid {
    pub cache_weight: f64,
    pub load_weight: f64,
}

impl Hybrid {
    pub fn new(cache_weight: f64, load_weight: f64) -> Self {
        Self {
            cache_weight,
            load_weight,
        }
    }
}

impl RoutingPolicy for Hybrid {
    fn choose_worker(
        &mut self,
        request: &Request,
        workers: &mut [Worker],
    ) -> usize {
        workers
            .iter_mut()
            .enumerate()
            .map(|(id, worker)| {
                let cache_tokens = 
                    worker.cache.lookup(&request.tokens);
                
                    let cache_ratio = 
                        cache_tokens as f64 /
                        request.tokens.len().max(1) as f64;
                    
                    let load = worker.load();

                    let score = 
                        self.cache_weight * cache_ratio
                        - self.load_weight * load;
                    
                    (id, score)
            })
            .max_by(|(_, a), (_, b)| {
                a.partial_cmp(b).unwrap()
            })
            .map(|(id, _)| id)
            .unwrap()
    }
}