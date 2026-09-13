pub mod cache;
pub mod model;
pub mod request;
pub mod simulation;
pub mod worker;
pub mod workload;

use request::Request;
use worker::Worker;

pub trait RoutingPolicy {
    fn choose_worker(
        &mut self,
        request: &Request,
        workers: &mut [Worker],
    ) -> usize;
}