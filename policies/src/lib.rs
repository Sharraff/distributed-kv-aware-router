pub mod cache_aware;
pub mod hybrid;
pub mod least_loaded;
pub mod round_robin;

//pub use simulator::{request::Request, worker::Woker};
pub use simulator::RoutingPolicy;

pub use cache_aware::CacheAware;
pub use hybrid::Hybrid;
pub use least_loaded::LeastLoaded;
pub use round_robin::RoundRobin;


// pub trait RoutingPolicy {
//     fn choose_worker(
//         &mut self,
//         request: &Request,
//         workers: &mut [Worker],
//     ) -> usize;
// }