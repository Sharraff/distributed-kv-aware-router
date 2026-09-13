pub mod request;
pub mod response;
pub mod worker_state;

pub type RequestId = u64;
pub type WorkerId = u64;

pub use request::InferenceRequest;
pub use response::InferenceResponse;
pub use worker_state::WorkerState;