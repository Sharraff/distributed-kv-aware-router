use serde::{Deserialize, Serialize};

use crate::{RequestId, WorkerId};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InferenceResponse {
    pub request_id: RequestId,

    /// The worker that processed the request.
    pub worker_id: WorkerId,

    /// Generated output tokens.
    pub output_tokens: Vec<u32>,
}

//pub type WorkerId = u64;
