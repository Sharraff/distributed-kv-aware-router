use serde::{Deserialize, Serialize};

use crate::RequestId;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InferenceRequest {
    pub request_id: RequestId,

    /// Input tokens for the request.
    pub tokens: Vec<u32>,

    /// Number of output tokens to generate.
    pub output_tokens: usize,
}
