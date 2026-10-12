use serde::{Deserialize, Serialize};

use crate::{InferenceRequest, InferenceResponse, WorkerId, WorkerState};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WorkerMessage {
    Register { worker_id: WorkerId },

    Heartbeat { state: WorkerState },

    Response { response: InferenceResponse },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RouterMessage {
    RegisterAck { worker_id: WorkerId },

    Request { request: InferenceRequest },
}
