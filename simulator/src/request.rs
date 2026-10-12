#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Request {
    pub id: u64,

    // Token IDs making up the input/prompt.
    pub tokens: Vec<u32>,

    // Number of tokens  we ask the model to generate.
    pub output_tokens: usize,

    // when the request enters the system.
    pub arrival_time_ms: u64,
}

impl Request {
    pub fn input_tokens(&self) -> usize {
        self.tokens.len()
    }
}
