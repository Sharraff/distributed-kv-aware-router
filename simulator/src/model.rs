#[derive(Debug, Clone, Copy)]
pub struct PerformanceModel {
    // Time to process one uncached input token
    pub prefill_ms_per_token: f64,

    // Time to generate one output token.
    pub decode_ms_per_token: f64,
}

impl PerformanceModel {
    pub fn request_time(
        &self,
        input_tokens: usize,
        cached_tokens: usize,
        output_tokens: usize,
    ) -> f64 {
        let uncached_tokens = input_tokens.saturating_sub(cached_tokens);

        let prefill = uncached_tokens as f64 * self.prefill_ms_per_token;

        let decode = output_tokens as f64 * self.decode_ms_per_token;

        prefill + decode
    }
}
