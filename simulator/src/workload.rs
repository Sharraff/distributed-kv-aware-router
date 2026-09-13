use rand::{
    rngs::StdRng,
    //Rng,
    SeedableRng,
};

use rand::RngExt;
use crate::request::Request;


#[derive(Debug, Clone)]
pub struct SyntheticWorkloadConfig {
    pub num_requests: usize,

    pub num_prefixes: usize,

    pub prefix_length: usize,

    pub suffix_length: usize,

    pub output_tokens: usize,

    pub locality: f64,

    pub arrival_interval_ms: u64,

    pub vocab_size: u32,

    pub seed: u64,
}

#[derive(Debug)]
pub enum WorkloadError {
    Zeroprefixes,

    TooManyPrefixes,

    InvalidLocality,

    ZeroVocabulary,
}

pub fn generate_synthetic_workload(
    config: &SyntheticWorkloadConfig,
) -> Result<Vec<Request>, WorkloadError> {
    validate_config(config)?;

    let mut rng = StdRng::seed_from_u64(config.seed);

    let prefixes = generate_prefixes(config, &mut rng,);

    let mut requests = Vec::with_capacity(config.num_requests);

    for id in 0..config.num_requests {
        let use_shared_prefix = rng.random::<f64>() < config.locality;
        let tokens = if use_shared_prefix {
            generate_local_request(
                config,
                &prefixes,
                &mut rng,
            )
        } else {
            generate_random_request(
                config,
                &mut rng,
            )
        };

        requests.push(Request {
            id: id as u64,

            tokens,

            output_tokens:
                config.output_tokens,

            arrival_time_ms:
                id as u64 * config
                                .arrival_interval_ms,
        });
    }
    Ok(requests)
}

fn validate_config(
    config: &SyntheticWorkloadConfig,
) -> Result<(), WorkloadError> {
    if config.num_prefixes == 0 {
        return Err(WorkloadError::Zeroprefixes);
    }

    if config.vocab_size == 0 {
        return Err(WorkloadError::ZeroVocabulary);
    }

    if config.num_prefixes > config.vocab_size as usize {
        return Err(WorkloadError::TooManyPrefixes);
    }

    if !(0.0..=1.0).contains(&config.locality) {
        return Err(WorkloadError::InvalidLocality);
    }

    Ok(())
}

fn generate_prefixes(
    config: &SyntheticWorkloadConfig,
    rng: &mut StdRng,
) -> Vec<Vec<u32>> {
    let mut prefixes = Vec::with_capacity(config.num_prefixes);

    for prefix_id in 0..config.num_prefixes {
        let mut prefix = Vec::with_capacity(
            config.prefix_length,
        );

        if config.prefix_length > 0 {
            prefix.push(
                prefix_id as u32,
            );
        }

        for _ in 1..config.prefix_length {
            prefix.push(
                rng.random_range(
                    0..config.vocab_size,
                ),
            );
        }

        prefixes.push(prefix);
    }

    prefixes
}

fn generate_local_request(
    config: &SyntheticWorkloadConfig,
    prefixes: &[Vec<u32>],
    rng: &mut StdRng,
) -> Vec<u32> {
    let prefix_index =
        rng.random_range(
            0..prefixes.len(),
        );

    let prefix =
        &prefixes[prefix_index];

    let mut tokens =
        Vec::with_capacity(
            config.prefix_length
                + config.suffix_length,
        );

    tokens.extend_from_slice(
        prefix,
    );

    for _ in 0..config.suffix_length {
        tokens.push(
            rng.random_range(
                0..config.vocab_size,
            ),
        );
    }

    tokens
}

fn generate_random_request(
    config: &SyntheticWorkloadConfig,
    rng: &mut StdRng,
) -> Vec<u32> {
    let total_length =
        config.prefix_length
            + config.suffix_length;

    let mut tokens =
        Vec::with_capacity(
            total_length,
        );

    for _ in 0..total_length {
        tokens.push(
            rng.random_range(
                0..config.vocab_size,
            ),
        );
    }

    tokens
}


pub fn deterministic_workload() -> Vec<Request> {
    vec![
        Request {
            id: 0,

            // A B C D
            tokens: vec![1, 2, 3, 4],

            output_tokens: 4,

            arrival_time_ms: 0,
        },

        Request {
            id: 1,

            // X Y Z W
            tokens: vec![10, 20, 30, 40],

            output_tokens: 5,

            arrival_time_ms: 0,
        },

        Request {
            id: 2,

            // A B C E
            //
            // Shares:
            //
            // A B C
            //
            // with Request 0.
            tokens: vec![1, 2, 3, 5],

            output_tokens: 4,

            arrival_time_ms: 2,
        },
    ]
}

pub fn generate_workload(
    num_requests: usize,
) -> Vec<Request> {
    let mut requests = Vec::with_capacity(num_requests);

    for i in 0..num_requests {
        let prefix = if i % 2 == 0 {
            vec![1, 2, 3, 4, 5]
        } else {
            vec![10, 20, 30]
        };

        let mut tokens = prefix;

        tokens.extend([
            (i % 100) as u32,
            ((i + 1) % 100) as u32,
            ((i + 2) % 100) as u32,
        ]);

        requests.push(Request {
            id: i as u64,
            tokens,
            output_tokens: 50,
            arrival_time_ms: i as u64,
        });
    }

    requests
}

#[cfg(test)]
mod tests {
    use super::*;


    fn test_config() -> SyntheticWorkloadConfig {
        SyntheticWorkloadConfig {
            num_requests: 100,

            num_prefixes: 4,

            prefix_length: 3,

            suffix_length: 5,

            output_tokens: 10,

            locality: 1.0,

            arrival_interval_ms: 2,

            vocab_size: 10_000,

            seed: 42,
        }
    }


    #[test]
    fn same_seed_produces_same_workload() {
        let config =
            test_config();

        let workload_a =
            generate_synthetic_workload(
                &config,
            )
            .unwrap();

        let workload_b =
            generate_synthetic_workload(
                &config,
            )
            .unwrap();

        assert_eq!(
            workload_a,
            workload_b,
        );
    }


    #[test]
    fn generates_requested_number_of_requests() {
        let mut config =
            test_config();

        config.num_requests = 1_000;

        let workload =
            generate_synthetic_workload(
                &config,
            )
            .unwrap();

        assert_eq!(
            workload.len(),
            1_000,
        );
    }


    #[test]
    fn generated_requests_have_correct_lengths() {
        let config =
            test_config();

        let workload =
            generate_synthetic_workload(
                &config,
            )
            .unwrap();

        let expected_length =
            config.prefix_length
                + config.suffix_length;

        for request in workload {
            assert_eq!(
                request.tokens.len(),
                expected_length,
            );

            assert_eq!(
                request.output_tokens,
                config.output_tokens,
            );
        }
    }


    #[test]
    fn request_arrival_times_are_ordered() {
        let config =
            test_config();

        let workload =
            generate_synthetic_workload(
                &config,
            )
            .unwrap();

        for window
            in workload.windows(2)
        {
            let first =
                &window[0];

            let second =
                &window[1];

            assert!(
                first.arrival_time_ms
                    <= second.arrival_time_ms
            );
        }
    }


    #[test]
    fn arrival_interval_is_respected() {
        let config =
            test_config();

        let workload =
            generate_synthetic_workload(
                &config,
            )
            .unwrap();

        for (index, request)
            in workload.iter().enumerate()
        {
            let expected_time =
                index as u64
                    * config
                        .arrival_interval_ms;

            assert_eq!(
                request.arrival_time_ms,
                expected_time,
            );
        }
    }


    #[test]
    fn full_locality_with_one_prefix_shares_prefix() {
        let mut config =
            test_config();

        config.num_prefixes = 1;

        config.locality = 1.0;

        config.prefix_length = 4;

        let workload =
            generate_synthetic_workload(
                &config,
            )
            .unwrap();

        let first_prefix =
            &workload[0]
                .tokens
                [..config.prefix_length];

        for request in &workload {
            let prefix =
                &request.tokens
                    [..config.prefix_length];

            assert_eq!(
                prefix,
                first_prefix,
            );
        }
    }


    #[test]
    fn request_ids_are_sequential() {
        let config =
            test_config();

        let workload =
            generate_synthetic_workload(
                &config,
            )
            .unwrap();

        for (index, request)
            in workload.iter().enumerate()
        {
            assert_eq!(
                request.id,
                index as u64,
            );
        }
    }


    #[test]
    fn rejects_invalid_locality() {
        let mut config =
            test_config();

        config.locality = 1.5;

        let result =
            generate_synthetic_workload(
                &config,
            );

        assert!(
            matches!(
                result,
                Err(
                    WorkloadError::InvalidLocality
                )
            )
        );
    }


    #[test]
    fn rejects_zero_prefixes() {
        let mut config =
            test_config();

        config.num_prefixes = 0;

        let result =
            generate_synthetic_workload(
                &config,
            );

        assert!(
            matches!(
                result,
                Err(
                    WorkloadError::Zeroprefixes
                )
            )
        );
    }
}