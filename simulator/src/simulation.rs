use crate::{model::PerformanceModel,
    request::Request,
    worker::{ActiveRequest, Worker},
};

use crate::RoutingPolicy;

#[derive(Debug, Clone)]
pub enum SimulationEventKind {
    Arrival,
    Routed,
    Started,
    Queued,
    Completed,
}

#[derive(Debug, Clone)]
pub struct SimulationEvent {
    pub time_ms: f64,

    pub kind: SimulationEventKind,

    pub request_id: u64,

    pub worker_id: Option<usize>,

    pub cached_tokens: Option<usize>,
}

#[derive(Debug, Clone, Copy)]
pub struct SimulationConfig {
    pub number_workers: usize,
    pub cache_capacity_tokens: usize,
    pub performance: PerformanceModel,
}

#[derive(Debug, Clone)]
pub struct RequestMetrics {
    pub request_id: u64,
    pub worker_id: usize,
    pub arrival_time_ms: f64,
    pub start_time_ms: f64,
    pub completion_time_ms: f64,
    pub input_tokens: usize,
    pub cached_tokens: usize,
    pub output_tokens: usize,
}

#[derive(Debug)]
pub struct SimulationResult {
    pub requests: Vec<RequestMetrics>,

    pub events: Vec<SimulationEvent>,

    pub simulation_time_ms: f64,

    pub total_requests: usize,

    pub total_cached_tokens: usize,

    pub cache_hit_rate: f64,

    pub average_latency_ms: f64,

    pub p50_latency_ms: f64,

    pub p95_latency_ms: f64,

    pub p99_latency_ms: f64,

    pub average_queue_time_ms: f64,

    pub throughput_requests_per_second: f64,
}

pub struct Simulator<P> {
    pub workers: Vec<Worker>,
    pub policy: P,
    pub config: SimulationConfig,
}

impl<P: RoutingPolicy> Simulator<P> {
    pub fn new(
        config: SimulationConfig,
        policy: P,
    ) -> Self {
        let workers = (0..config.number_workers)
            .map(|id| {
                Worker::new(
                    id,
                    config.cache_capacity_tokens,
                )
            })
            .collect();
        
        Self {
            workers,
            policy,
            config,
        }
    }

    pub fn run(
        &mut self,
        requests: &[Request],
    ) -> SimulationResult {
        let mut current_time_ms = 0.0;

        let mut metrics = Vec::with_capacity(requests.len());

        let mut events = Vec::new();

        let mut next_request = 0;

        while next_request < requests.len()
            || self.has_active_requests()
            || self.has_queued_requests()
        {
            /*
            * Determine when the next interesting event happens.
            */
            let next_arrival = requests
                .get(next_request)
                .map(|r| r.arrival_time_ms as f64);
            
            let next_completion = self.next_completion_time(
                current_time_ms,
            );

            let next_event = match (next_arrival, next_completion) {
                (Some(arrival), Some(completion)) => {
                    arrival.min(completion)
                }

                (Some(arrival), None) => arrival,

                (None, Some(completion)) => completion,

                (None, None) => break,
            };

            /*
            * Advance simulation time.
            */
            let elasped = next_event - current_time_ms;

            self.advance_time(elasped);

            current_time_ms = next_event;


            /*
            * Finish everything that completed at this time.
            */
            self.process_completions(
                current_time_ms,
                &mut metrics,
                &mut events,
            );

            /*
             * Process all requests arriving at this timestamp.
            */
            while next_request < requests.len()
                && requests[next_request]
                   .arrival_time_ms as f64
                   <= current_time_ms
            {
                let request = 
                    requests[next_request].clone();

                events.push(SimulationEvent {
                    time_ms: current_time_ms,

                    kind: SimulationEventKind::Arrival,

                    request_id: request.id,

                    worker_id: None,

                    cached_tokens: None,
                });

                let worker_id = 
                    self.policy.choose_worker(
                        &request,
                        &mut self.workers,
                    );
                
                events.push(SimulationEvent {
                    time_ms: current_time_ms,

                    kind: SimulationEventKind::Routed,

                    request_id: request.id,

                    worker_id: Some(worker_id),

                    cached_tokens: None,
                });

                self.workers[worker_id]
                    .queue
                    .push_back(request);
                
                events.push(SimulationEvent {
                    time_ms: current_time_ms,

                    kind: SimulationEventKind::Queued,

                    request_id: 
                        self.workers[worker_id]
                            .queue
                            .back()
                            .unwrap()
                            .id,
                    worker_id: Some(worker_id),

                    cached_tokens: None,
                });

                next_request += 1;
            } 

            // Start work on every idle worker.
            self.start_queued_requests(
                current_time_ms,
                &mut events,
            );
        }

        self.build_result(
            metrics,
            events,
            current_time_ms,
        )
    }

    fn has_active_requests(&self) -> bool {
        self.workers
            .iter()
            .any(|worker| worker.active.is_some())
    }

    fn has_queued_requests(&self) -> bool {
        self.workers
            .iter()
            .any(|worker| !worker.queue.is_empty())
    }

    fn next_completion_time(
        &self,
        current_time_ms: f64,
    ) -> Option<f64> {
        self.workers
            .iter()
            .filter_map(|worker| {
                worker.active.as_ref().map(|active| {
                    current_time_ms + active.remaining_ms
                })
            })
            .min_by(|a, b| {
                a.partial_cmp(b).unwrap()
            })
    }

    fn advance_time(&mut self, elasped_ms: f64) {
        if elasped_ms <= 0.0 {
            return;
        }

        for worker in &mut self.workers {
            if let Some(active) = &mut worker.active {
                active.remaining_ms = 
                    (active.remaining_ms - elasped_ms)
                        .max(0.0);
                
                worker.total_busy_ms += elasped_ms;
            }
        }
    }


    fn start_queued_requests(
        &mut self,
        current_time_ms: f64,
        events: &mut Vec<SimulationEvent>,
    ) {
        for worker in &mut self.workers {
            if worker.active.is_some() {
                continue;
            }

            let Some(request) = worker.queue.pop_front()
            else {
                continue;
            };

            let cached_tokens = 
                worker.cache.lookup(&request.tokens);
            
            let processing_time = 
                self.config.performance.request_time(
                    request.input_tokens(),
                    cached_tokens,
                    request.output_tokens,
                );
            
            events.push(SimulationEvent {
                time_ms: current_time_ms,

                kind: SimulationEventKind::Started,

                request_id: request.id,

                worker_id: Some(worker.id),

                cached_tokens: Some(cached_tokens),
            });

            worker.active = Some(
                ActiveRequest {
                    request,
                    remaining_ms: processing_time,
                    start_time_ms: current_time_ms,
                    cached_tokens,
                }
            );
        }
    }

    fn process_completions(
        &mut self,
        current_time_ms: f64,
        metrics: &mut Vec<RequestMetrics>,
        events: &mut Vec<SimulationEvent>,
    ) {
        for worker in &mut self.workers {
            let completed = worker.active.take();
    
            let Some(active) = completed else {
                continue;
            };
    
            if active.remaining_ms > 0.000001 {
                worker.active = Some(active);
                continue;
            }

            /*
            *
            * The request has finished.
            * Its tokens can now become reusable
            * cached state.
            */
    
            let request = active.request;

            events.push(SimulationEvent {
                time_ms: current_time_ms,
            
                kind: SimulationEventKind::Completed,
            
                request_id: request.id,
            
                worker_id: Some(worker.id),
            
                cached_tokens:
                    Some(active.cached_tokens),
            });
    
            metrics.push(RequestMetrics {
                request_id: request.id,
                worker_id: worker.id,
                arrival_time_ms: request.arrival_time_ms as f64,
                start_time_ms: active.start_time_ms,
                completion_time_ms: current_time_ms,
                input_tokens: request.input_tokens(),
                cached_tokens: active.cached_tokens,
                output_tokens: request.output_tokens,
            });
    
            worker
                .cache
                .insert(&request.tokens);
    
            worker.completed_requests += 1;
        }
    }

    fn percentile(
        sorted: &[f64],
        percentile: f64,
    ) -> f64 {
        if sorted.is_empty() {
            return 0.0;
        }
    
        let index =
            percentile * (sorted.len() - 1) as f64;
    
        let lower = index.floor() as usize;
        let upper = index.ceil() as usize;
    
        if lower == upper {
            return sorted[lower];
        }
    
        let weight = index - lower as f64;
    
        sorted[lower] * (1.0 - weight)
            + sorted[upper] * weight
    }

    fn build_result(
        &self,
        requests: Vec<RequestMetrics>,
        events: Vec<SimulationEvent>,
        simulation_time_ms: f64,
    ) -> SimulationResult {
        let total_requests = requests.len();

        if total_requests == 0 {
            return SimulationResult {
                requests,
                events,
                simulation_time_ms,
                total_requests: 0,
                total_cached_tokens: 0,
                cache_hit_rate: 0.0,
                average_latency_ms: 0.0,
                p50_latency_ms: 0.0,
                p95_latency_ms: 0.0,
                p99_latency_ms: 0.0,
                average_queue_time_ms: 0.0,
                throughput_requests_per_second: 0.0,
            };
        }

        let mut latencies: Vec<f64> =
            requests
                .iter()
                .map(|r| r.completion_time_ms
                               - r.arrival_time_ms)
                .collect();

        latencies.sort_by(|a, b| {
            a.partial_cmp(b).unwrap()
        });

        let total_latency: f64 =
            latencies.iter().sum();

        let total_queue_time: f64 =
            requests
                .iter()
                .map(|r| r.start_time_ms - r.arrival_time_ms)
                .sum();

        let total_input_tokens: usize =
            requests
                .iter()
                .map(|r| r.input_tokens)
                .sum();

        let total_cached_tokens: usize =
            requests
                .iter()
                .map(|r| r.cached_tokens)
                .sum();

        let cache_hit_rate =
            if total_input_tokens == 0 {
                0.0
            } else {
                total_cached_tokens as f64
                    / total_input_tokens as f64
            };

        let average_latency =
            total_latency / total_requests as f64;

        let average_queue_time =
            total_queue_time / total_requests as f64;

        let throughput =
            if simulation_time_ms > 0.0 {
                total_requests as f64
                    / (simulation_time_ms / 1000.0)
            } else {
                0.0
            };

        SimulationResult {
            requests,

            events,

            simulation_time_ms,

            total_requests,

            total_cached_tokens,

            cache_hit_rate,

            average_latency_ms: average_latency,

            p50_latency_ms:
                Self::percentile(&latencies, 0.50),

            p95_latency_ms:
                Self::percentile(&latencies, 0.95),

            p99_latency_ms:
                Self::percentile(&latencies, 0.99),

            average_queue_time_ms:
                average_queue_time,

            throughput_requests_per_second:
                throughput,
        }
    }

}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::{
        model::PerformanceModel,
        request::Request,
        worker::Worker,
        RoutingPolicy,
    };

    /*
     * A very simple policy for testing.
     *
     * Every request goes to Worker 0.
     *
     * This makes the tests deterministic and
     * lets us explicitly test queueing and
     * cache reuse.
     */
    struct FirstWorker;

    impl RoutingPolicy for FirstWorker {
        fn choose_worker(
            &mut self,
            _request: &Request,
            _workers: &mut [Worker],
        ) -> usize {
            0
        }
    }

    fn test_config(
        num_workers: usize,
    ) -> SimulationConfig {
        SimulationConfig {
            number_workers: num_workers,

            cache_capacity_tokens: 1_000,

            performance: PerformanceModel {
                prefill_ms_per_token: 0.5,
                decode_ms_per_token: 1.0,
            },
        }
    }

    fn assert_close(
        actual: f64,
        expected: f64,
    ) {
        let epsilon = 0.0001;

        assert!(
            (actual - expected).abs() < epsilon,
            "expected {}, got {}",
            expected,
            actual,
        );
    }

    #[test]
    fn empty_workload() {
        let config = test_config(1);

        let policy = FirstWorker;

        let mut simulator =
            Simulator::new(config, policy);

        let requests = vec![];

        let result =
            simulator.run(&requests);

        assert_eq!(
            result.total_requests,
            0,
        );

        assert_close(
            result.simulation_time_ms,
            0.0,
        );

        assert_close(
            result.average_latency_ms,
            0.0,
        );

        assert_close(
            result.cache_hit_rate,
            0.0,
        );
    }

    #[test]
    fn single_request_has_expected_latency() {
        let config = test_config(1);

        let policy = FirstWorker;

        let mut simulator =
            Simulator::new(config, policy);

        let requests = vec![
            Request {
                id: 0,

                tokens: vec![1, 2, 3, 4],

                output_tokens: 4,

                arrival_time_ms: 0,
            },
        ];

        let result =
            simulator.run(&requests);

        assert_eq!(
            result.total_requests,
            1,
        );

        let request =
            &result.requests[0];

        /*
         * Input:
         *
         * 4 tokens
         *
         * Prefill:
         *
         * 4 * 0.5 = 2 ms
         *
         * Decode:
         *
         * 4 * 1.0 = 4 ms
         *
         * Total:
         *
         * 6 ms
         */

        assert_close(
            request.start_time_ms,
            0.0,
        );

        assert_close(
            request.completion_time_ms,
            6.0,
        );

        assert_close(
            request.start_time_ms
                - request.arrival_time_ms,
            0.0,
        );

        assert_close(
            request.completion_time_ms
                - request.arrival_time_ms,
            6.0,
        );

        assert_eq!(
            request.cached_tokens,
            0,
        );

        assert_close(
            result.simulation_time_ms,
            6.0,
        );
    }

    #[test]
    fn requests_queue_behind_busy_worker() {
        let config = test_config(1);

        let policy = FirstWorker;

        let mut simulator =
            Simulator::new(config, policy);

        let requests = vec![
            Request {
                id: 0,

                tokens: vec![1, 2, 3, 4],

                output_tokens: 4,

                arrival_time_ms: 0,
            },

            Request {
                id: 1,

                tokens: vec![10, 20, 30, 40],

                output_tokens: 4,

                arrival_time_ms: 2,
            },
        ];

        let result =
            simulator.run(&requests);

        assert_eq!(
            result.total_requests,
            2,
        );

        let r0 =
            result
                .requests
                .iter()
                .find(|r| r.request_id == 0)
                .unwrap();

        let r1 =
            result
                .requests
                .iter()
                .find(|r| r.request_id == 1)
                .unwrap();

        /*
         * R0:
         *
         * arrival = 0
         * start   = 0
         *
         * execution:
         *
         * prefill = 4 * 0.5 = 2
         * decode  = 4 * 1.0 = 4
         *
         * total = 6 ms
         *
         * completes at 6 ms
         */

        assert_close(
            r0.completion_time_ms,
            6.0,
        );

        /*
         * R1 arrives at 2 ms.
         *
         * Worker is busy until 6 ms.
         *
         * queue time:
         *
         * 6 - 2 = 4 ms
         */

        assert_close(
            r1.start_time_ms,
            6.0,
        );

        // assert_close(
        //     r1.queue_time_ms(),
        //     4.0,
        // );

        /*
         * R1 has no cache hit.
         *
         * Its processing time is also 6 ms.
         */

        assert_close(
            r1.completion_time_ms,
            12.0,
        );

        /*
         * Total latency:
         *
         * 12 - 2 = 10 ms
         */

        // assert_close(
        //     r1.latency_ms(),
        //     10.0,
        // );
    }

    #[test]
    fn completed_request_populates_cache() {
        let config = test_config(1);

        let policy = FirstWorker;

        let mut simulator =
            Simulator::new(config, policy);

        let requests = vec![
            Request {
                id: 0,

                tokens: vec![1, 2, 3, 4],

                output_tokens: 4,

                arrival_time_ms: 0,
            },

            Request {
                id: 1,

                /*
                 * Shares:
                 *
                 * 1 2 3
                 *
                 * with R0.
                 */
                tokens: vec![1, 2, 3, 5],

                output_tokens: 4,

                arrival_time_ms: 2,
            },
        ];

        let result =
            simulator.run(&requests);

        let r0 =
            result
                .requests
                .iter()
                .find(|r| r.request_id == 0)
                .unwrap();

        let r1 =
            result
                .requests
                .iter()
                .find(|r| r.request_id == 1)
                .unwrap();

        /*
         * R0 finishes at:
         *
         * 6 ms
         */

        assert_close(
            r0.completion_time_ms,
            6.0,
        );

        /*
         * R1 starts after R0.
         */

        assert_close(
            r1.start_time_ms,
            6.0,
        );

        /*
         * R0:
         *
         * 1 2 3 4
         *
         * R1:
         *
         * 1 2 3 5
         *
         * Longest common prefix:
         *
         * 1 2 3
         *
         * Therefore:
         */

        assert_eq!(
            r1.cached_tokens,
            3,
        );

        /*
         * Only one input token remains
         * to prefill.
         *
         * Prefill:
         *
         * 1 * 0.5 = 0.5 ms
         *
         * Decode:
         *
         * 4 * 1.0 = 4 ms
         *
         * Total:
         *
         * 4.5 ms
         */

        assert_close(
            r1.completion_time_ms,
            10.5,
        );

        /*
         * R1 arrived at 2 ms.
         *
         * Completion = 10.5 ms.
         *
         * Latency:
         *
         * 10.5 - 2 = 8.5 ms
         */

        assert_close(
            r1.completion_time_ms -
                r1.arrival_time_ms,
            8.5,
        );
    }

    #[test]
    fn simulation_tracks_cache_hit_rate() {
        let config = test_config(1);

        let policy = FirstWorker;

        let mut simulator =
            Simulator::new(config, policy);

        let requests = vec![
            Request {
                id: 0,

                tokens: vec![1, 2, 3, 4],

                output_tokens: 1,

                arrival_time_ms: 0,
            },

            Request {
                id: 1,

                tokens: vec![1, 2, 3, 5],

                output_tokens: 1,

                arrival_time_ms: 10,
            },
        ];

        let result =
            simulator.run(&requests);

        /*
         * Total input tokens:
         *
         * R0 = 4
         * R1 = 4
         *
         * Total = 8
         *
         * Cached:
         *
         * R0 = 0
         * R1 = 3
         *
         * Total = 3
         *
         * Cache hit rate:
         *
         * 3 / 8 = 0.375
         */

        assert_close(
            result.cache_hit_rate,
            3.0 / 8.0,
        );

        assert_eq!(
            result.total_cached_tokens,
            3,
        );
    }
}