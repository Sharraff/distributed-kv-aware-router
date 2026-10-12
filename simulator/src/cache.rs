use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct CacheEntry {
    pub tokens: usize,
    pub last_used: u64,
}

#[derive(Debug)]
pub struct PrefixCache {
    entries: HashMap<Vec<u32>, CacheEntry>,
    capacity_tokens: usize,
    used_tokens: usize,
    clock: u64,
}

impl PrefixCache {
    pub fn new(capacity_tokens: usize) -> Self {
        Self {
            entries: HashMap::new(),
            capacity_tokens,
            used_tokens: 0,
            clock: 0,
        }
    }

    pub fn lookup(&mut self, tokens: &[u32]) -> usize {
        self.clock += 1;

        let mut longest = 0;

        for len in (1..=tokens.len()).rev() {
            let prefix = tokens[..len].to_vec();

            if let Some(entry) = self.entries.get_mut(&prefix) {
                entry.last_used = self.clock;
                longest = len;
                break;
            }
        }

        longest
    }

    pub fn insert(&mut self, tokens: &[u32]) {
        self.clock += 1;

        // Insert every prefix.
        for len in 1..=tokens.len() {
            let prefix = tokens[..len].to_vec();

            if self.entries.contains_key(&prefix) {
                continue;
            }

            while self.used_tokens + len > self.capacity_tokens {
                if !self.evict_one() {
                    return;
                }
            }

            self.entries.insert(
                prefix,
                CacheEntry {
                    tokens: len,
                    last_used: self.clock,
                },
            );

            self.used_tokens += len;
        }
    }

    fn evict_one(&mut self) -> bool {
        let victim = self
            .entries
            .iter()
            .min_by_key(|(_, entry)| entry.last_used)
            .map(|(key, _)| key.clone());

        match victim {
            Some(key) => {
                if let Some(entry) = self.entries.remove(&key) {
                    self.used_tokens -= entry.tokens;
                }

                true
            }

            None => false,
        }
    }

    pub fn used_tokens(&self) -> usize {
        self.used_tokens
    }

    pub fn capacity_tokens(&self) -> usize {
        self.capacity_tokens
    }

    pub fn usage(&self) -> f64 {
        self.used_tokens as f64 / self.capacity_tokens as f64
    }
}
