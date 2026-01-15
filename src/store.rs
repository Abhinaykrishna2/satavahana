use crate::models::Tick;
use crossbeam::utils::CachePadded;
use dashmap::DashMap;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Clone)]
pub struct TickStore {
    hot_path: Arc<Vec<CachePadded<AtomicTick>>>,
    hot_token_map: Arc<HashMap<u32, usize>>,

    cold_path: Arc<DashMap<u32, Tick>>,
}

#[derive(Default)]
struct AtomicTick {
    token_and_valid: AtomicU64,
    ltp_paise: AtomicU64,
    last_update_us: AtomicU64,
}

impl AtomicTick {
    fn new() -> Self {
        Self::default()
    }

    #[inline(always)]
    fn update(&self, tick: &Tick) {
        let ltp = (tick.ltp * 100.0) as u64;
        self.ltp_paise.store(ltp, Ordering::Release);

        let token_valid = ((tick.token as u64) << 32) | 1u64;
        self.token_and_valid.store(token_valid, Ordering::Release);

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;
        self.last_update_us.store(now, Ordering::Release);
    }

    #[inline(always)]
    fn read(&self) -> Option<(u32, f64)> {
        let token_valid = self.token_and_valid.load(Ordering::Acquire);
        if token_valid & 1 == 0 {
            return None;
        }

        let token = (token_valid >> 32) as u32;
        let ltp_paise = self.ltp_paise.load(Ordering::Acquire);
        let ltp = (ltp_paise as f64) / 100.0;

        Some((token, ltp))
    }
}

impl TickStore {
    pub fn new_with_hot_tokens(hot_tokens: &[u32]) -> Self {
        let mut hot_path = Vec::with_capacity(hot_tokens.len());
        let mut hot_token_map = HashMap::with_capacity(hot_tokens.len());

        for (idx, &token) in hot_tokens.iter().enumerate() {
            hot_path.push(CachePadded::new(AtomicTick::new()));
            hot_token_map.insert(token, idx);
        }

        tracing::info!(
            "TickStore: Pre-allocated {} hot path slots (lock-free, cache-aligned)",
            hot_tokens.len()
        );

        Self {
            hot_path: Arc::new(hot_path),
            hot_token_map: Arc::new(hot_token_map),
            cold_path: Arc::new(DashMap::with_capacity_and_shard_amount(1_000, 64)),
        }
    }

    pub fn new() -> Self {
        Self {
            hot_path: Arc::new(Vec::new()),
            hot_token_map: Arc::new(HashMap::new()),
            cold_path: Arc::new(DashMap::with_capacity_and_shard_amount(10_000, 64)),
        }
    }

    #[inline(always)]
    pub fn update(&self, tick: Tick) {
        if let Some(&idx) = self.hot_token_map.get(&tick.token) {
            self.hot_path[idx].update(&tick);
        } else {
            self.cold_path.insert(tick.token, tick);
        }
    }

    #[inline(always)]
    pub fn get_ltp(&self, token: u32) -> Option<f64> {
        if let Some(&idx) = self.hot_token_map.get(&token) {
            return self.hot_path[idx].read().map(|(_, ltp)| ltp);
        }

        self.cold_path.get(&token).map(|entry| entry.value().ltp)
    }

    #[inline(always)]
    pub fn get(&self, token: u32) -> Option<Tick> {
        self.cold_path.get(&token).map(|entry| entry.value().clone())
    }

    #[inline]
    pub fn update_batch(&self, ticks: &[Tick]) {
        for tick in ticks {
            self.update(tick.clone());
        }
    }

    pub fn len(&self) -> usize {
        self.hot_path.len() + self.cold_path.len()
    }

    pub fn is_empty(&self) -> bool {
        self.hot_path.is_empty() && self.cold_path.is_empty()
    }
}

impl Default for TickStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::TickMode;

    #[test]
    fn test_store_insert_and_get() {
        let store = TickStore::new();
        let tick = Tick {
            token: 408065,
            ltp: 1412.95,
            mode: TickMode::Ltp,
            ..Tick::default()
        };
        store.update(tick);

        let retrieved = store.get(408065).unwrap();
        assert!((retrieved.ltp - 1412.95).abs() < 0.01);
    }

    #[test]
    fn test_store_overwrite() {
        let store = TickStore::new();
        let tick1 = Tick {
            token: 408065,
            ltp: 1412.95,
            mode: TickMode::Ltp,
            ..Tick::default()
        };
        store.update(tick1);

        let tick2 = Tick {
            token: 408065,
            ltp: 1415.00,
            mode: TickMode::Ltp,
            ..Tick::default()
        };
        store.update(tick2);

        let retrieved = store.get(408065).unwrap();
        assert!((retrieved.ltp - 1415.00).abs() < 0.01);
    }

    #[test]
    fn test_store_missing() {
        let store = TickStore::new();
        assert!(store.get(999999).is_none());
    }

    #[test]
    fn test_batch_update() {
        let store = TickStore::new();
        let ticks = vec![
            Tick {
                token: 1,
                ltp: 100.0,
                ..Tick::default()
            },
            Tick {
                token: 2,
                ltp: 200.0,
                ..Tick::default()
            },
        ];
        store.update_batch(&ticks);
        assert_eq!(store.len(), 2);
        assert!((store.get(1).unwrap().ltp - 100.0).abs() < 0.01);
        assert!((store.get(2).unwrap().ltp - 200.0).abs() < 0.01);
    }
}
