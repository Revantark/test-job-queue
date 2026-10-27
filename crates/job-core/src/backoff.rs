use std::time::Duration;

/// Exponential retry backoff with deterministic jitter.
///
/// The exponential term is `base * 2^(attempt-1)` capped at `cap`. Jitter is up
/// to 20% of that, derived from `seed` (the job id) rather than RNG so retries
/// stay reproducible in tests while still spreading *different* jobs apart in
/// time to avoid a thundering herd on a shared dependency.
///
/// `attempt` is 1 for the first failure (it is incremented when a job is
/// claimed).
pub fn compute_backoff(attempt: u32, base: Duration, cap: Duration, seed: u64) -> Duration {
    let exponential = exponential_part(attempt, base, cap);
    exponential + jitter(exponential, seed)
}

/// The capped exponential component, exposed separately so it can be asserted
/// in isolation.
pub fn exponential_part(attempt: u32, base: Duration, cap: Duration) -> Duration {
    // Saturating shift keeps pathological attempt counts from overflowing.
    let shift = attempt.saturating_sub(1).min(31);
    let factor = 1u64 << shift;
    let millis = base.as_millis().saturating_mul(factor as u128);
    let capped = millis.min(cap.as_millis());
    Duration::from_millis(capped as u64)
}

/// Jitter of up to 20% of the exponential delay, deterministic in `seed`.
fn jitter(delay: Duration, seed: u64) -> Duration {
    if delay.is_zero() {
        return Duration::ZERO;
    }
    // Cheap integer hash (splitmix64) to turn the seed into a stable fraction.
    let mut x = seed.wrapping_add(0x9E37_79B9_7F4A_7C15);
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^= x >> 31;
    let frac = (x % 1000) as u128; // 0..999
    let extra = delay.as_millis() * 20 / 100 * frac / 1000;
    Duration::from_millis(extra as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn secs(n: u64) -> Duration {
        Duration::from_secs(n)
    }

    #[test]
    fn exponential_doubles_each_attempt() {
        assert_eq!(exponential_part(1, secs(1), secs(3600)), secs(1));
        assert_eq!(exponential_part(2, secs(1), secs(3600)), secs(2));
        assert_eq!(exponential_part(3, secs(1), secs(3600)), secs(4));
        assert_eq!(exponential_part(4, secs(1), secs(3600)), secs(8));
    }

    #[test]
    fn exponential_is_capped() {
        assert_eq!(exponential_part(20, secs(1), secs(60)), secs(60));
    }

    #[test]
    fn jitter_stays_within_bounds_and_is_deterministic() {
        let base = exponential_part(3, secs(1), secs(3600)); // 4s
        let a = compute_backoff(3, secs(1), secs(3600), 42);
        let b = compute_backoff(3, secs(1), secs(3600), 42);
        assert_eq!(a, b, "jitter must be deterministic in the seed");
        assert!(a >= base);
        assert!(a <= base + base.mul_f64(0.2) + Duration::from_millis(1));
    }

    #[test]
    fn different_seeds_spread() {
        let samples: Vec<_> = (0..50)
            .map(|id| compute_backoff(3, secs(1), secs(3600), id))
            .collect();
        let distinct = samples.iter().collect::<std::collections::HashSet<_>>();
        assert!(distinct.len() > 10, "seeds should produce varied jitter");
    }
}
