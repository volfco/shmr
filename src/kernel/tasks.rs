use std::time::Duration;
use rand::Rng;

// 90% AI Generated! Go being lazy.
pub(crate) fn split_duration(duration: Duration) -> (Duration, Duration) {
    let total_secs = duration.as_secs();
    let mut rng = rand::thread_rng();
    let part1_secs = rng.gen_range(0..=total_secs);
    let part2_secs = total_secs - part1_secs;
    (Duration::from_millis(part1_secs), Duration::from_millis(part2_secs))
}
