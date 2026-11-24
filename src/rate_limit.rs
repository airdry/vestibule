//! Local in-memory Token Bucket rate limiting (per ADR-0001).
//!
//! Enforces `Rate Limit Rule` constraints across `project_id`, `environment`,
//! and `Item Category`, computing accurate `retry_after` values for
//! `X-Sentry-Rate-Limits` responses.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::codec::ItemCategory;
use crate::config::{RateLimitSettings, Rule, ScopeRules};
use crate::wal::Clock;

/// The verdict for one item against its `Rate Limit Rule`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decision {
    Allowed,
    /// The item exceeds its `Rate Limit Rule`; `retry_after_secs` is the
    /// ceiling-integer seconds (minimum 1) until one token refills.
    Throttled {
        retry_after_secs: u64,
    },
}

/// One item to check against the `Rate Limit Rule` resolved for it.
pub struct CheckItem<'a> {
    pub project_id: Option<u64>,
    pub environment: &'a str,
    pub category: ItemCategory,
    /// `None` means the item is unlimited.
    pub rule: Option<Rule>,
}

/// Local in-memory Token Bucket engine over a bounded LRU/TTL table.
pub struct RateLimiter {
    table: Mutex<HashMap<BucketKey, Bucket>>,
    clock: Arc<dyn Clock>,
    max_buckets: usize,
    bucket_ttl_ms: u64,
}

#[derive(Hash, PartialEq, Eq, Clone)]
struct BucketKey {
    project_id: Option<u64>,
    environment: String,
    category: ItemCategory,
}

struct Bucket {
    tokens: f64,
    last_refill_ms: u64,
    last_used_ms: u64,
}

impl RateLimiter {
    pub fn new(clock: Arc<dyn Clock>, max_buckets: usize, bucket_ttl: Duration) -> Self {
        Self {
            table: Mutex::new(HashMap::new()),
            clock,
            max_buckets,
            bucket_ttl_ms: bucket_ttl.as_millis() as u64,
        }
    }

    /// Consumes one token for the item when its rule allows it.
    pub fn check(&self, item: CheckItem<'_>) -> Decision {
        let Some(rule) = item.rule else {
            return Decision::Allowed;
        };
        let now = self.clock.now_unix_ms();
        let key = BucketKey {
            project_id: item.project_id,
            environment: item.environment.to_string(),
            category: item.category,
        };

        let mut table = lock_table(&self.table);
        let bucket = match table.get_mut(&key) {
            Some(bucket) if now.saturating_sub(bucket.last_used_ms) <= self.bucket_ttl_ms => bucket,
            _ => {
                if table.len() >= self.max_buckets {
                    evict_one(&mut table, now, self.bucket_ttl_ms);
                }
                table.entry(key).or_insert(Bucket {
                    tokens: rule.burst as f64,
                    last_refill_ms: now,
                    last_used_ms: now,
                })
            }
        };

        refill(bucket, rule, now);
        bucket.last_used_ms = now;
        if bucket.tokens >= 1.0 {
            bucket.tokens -= 1.0;
            Decision::Allowed
        } else {
            Decision::Throttled {
                retry_after_secs: retry_after(bucket.tokens, rule),
            }
        }
    }
}

/// Refills the bucket up to `burst` and computes the exact retry_after.
fn refill(bucket: &mut Bucket, rule: Rule, now_ms: u64) {
    let elapsed_ms = now_ms.saturating_sub(bucket.last_refill_ms);
    let tokens = bucket.tokens + elapsed_ms as f64 * rule.rate / 1000.0;
    bucket.tokens = tokens.min(rule.burst as f64);
    bucket.last_refill_ms = now_ms;
}

fn retry_after(tokens: f64, rule: Rule) -> u64 {
    let deficit = 1.0 - tokens;
    (deficit / rule.rate).ceil().max(1.0) as u64
}

/// Evicts expired buckets first, otherwise the least recently used one.
fn evict_one(table: &mut HashMap<BucketKey, Bucket>, now: u64, ttl_ms: u64) {
    let victim = table
        .iter()
        .min_by_key(|(_, bucket)| {
            let idle_ms = now.saturating_sub(bucket.last_used_ms);
            (if idle_ms > ttl_ms { 0 } else { 1 }, bucket.last_used_ms)
        })
        .map(|(key, _)| (*key).clone());
    let Some(victim) = victim else { return };
    table.remove(&victim);
}

/// Locks the bucket table, tolerating poisoning like the WAL lock.
fn lock_table(
    table: &Mutex<HashMap<BucketKey, Bucket>>,
) -> std::sync::MutexGuard<'_, HashMap<BucketKey, Bucket>> {
    table
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Resolves the `Rate Limit Rule` for one item, from most specific to most
/// general scope. `None` means the item is unlimited.
///
/// Hierarchy: `project_id + environment + category` →
/// `project_id + * + category` → `defaults + environment + category` →
/// `defaults + * + category`.
pub fn resolve_rule(
    settings: &RateLimitSettings,
    project_id: Option<u64>,
    environment: &str,
    category: ItemCategory,
) -> Option<Rule> {
    let project_scope = project_id.and_then(|id| settings.projects.get(&id));
    [
        project_scope.and_then(|scope| environment_rule(scope, environment, category)),
        project_scope.and_then(|scope| category_rule(scope, category)),
        environment_rule(&settings.defaults, environment, category),
        category_rule(&settings.defaults, category),
    ]
    .into_iter()
    .flatten()
    .next()
}

fn environment_rule(scope: &ScopeRules, environment: &str, category: ItemCategory) -> Option<Rule> {
    scope
        .environments
        .get(environment)
        .and_then(|env_scope| category_rule(env_scope, category))
}

fn category_rule(scope: &ScopeRules, category: ItemCategory) -> Option<Rule> {
    match category {
        ItemCategory::Event => scope.event,
        ItemCategory::Transaction => scope.transaction,
        ItemCategory::Other => scope.other,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;

    use super::{CheckItem, Decision, RateLimiter, resolve_rule};
    use crate::codec::ItemCategory;
    use crate::config::{RateLimitSettings, Rule, ScopeRules};
    use crate::wal::Clock;

    fn rule(rate: f64, burst: u32) -> Option<Rule> {
        Some(Rule { rate, burst })
    }

    const RATE_1_RPS: Option<Rule> = Some(Rule {
        rate: 1.0,
        burst: 2,
    });

    #[test]
    fn allows_within_burst_then_throttles_with_retry_after() {
        let clock = MockClock::new(1_000);
        let limiter = limiter_with(&clock, 10);

        assert_eq!(
            limiter.check(item(RATE_1_RPS, Some(42), "prod")),
            Decision::Allowed
        );
        assert_eq!(
            limiter.check(item(RATE_1_RPS, Some(42), "prod")),
            Decision::Allowed
        );
        // Bucket exhausted: needs ~1s for one token at 1 RPS.
        assert_eq!(
            limiter.check(item(RATE_1_RPS, Some(42), "prod")),
            Decision::Throttled {
                retry_after_secs: 1
            }
        );
    }

    #[test]
    fn refills_over_time_according_to_rate() {
        let clock = MockClock::new(1_000);
        let limiter = limiter_with(&clock, 10);
        for _ in 0..2 {
            limiter.check(item(RATE_1_RPS, Some(42), "prod"));
        }

        clock.advance_ms(1000);
        // One token refilled at 1 RPS.
        assert_eq!(
            limiter.check(item(RATE_1_RPS, Some(42), "prod")),
            Decision::Allowed
        );
        // The refill accumulated only one token; the next request is throttled.
        assert_eq!(
            limiter.check(item(RATE_1_RPS, Some(42), "prod")),
            Decision::Throttled {
                retry_after_secs: 1
            }
        );
    }

    #[test]
    fn retry_after_covers_fractional_deficits() {
        let clock = MockClock::new(1_000);
        let limiter = limiter_with(&clock, 10);
        let rule = Some(Rule {
            rate: 0.25,
            burst: 1,
        });

        limiter.check(item(rule, Some(42), "prod"));
        clock.advance_ms(1000);
        // tokens = 0.25 after 1s; deficit 0.75 at 0.25 RPS → 3s.
        assert_eq!(
            limiter.check(item(rule, Some(42), "prod")),
            Decision::Throttled {
                retry_after_secs: 3
            }
        );
    }

    #[test]
    fn lru_eviction_bounds_the_bucket_table() {
        let clock = MockClock::new(1_000);
        let limiter = limiter_with(&clock, 2);

        // Exhaust the bucket for project 1.
        for _ in 0..2 {
            limiter.check(item(RATE_1_RPS, Some(1), "prod"));
        }
        assert_eq!(
            limiter.check(item(RATE_1_RPS, Some(1), "prod")),
            Decision::Throttled {
                retry_after_secs: 1
            }
        );

        // Touch two more buckets; the project-1 bucket becomes the LRU
        // entry and is evicted once the table reaches capacity. Distinct
        // `last_used_ms` values make the LRU order deterministic.
        limiter.check(item(RATE_1_RPS, Some(2), "prod"));
        clock.advance_ms(1);
        limiter.check(item(RATE_1_RPS, Some(3), "prod"));

        // The project-1 bucket was evicted; a fresh full bucket allows again.
        assert_eq!(
            limiter.check(item(RATE_1_RPS, Some(1), "prod")),
            Decision::Allowed
        );
    }

    #[test]
    fn idle_buckets_expire_by_ttl() {
        let clock = MockClock::new(1_000);
        let limiter = limiter_with(&clock, 10);

        for _ in 0..2 {
            limiter.check(item(RATE_1_RPS, Some(42), "prod"));
        }
        // Advance past the TTL: the bucket resets to a full state.
        clock.advance_ms(61_000);
        assert_eq!(
            limiter.check(item(RATE_1_RPS, Some(42), "prod")),
            Decision::Allowed
        );
    }

    #[test]
    fn unlimited_items_consume_no_tokens() {
        let clock = MockClock::new(1_000);
        let limiter = limiter_with(&clock, 10);

        for _ in 0..5 {
            assert_eq!(
                limiter.check(unlimited(Some(42), "prod")),
                Decision::Allowed
            );
        }
    }

    struct MockClock(std::sync::Mutex<u64>);

    impl MockClock {
        fn new(now_ms: u64) -> std::sync::Arc<Self> {
            std::sync::Arc::new(Self(std::sync::Mutex::new(now_ms)))
        }

        fn advance_ms(&self, millis: u64) {
            *self.0.lock().unwrap() += millis;
        }
    }

    impl Clock for MockClock {
        fn now_unix_ms(&self) -> u64 {
            *self.0.lock().unwrap()
        }
    }

    fn unlimited(project: Option<u64>, environment: &str) -> CheckItem<'_> {
        CheckItem {
            project_id: project,
            environment,
            category: ItemCategory::Event,
            rule: None,
        }
    }

    fn item(rule: Option<Rule>, project: Option<u64>, environment: &str) -> CheckItem<'_> {
        CheckItem {
            project_id: project,
            environment,
            category: ItemCategory::Event,
            rule,
        }
    }

    fn limiter_with(clock: &Arc<MockClock>, max_buckets: usize) -> RateLimiter {
        let clock: Arc<dyn Clock> = clock.clone();
        RateLimiter::new(clock, max_buckets, Duration::from_secs(60))
    }

    #[test]
    fn resolves_rules_hierarchically() {
        let settings = RateLimitSettings {
            max_buckets: 10,
            bucket_ttl: Duration::from_secs(60),
            defaults: ScopeRules {
                event: rule(1.0, 1),
                transaction: None,
                other: None,
                environments: [(
                    "production".to_string(),
                    ScopeRules {
                        event: rule(2.0, 2),
                        transaction: None,
                        other: None,
                        environments: HashMap::new(),
                    },
                )]
                .into_iter()
                .collect(),
            },
            projects: [(
                42,
                ScopeRules {
                    event: rule(3.0, 3),
                    transaction: rule(4.0, 4),
                    other: None,
                    environments: [(
                        "staging".to_string(),
                        ScopeRules {
                            event: rule(5.0, 5),
                            transaction: rule(6.0, 6),
                            other: None,
                            environments: HashMap::new(),
                        },
                    )]
                    .into_iter()
                    .collect(),
                },
            )]
            .into_iter()
            .collect(),
        };

        use ItemCategory::{Event, Other, Transaction};

        // project + environment + category
        assert_eq!(
            resolve_rule(&settings, Some(42), "staging", Event),
            rule(5.0, 5)
        );
        // project + * + category
        assert_eq!(
            resolve_rule(&settings, Some(42), "unknown-env", Transaction),
            rule(4.0, 4)
        );
        // defaults + environment + category
        assert_eq!(
            resolve_rule(&settings, None, "production", Event),
            rule(2.0, 2)
        );
        // defaults + * + category
        assert_eq!(
            resolve_rule(&settings, Some(7), "whatever", Event),
            rule(1.0, 1)
        );
        // no rule resolved → unlimited
        assert_eq!(resolve_rule(&settings, Some(42), "anywhere", Other), None);
        assert_eq!(resolve_rule(&settings, None, "anywhere", Transaction), None);
    }
}
