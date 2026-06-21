//! Unit tests for the C5 Freshness Checker over in-memory fakes for the two injected seams
//! (`BrokerClient`, `WorkQueue`). One test per Acceptance-Criteria scenario and per error-table row;
//! the wiremock-backed end-to-end coverage lives in `tests/features/freshness.feature`.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;

use super::*;
use crate::types::domain::{Fact, MemoryClass, Source, Visibility};
use crate::types::job::{JobKind, WorkJob};
use crate::types::ports::{BrokerClient, ProviderError, QueueError, SourceState, WorkQueue};
use crate::types::scope::{OpSet, ScopeContext, ScopeRef};

/// What the fake broker should answer for every `check_source` call.
#[derive(Clone, Copy)]
enum BrokerMode {
    Unchanged,
    Changed,
    Unreachable,
    Error,
    /// Sleep this long before answering Unchanged — used to trip per-call / batch timeouts.
    SlowMs(u64),
}

struct FakeBroker {
    mode: BrokerMode,
    calls: AtomicUsize,
}

impl FakeBroker {
    fn new(mode: BrokerMode) -> Self {
        Self {
            mode,
            calls: AtomicUsize::new(0),
        }
    }
}

#[async_trait]
impl BrokerClient for FakeBroker {
    async fn check_source(
        &self,
        _ctx: &ScopeContext,
        _src: &Source,
    ) -> Result<SourceState, ProviderError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        match self.mode {
            BrokerMode::Unchanged => Ok(SourceState::Unchanged),
            BrokerMode::Changed => Ok(SourceState::Changed),
            BrokerMode::Unreachable => Ok(SourceState::Unreachable),
            BrokerMode::Error => Err(ProviderError::Status(503)),
            BrokerMode::SlowMs(ms) => {
                tokio::time::sleep(Duration::from_millis(ms)).await;
                Ok(SourceState::Unchanged)
            }
        }
    }
}

struct FakeQueue {
    enqueued: Mutex<Vec<WorkJob>>,
    fail: bool,
}

impl FakeQueue {
    fn new(fail: bool) -> Self {
        Self {
            enqueued: Mutex::new(Vec::new()),
            fail,
        }
    }
}

#[async_trait]
impl WorkQueue for FakeQueue {
    async fn enqueue(&self, job: WorkJob) -> Result<String, QueueError> {
        if self.fail {
            return Err(QueueError::BackendUnavailable("test".into()));
        }
        let id = job.id.clone();
        self.enqueued.lock().expect("lock").push(job);
        Ok(id)
    }
    async fn claim(
        &self,
        _kinds: &[JobKind],
        _lease: Duration,
    ) -> Result<Option<WorkJob>, QueueError> {
        unreachable!("freshness never claims")
    }
    async fn complete(&self, _job_id: &str) -> Result<(), QueueError> {
        unreachable!("freshness never completes")
    }
    async fn fail(&self, _job_id: &str, _retryable: bool) -> Result<(), QueueError> {
        unreachable!("freshness never fails jobs")
    }
}

fn ctx() -> ScopeContext {
    ScopeContext {
        tenant: "acme".into(),
        teams: vec!["platform".into()],
        user: "u-42".into(),
        token_jti: "jti-9".into(),
        allowed_ops: OpSet {
            read: true,
            write: false,
            forget: false,
        },
        correlation_id: "c-7".into(),
    }
}

fn source(id: &str) -> Source {
    Source {
        id: id.into(),
        origin_ref: format!("origin-of-{id}"),
        modification_marker: Some("W/\"abc\"".into()),
        trust_signal: 0.9,
        owner: ScopeRef {
            tenant: "acme".into(),
            team: None,
            user: "u-42".into(),
        },
    }
}

fn fact(id: &str, source_id: &str) -> Fact {
    Fact {
        id: id.into(),
        content: serde_json::json!({"text": "x"}),
        entities: vec![],
        source_id: Some(source_id.into()),
        memory_class: MemoryClass::Semantic,
        visibility: Visibility::UserPrivate,
        owner: ScopeRef {
            tenant: "acme".into(),
            team: None,
            user: "u-42".into(),
        },
        valid_from: Utc::now(),
        valid_to: None,
        ingested_at: Utc::now(),
        confidence: 0.9,
        salience: 0.7,
        stability: 1.0,
        pii_review: false,
        supersedes: None,
        superseded_by: None,
        derived_from: vec![],
        last_recalled_at: None,
    }
}

fn checker(broker: Arc<FakeBroker>, queue: Arc<FakeQueue>) -> BrokerFreshnessChecker {
    BrokerFreshnessChecker::new(
        broker,
        queue,
        Duration::from_millis(25),
        Duration::from_millis(20),
    )
}

#[tokio::test]
async fn empty_input_returns_empty_and_skips_broker() {
    let broker = Arc::new(FakeBroker::new(BrokerMode::Unchanged));
    let queue = Arc::new(FakeQueue::new(false));
    let c = checker(broker.clone(), queue.clone());
    let out = c.check(&ctx(), &[]).await;
    assert!(out.is_empty());
    assert_eq!(broker.calls.load(Ordering::SeqCst), 0, "no broker call on empty");
}

#[tokio::test]
async fn unchanged_source_marks_current_no_enqueue() {
    let broker = Arc::new(FakeBroker::new(BrokerMode::Unchanged));
    let queue = Arc::new(FakeQueue::new(false));
    let c = checker(broker.clone(), queue.clone());
    let out = c.check(&ctx(), &[(fact("fact:1", "source:A"), source("source:A"))]).await;
    assert_eq!(out, vec![("fact:1".to_string(), Currency::Current)]);
    assert!(queue.enqueued.lock().unwrap().is_empty(), "no re-read job");
}

#[tokio::test]
async fn changed_source_enqueues_one_idempotent_job_for_two_facts() {
    let broker = Arc::new(FakeBroker::new(BrokerMode::Changed));
    let queue = Arc::new(FakeQueue::new(false));
    let c = checker(broker.clone(), queue.clone());
    let facts = vec![
        (fact("fact:1", "source:A"), source("source:A")),
        (fact("fact:2", "source:A"), source("source:A")),
    ];
    let out = c.check(&ctx(), &facts).await;
    assert_eq!(out[0], ("fact:1".to_string(), Currency::StalePendingRefresh));
    assert_eq!(out[1], ("fact:2".to_string(), Currency::StalePendingRefresh));
    // One distinct source -> one broker round-trip and one enqueued job.
    assert_eq!(broker.calls.load(Ordering::SeqCst), 1, "one broker call for the shared source");
    let jobs = queue.enqueued.lock().unwrap();
    assert_eq!(jobs.len(), 1, "exactly one re-read job");
    assert_eq!(jobs[0].kind, JobKind::ReReadSource);
    assert_eq!(
        jobs[0].idempotency_key.as_deref(),
        Some("re-read-source:source:A")
    );
    assert_eq!(
        jobs[0].payload.get("source_id").and_then(|v| v.as_str()),
        Some("source:A")
    );
    assert_eq!(jobs[0].scope.tenant, "acme");
    assert_eq!(jobs[0].scope.team.as_deref(), Some("platform"));
}

#[tokio::test]
async fn broker_unreachable_returns_unverified_without_error() {
    let broker = Arc::new(FakeBroker::new(BrokerMode::Unreachable));
    let queue = Arc::new(FakeQueue::new(false));
    let c = checker(broker, queue.clone());
    let out = c.check(&ctx(), &[(fact("fact:4", "source:C"), source("source:C"))]).await;
    assert_eq!(out, vec![("fact:4".to_string(), Currency::UnverifiedCurrency)]);
    assert!(queue.enqueued.lock().unwrap().is_empty());
}

#[tokio::test]
async fn provider_error_returns_unverified() {
    let broker = Arc::new(FakeBroker::new(BrokerMode::Error));
    let queue = Arc::new(FakeQueue::new(false));
    let c = checker(broker, queue);
    let out = c.check(&ctx(), &[(fact("fact:e", "source:E"), source("source:E"))]).await;
    assert_eq!(out, vec![("fact:e".to_string(), Currency::UnverifiedCurrency)]);
}

#[tokio::test]
async fn per_call_timeout_returns_unverified() {
    // Broker sleeps 200 ms; per-call timeout is 20 ms -> the single call times out -> Unverified.
    let broker = Arc::new(FakeBroker::new(BrokerMode::SlowMs(200)));
    let queue = Arc::new(FakeQueue::new(false));
    let c = checker(broker, queue);
    let out = c.check(&ctx(), &[(fact("fact:s", "source:S"), source("source:S"))]).await;
    assert_eq!(out, vec![("fact:s".to_string(), Currency::UnverifiedCurrency)]);
}

#[tokio::test]
async fn batch_deadline_breach_degrades_all_to_unverified_within_budget() {
    // Three distinct slow sources; budget == per_call == 25 ms, each call sleeps 200 ms. The batch
    // returns at ~budget with every fact Unverified, well under the slow-call duration.
    let broker = Arc::new(FakeBroker::new(BrokerMode::SlowMs(200)));
    let queue = Arc::new(FakeQueue::new(false));
    let c = BrokerFreshnessChecker::new(
        broker,
        queue,
        Duration::from_millis(25),
        Duration::from_millis(25),
    );
    let facts = vec![
        (fact("fact:5", "source:D"), source("source:D")),
        (fact("fact:6", "source:E"), source("source:E")),
        (fact("fact:7", "source:F"), source("source:F")),
    ];
    let start = tokio::time::Instant::now();
    let out = c.check(&ctx(), &facts).await;
    let elapsed = start.elapsed();
    assert!(out.iter().all(|(_, cur)| *cur == Currency::UnverifiedCurrency));
    assert!(elapsed < Duration::from_millis(150), "batch must not overrun: {elapsed:?}");
}

#[tokio::test]
async fn queue_failure_on_change_still_flags_stale_pending_refresh() {
    let broker = Arc::new(FakeBroker::new(BrokerMode::Changed));
    let queue = Arc::new(FakeQueue::new(true)); // enqueue always fails
    let c = checker(broker, queue.clone());
    let out = c.check(&ctx(), &[(fact("fact:5", "source:G"), source("source:G"))]).await;
    assert_eq!(out, vec![("fact:5".to_string(), Currency::StalePendingRefresh)]);
    assert!(queue.enqueued.lock().unwrap().is_empty(), "failed enqueue records nothing");
}
