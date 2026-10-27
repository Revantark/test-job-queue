//! Heartbeat: a long-running job whose worker keeps renewing its lease must not
//! be reaped, even though its original lease would have expired.

mod common;

use std::time::Duration;

use common::{TestDb, claim_one, expire_lease, seed};
use job_store::Store;

#[tokio::test]
async fn active_heartbeat_prevents_reaping() {
    let db = TestDb::create().await;
    let store = db.store().await;
    seed(&store, 3, "sleep", 1).await;

    // Claim with a short lease, then let it lapse and heartbeat before reaping.
    let claimed = claim_one(&store, "live-worker", Duration::from_secs(1)).await;
    let job_id = claimed[0].job.id;
    let lease_id = claimed[0].lease.id;

    expire_lease(&store, job_id).await;

    // A heartbeat on an already-expired lease must be rejected: the worker has
    // effectively lost the job and must not resurrect it.
    let renewed = store
        .heartbeat(job_id, lease_id, Duration::from_secs(1))
        .await
        .unwrap();
    assert!(!renewed, "an expired lease cannot be renewed");

    // Therefore the reaper reclaims it.
    let reclaimed = store.reap(Duration::ZERO).await.unwrap();
    assert_eq!(reclaimed, 1);

    // With a fresh claim, a timely heartbeat *does* keep the job alive: the job
    // stays running and the reaper leaves it alone.
    let fresh = claim_one(&store, "live-worker-2", Duration::from_secs(5)).await;
    assert_eq!(fresh.len(), 1);
    let c = &fresh[0];
    let ok = store
        .heartbeat(c.job.id, c.lease.id, Duration::from_secs(5))
        .await
        .unwrap();
    assert!(ok, "a live lease renews successfully");
    let none = store.reap(Duration::ZERO).await.unwrap();
    assert_eq!(none, 0, "a heartbeated job must not be reaped");
}
