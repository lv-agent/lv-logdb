//! Integration tests for logdb-broker gRPC service (cr-037 Phase 2).

use std::sync::Arc;
use std::time::Duration;

use tonic::transport::Server;
use tokio_stream::wrappers::TcpListenerStream;

use logdb_broker::coordinator::CoordinatorRegistry;
use logdb_broker::service::BrokerServiceImpl;
use logdb_broker_proto::pb::broker_service_client::BrokerServiceClient;
use logdb_broker_proto::pb::broker_service_server::BrokerServiceServer;
use logdb_broker_proto::pb::{
    JoinGroupRequest, LeaveGroupRequest, ListMembersRequest,
};

// ── Harness ──────────────────────────────────────────────────────────────────

async fn start_broker(num_shards: u32) -> std::net::SocketAddr {
    let registry = Arc::new(CoordinatorRegistry::new(num_shards));
    // No Forwarder/Persistence (membership-only): Consume/Produce/Commit return
    // UNIMPLEMENTED or in-memory-only here.
    let svc = BrokerServiceImpl::new(registry, None, None);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        Server::builder()
            .add_service(BrokerServiceServer::new(svc))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    addr
}

fn join_req(consumer_id: &str) -> JoinGroupRequest {
    JoinGroupRequest {
        namespace: "ns".into(),
        stream: "s".into(),
        group: "g".into(),
        consumer_id: consumer_id.into(),
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn join_group_assigns_shards_round_robin() {
    let addr = start_broker(4).await;
    let mut client = BrokerServiceClient::connect(format!("http://{addr}"))
        .await
        .unwrap();

    // First consumer: sole member ⇒ owns all 4 shards (snapshot at join time).
    let r1 = client
        .join_group(join_req("c1"))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(r1.num_shards, 4);
    assert_eq!(r1.generation, 1);
    let mut a1 = r1.assigned_shards.clone();
    a1.sort();
    assert_eq!(a1, vec![0, 1, 2, 3]);

    // Second consumer: generation bumped; c2's snapshot reflects the split.
    let r2 = client
        .join_group(join_req("c2"))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(r2.generation, 2);
    assert!(!r2.assigned_shards.is_empty());

    // The disjoint-partition property must hold over CURRENT assignments (after
    // the rebalance c2 caused), not the stale join-time snapshots: c1's
    // JoinGroup response predates c2's join. ListMembers reflects post-rebalance
    // state for both. (Phase 5's rebalance protocol is what notifies c1 of its
    // new, smaller assignment.)
    let members = client
        .list_members(ListMembersRequest {
            namespace: "ns".into(),
            stream: "s".into(),
            group: "g".into(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(members.generation, 2);
    assert_eq!(members.members.len(), 2);

    let mut union: std::collections::BTreeSet<u32> = std::collections::BTreeSet::new();
    for m in &members.members {
        for shard in &m.assigned_shards {
            assert!(
                union.insert(*shard),
                "shard {shard} assigned to two members post-rebalance"
            );
        }
    }
    assert_eq!(union.len(), 4, "all 4 shards covered by the 2 members");
}

#[tokio::test]
async fn leave_group_rebalances_remaining() {
    let addr = start_broker(4).await;
    let mut client = BrokerServiceClient::connect(format!("http://{addr}"))
        .await
        .unwrap();
    client.join_group(join_req("c1")).await.unwrap();
    client.join_group(join_req("c2")).await.unwrap();

    let leave = client
        .leave_group(LeaveGroupRequest {
            namespace: "ns".into(),
            stream: "s".into(),
            group: "g".into(),
            consumer_id: "c1".into(),
            generation: 2,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(leave.ok);

    let members = client
        .list_members(ListMembersRequest {
            namespace: "ns".into(),
            stream: "s".into(),
            group: "g".into(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(members.members.len(), 1);
    assert_eq!(members.generation, 3); // join c1→1, join c2→2, leave→3
    assert_eq!(members.members[0].consumer_id, "c2");
    let mut shards = members.members[0].assigned_shards.clone();
    shards.sort();
    assert_eq!(shards, vec![0, 1, 2, 3]); // remaining member owns all
}

#[tokio::test]
async fn join_group_rejects_empty_consumer_id() {
    let addr = start_broker(4).await;
    let mut client = BrokerServiceClient::connect(format!("http://{addr}"))
        .await
        .unwrap();
    let err = client
        .join_group(JoinGroupRequest {
            namespace: "ns".into(),
            stream: "s".into(),
            group: "g".into(),
            consumer_id: "".into(),
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn list_members_unknown_group_is_not_found() {
    let addr = start_broker(4).await;
    let mut client = BrokerServiceClient::connect(format!("http://{addr}"))
        .await
        .unwrap();
    let err = client
        .list_members(ListMembersRequest {
            namespace: "ns".into(),
            stream: "s".into(),
            group: "nope".into(),
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::NotFound);
}
