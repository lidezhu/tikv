use std::sync::atomic::AtomicBool;
use std::sync::mpsc;
use std::sync::Arc;
use std::time::Duration;

use super::cluster::{Cluster, Simulator};
use super::server::new_server_cluster;
use super::transport_simulate::*;
use super::util::*;
use raft::eraftpb::MessageType;

// Validate that prevote is used in elections after a leader is isolated if it is on, or not if it is off.
fn test_prevote_on_leader_isolation<T: Simulator>(cluster: &mut Cluster<T>, prevote_enabled: bool) {
    cluster.cfg.raft_store.prevote = prevote_enabled;

    // Setup a notifier
    let (tx, rx) = mpsc::channel();
    let response_notifier = Box::new(MessageTypeNotifier::new(
        MessageType::MsgRequestPreVoteResponse,
        tx.clone(),
        Arc::from(AtomicBool::new(true)),
    ));
    let request_notifier = Box::new(MessageTypeNotifier::new(
        MessageType::MsgRequestPreVote,
        tx.clone(),
        Arc::from(AtomicBool::new(true)),
    ));

    // We must start the cluster before adding send filters, otherwise it panics.
    cluster.run();
    cluster
        .sim
        .write()
        .unwrap()
        .add_send_filter(2, response_notifier);
    cluster
        .sim
        .write()
        .unwrap()
        .add_send_filter(2, request_notifier);

    // Force an unplanned election.
    cluster.must_transfer_leader(1, new_peer(1, 1));
    cluster.add_send_filter(IsolationFilterFactory::new(1));

    // Once we see a response on the wire we know a prevote round is happening.
    let recieved = rx.recv_timeout(Duration::from_secs(2));
    assert_eq!(
        recieved.is_ok(),
        prevote_enabled,
        "Recieve a PreVote or PreVoteResponse to a peer when a leader was isolated.",
    );
}

#[test]
fn test_server_prevote_on_leader_isolation() {
    let mut cluster = new_server_cluster(0, 3);
    test_prevote_on_leader_isolation(&mut cluster, true);
}

#[test]
fn test_server_no_prevote_on_leader_isolation() {
    let mut cluster = new_server_cluster(0, 3);
    test_prevote_on_leader_isolation(&mut cluster, false);
}

fn test_prevote_on_leader_reboot<T: Simulator>(cluster: &mut Cluster<T>, prevote_enabled: bool) {
    cluster.cfg.raft_store.prevote = prevote_enabled;
    cluster.run();

    // Setup a notifier
    let (tx, rx) = mpsc::channel();
    let response_notifier = Box::new(MessageTypeNotifier::new(
        MessageType::MsgRequestPreVoteResponse,
        tx.clone(),
        Arc::from(AtomicBool::new(true)),
    ));
    let request_notifier = Box::new(MessageTypeNotifier::new(
        MessageType::MsgRequestPreVote,
        tx.clone(),
        Arc::from(AtomicBool::new(true)),
    ));

    // Make a node a leader, then kill it and bring it back up letting it ask for a prevote.
    cluster.must_transfer_leader(1, new_peer(1, 1));
    cluster.stop_node(1);
    cluster.run_node(1);

    // The remaining nodes should hold a new election.
    cluster
        .sim
        .write()
        .unwrap()
        .add_send_filter(3, response_notifier);
    cluster
        .sim
        .write()
        .unwrap()
        .add_send_filter(3, request_notifier);

    // Once we see a response on the wire we know a prevote round is happening.
    let recieved = rx.recv_timeout(Duration::from_secs(2));
    assert_eq!(
        recieved.is_ok(),
        prevote_enabled,
        "Recieved a PreVote or PreVoteResponse when a leader is elected then unexpectedly killed."
    );
}

#[test]
fn test_server_prevote_on_leader_reboot() {
    let mut cluster = new_server_cluster(0, 3);
    test_prevote_on_leader_reboot(&mut cluster, true);
}

#[test]
fn test_server_no_prevote_on_leader_reboot() {
    let mut cluster = new_server_cluster(0, 3);
    test_prevote_on_leader_reboot(&mut cluster, false);
}

// Test isolating a minority of the cluster and make sure that the remove themselves.
fn test_pair_isolated<T: Simulator>(cluster: &mut Cluster<T>) {
    let region = 1;
    let pd_client = Arc::clone(&cluster.pd_client);

    // Given some nodes A, B, C, D, E, we partition the cluster such that D, E are isolated from the rest.
    cluster.run();
    // Choose a predictable leader so we don't accidently partition the leader.
    cluster.must_transfer_leader(region, new_peer(1, 1));
    cluster.partition(vec![1, 2, 3], vec![4, 5]);

    // Then, add a policy to PD that it should ask the Raft leader to remove the peer from the group.
    pd_client.must_remove_peer(region, new_peer(4, 4));
    pd_client.must_remove_peer(region, new_peer(5, 5));

    // Verify the nodes have self removed.
    cluster.must_remove_region(4, region);
    cluster.must_remove_region(5, region);
}

#[test]
fn test_server_pair_isolated() {
    let mut cluster = new_server_cluster(0, 5);
    test_pair_isolated(&mut cluster);
}
