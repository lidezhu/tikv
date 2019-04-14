// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use test_coprocessor::*;

#[test]
fn test_outdated() {
    let _guard = crate::setup();

    let product = ProductTable::new();
    let (_, endpoint) = init_with_data(&product, &[]);
    let req = DAGSelect::from(&product).build();

    fail::cfg("coprocessor_deadline_check_exceeded", "return()").unwrap();
    let resp = handle_request(&endpoint, req);

    assert!(resp.get_other_error().contains("request outdated"));
}

#[test]
fn test_outdated_2() {
    // It should not even take any snapshots when request is outdated from the beginning.

    let _guard = crate::setup();

    let product = ProductTable::new();
    let (_, endpoint) = init_with_data(&product, &[]);
    let req = DAGSelect::from(&product).build();

    fail::cfg("rockskv_async_snapshot", "panic").unwrap();
    fail::cfg("coprocessor_deadline_check_exceeded", "return()").unwrap();
    let resp = handle_request(&endpoint, req);

    assert!(resp.get_other_error().contains("request outdated"));
}

#[test]
fn test_parse_request_failed() {
    let _guard = crate::setup();

    let product = ProductTable::new();
    let (_, endpoint) = init_with_data(&product, &[]);
    let req = DAGSelect::from(&product).build();

    fail::cfg("coprocessor_parse_request", "return()").unwrap();
    let resp = handle_request(&endpoint, req);

    assert!(resp.get_other_error().contains("unsupported tp"));
}

#[test]
fn test_parse_request_failed_2() {
    // It should not even take any snapshots when parse failed.

    let _guard = crate::setup();

    let product = ProductTable::new();
    let (_, endpoint) = init_with_data(&product, &[]);
    let req = DAGSelect::from(&product).build();

    fail::cfg("rockskv_async_snapshot", "panic").unwrap();
    fail::cfg("coprocessor_parse_request", "return()").unwrap();
    let resp = handle_request(&endpoint, req);

    assert!(resp.get_other_error().contains("unsupported tp"));
}

#[test]
fn test_readpool_full() {
    let _guard = crate::setup();

    let product = ProductTable::new();
    let (_, endpoint) = init_with_data(&product, &[]);
    let req = DAGSelect::from(&product).build();

    fail::cfg("read_pool_execute_full", "return()").unwrap();
    let resp = handle_request(&endpoint, req);

    assert!(resp.get_region_error().has_server_is_busy());
}

#[test]
fn test_snapshot_failed() {
    let _guard = crate::setup();

    let product = ProductTable::new();
    let (_, endpoint) = init_with_data(&product, &[]);
    let req = DAGSelect::from(&product).build();

    fail::cfg("rockskv_async_snapshot", "return()").unwrap();
    let resp = handle_request(&endpoint, req);

    assert!(resp.get_other_error().contains("snapshot failed"));
}

#[test]
fn test_snapshot_failed_2() {
    let _guard = crate::setup();

    let product = ProductTable::new();
    let (_, endpoint) = init_with_data(&product, &[]);
    let req = DAGSelect::from(&product).build();

    fail::cfg("rockskv_async_snapshot_not_leader", "return()").unwrap();
    let resp = handle_request(&endpoint, req);

    assert!(resp.get_region_error().has_not_leader());
}
