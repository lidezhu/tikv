// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use std::time::Instant;

use prometheus::*;
use rocksdb::{set_perf_level, PerfContext, PerfLevel};
use util::time::duration_to_ms;

lazy_static! {
    pub static ref PEER_PROPOSAL_COUNTER_VEC: IntCounterVec =
        register_int_counter_vec!(
            "tikv_raftstore_proposal_total",
            "Total number of proposal made.",
            &["type"]
        ).unwrap();

    pub static ref PEER_ADMIN_CMD_COUNTER_VEC: IntCounterVec =
        register_int_counter_vec!(
            "tikv_raftstore_admin_cmd_total",
            "Total number of admin cmd processed.",
            &["type", "status"]
        ).unwrap();

    pub static ref PEER_APPEND_LOG_HISTOGRAM: Histogram =
        register_histogram!(
            "tikv_raftstore_append_log_duration_seconds",
            "Bucketed histogram of peer appending log duration",
            exponential_buckets(0.0005, 2.0, 20).unwrap()
        ).unwrap();

    pub static ref STORE_APPLY_LOG_HISTOGRAM: Histogram =
        register_histogram!(
            "tikv_raftstore_apply_log_duration_seconds",
            "Bucketed histogram of peer applying log duration",
            exponential_buckets(0.0005, 2.0, 20).unwrap()
        ).unwrap();

    pub static ref STORE_APPLY_DURATION: Histogram =
        register_histogram!(
            "tikv_raftstore_apply_write_duration_seconds",
            "Bucketed histogram of peer applying log duration",
            exponential_buckets(0.0001, 2.0, 20).unwrap()
        ).unwrap();

    pub static ref APPLY_TASK_WAIT_TIME_HISTOGRAM: Histogram =
        register_histogram!(
            "tikv_raftstore_apply_wait_time_duration_secs",
            "Bucketed histogram of apply task wait time duration",
            exponential_buckets(0.0005, 2.0, 20).unwrap()
        ).unwrap();

    pub static ref APPLY_TASK_SIZE_HISTOGRAM: Histogram =
        register_histogram!(
            "tikv_raftstore_apply_task_size",
            "Bucketed histogram of apply task size",
            exponential_buckets(1.0, 2.0, 14).unwrap()
        ).unwrap();

    pub static ref PEER_COMMIT_LOG_HISTOGRAM: Histogram =
        register_histogram!(
            "tikv_raftstore_commit_log_duration_seconds",
            "Bucketed histogram of peer commits logs duration",
            exponential_buckets(0.0005, 2.0, 20).unwrap()
        ).unwrap();

    pub static ref STORE_RAFT_READY_COUNTER_VEC: IntCounterVec =
        register_int_counter_vec!(
            "tikv_raftstore_raft_ready_handled_total",
            "Total number of raft ready handled.",
            &["type"]
        ).unwrap();

    pub static ref STORE_RAFT_SENT_MESSAGE_COUNTER_VEC: IntCounterVec =
        register_int_counter_vec!(
            "tikv_raftstore_raft_sent_message_total",
            "Total number of raft ready sent messages.",
            &["type"]
        ).unwrap();

    pub static ref STORE_RAFT_DROPPED_MESSAGE_COUNTER_VEC: IntCounterVec =
        register_int_counter_vec!(
            "tikv_raftstore_raft_dropped_message_total",
            "Total number of raft dropped messages.",
            &["type"]
        ).unwrap();

    pub static ref STORE_PD_HEARTBEAT_GAUGE_VEC: IntGaugeVec =
        register_int_gauge_vec!(
            "tikv_pd_heartbeat_tick_total",
            "Total number of pd heartbeat ticks.",
            &["type"]
        ).unwrap();

    pub static ref STORE_SNAPSHOT_TRAFFIC_GAUGE_VEC: IntGaugeVec =
        register_int_gauge_vec!(
            "tikv_raftstore_snapshot_traffic_total",
            "Total number of raftstore snapshot traffic.",
            &["type"]
        ).unwrap();

    pub static ref STORE_SNAPSHOT_VALIDATION_FAILURE_COUNTER: IntCounterVec =
        register_int_counter_vec!(
            "tikv_raftstore_snapshot_validation_failure_total",
            "Total number of raftstore snapshot validation failure.",
            &["type"]
        ).unwrap();

    pub static ref PEER_RAFT_PROCESS_DURATION: HistogramVec =
        register_histogram_vec!(
            "tikv_raftstore_raft_process_duration_secs",
            "Bucketed histogram of peer processing raft duration",
            &["type"],
            exponential_buckets(0.0005, 2.0, 20).unwrap()
        ).unwrap();

    pub static ref PEER_PROPOSE_LOG_SIZE_HISTOGRAM: Histogram =
        register_histogram!(
            "tikv_raftstore_propose_log_size",
            "Bucketed histogram of peer proposing log size",
            vec![256.0, 512.0, 1024.0, 4096.0, 65536.0, 262144.0, 524288.0, 1048576.0,
                    2097152.0, 4194304.0, 8388608.0, 16777216.0]
        ).unwrap();

    pub static ref REGION_HASH_COUNTER_VEC: IntCounterVec =
        register_int_counter_vec!(
            "tikv_raftstore_hash_total",
            "Total number of hash has been computed.",
            &["type", "result"]
        ).unwrap();

    pub static ref REGION_MAX_LOG_LAG: Histogram =
        register_histogram!(
            "tikv_raftstore_log_lag",
            "Bucketed histogram of log lag in a region",
            vec![2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0, 256.0,
                    512.0, 1024.0, 5120.0, 10240.0]
        ).unwrap();

    pub static ref REQUEST_WAIT_TIME_HISTOGRAM: Histogram =
        register_histogram!(
            "tikv_raftstore_request_wait_time_duration_secs",
            "Bucketed histogram of request wait time duration",
            exponential_buckets(0.0005, 2.0, 20).unwrap()
        ).unwrap();

    pub static ref PEER_GC_RAFT_LOG_COUNTER: IntCounter =
        register_int_counter!(
            "tikv_raftstore_gc_raft_log_total",
            "Total number of GC raft log."
        ).unwrap();

    pub static ref UPDATE_REGION_SIZE_BY_COMPACTION_COUNTER: IntCounter =
        register_int_counter!(
            "update_region_size_count_by_compaction",
            "Total number of update region size caused by compaction."
        ).unwrap();

    pub static ref COMPACTION_RELATED_REGION_COUNT: HistogramVec =
        register_histogram_vec!(
            "compaction_related_region_count",
            "Associated number of regions for each compaction job",
            &["output_level"],
            exponential_buckets(1.0, 2.0, 20).unwrap()
        ).unwrap();

    pub static ref COMPACTION_DECLINED_BYTES: HistogramVec =
        register_histogram_vec!(
            "compaction_declined_bytes",
            "total bytes declined for each compaction job",
            &["output_level"],
            exponential_buckets(1024.0, 2.0, 30).unwrap()
        ).unwrap();

    pub static ref SNAPSHOT_CF_KV_COUNT: HistogramVec =
        register_histogram_vec!(
            "tikv_snapshot_cf_kv_count",
            "Total number of kv in each cf file of snapshot",
            &["type"],
            exponential_buckets(100.0, 2.0, 20).unwrap()
        ).unwrap();

    pub static ref SNAPSHOT_CF_SIZE: HistogramVec =
        register_histogram_vec!(
            "tikv_snapshot_cf_size",
            "Total size of each cf file of snapshot",
            &["type"],
            exponential_buckets(1024.0, 2.0, 31).unwrap()
        ).unwrap();

    pub static ref SNAPSHOT_BUILD_TIME_HISTOGRAM: Histogram =
        register_histogram!(
            "tikv_snapshot_build_time_duration_secs",
            "Bucketed histogram of snapshot build time duration.",
            exponential_buckets(0.05, 2.0, 20).unwrap()
        ).unwrap();

    pub static ref SNAPSHOT_KV_COUNT_HISTOGRAM: Histogram =
        register_histogram!(
            "tikv_snapshot_kv_count",
            "Total number of kv in snapshot",
             exponential_buckets(100.0, 2.0, 20).unwrap() //100,100*2^1,...100M
        ).unwrap();

    pub static ref SNAPSHOT_SIZE_HISTOGRAM: Histogram =
        register_histogram!(
            "tikv_snapshot_size",
            "Size of snapshot",
             exponential_buckets(1024.0, 2.0, 22).unwrap() // 1024,1024*2^1,..,4G
        ).unwrap();

    pub static ref RAFT_ENTRY_FETCHES: IntCounterVec =
        register_int_counter_vec!(
            "tikv_raftstore_entry_fetches",
            "Total number of raft entry fetches",
            &["type"]
        ).unwrap();

    pub static ref BATCH_SNAPSHOT_COMMANDS: Histogram =
        register_histogram!(
            "tikv_raftstore_batch_snapshot_commands_total",
            "Bucketed histogram of total size of batch snapshot commands",
            vec![1.0, 2.0, 4.0, 6.0, 8.0, 10.0, 12.0, 14.0, 16.0, 18.0,
                 20.0, 24.0, 32.0, 64.0, 128.0, 256.0]
        ).unwrap();

    pub static ref LEADER_MISSING: IntGauge =
        register_int_gauge!(
            "tikv_raftstore_leader_missing",
            "Total number of leader missed region"
        ).unwrap();

    pub static ref INGEST_SST_DURATION_SECONDS: Histogram =
        register_histogram!(
            "tikv_snapshot_ingest_sst_duration_seconds",
            "Bucketed histogram of rocksdb ingestion durations",
            exponential_buckets(0.005, 2.0, 20).unwrap()
        ).unwrap();

    pub static ref MIO_EVENT_COUNTER_VEC: IntCounterVec =
        register_int_counter_vec!(
            "tikv_raftstore_mio_event_total",
            "Total number of raftstore mio events.",
            &["event"]
        ).unwrap();

    pub static ref MIO_ITER_DURATION: Histogram =
        register_histogram!(
            "tikv_raftstore_mio_run_once_duration",
            "Bucketed histogram of raftstore mio run once duration.",
            exponential_buckets(0.000001, 2.0, 20).unwrap()
        ).unwrap();
    pub static ref STORE_WRITE_TIME: HistogramVec =
        register_histogram_vec!(
            "tikv_raftstore_write_time_micros",
            "Bucketed histogram of rocksdb write time",
            &["tag", "state"],
            exponential_buckets(1.0, 2.0, 20).unwrap()
        ).unwrap();
}

pub struct WritePerfContext {
    tag: &'static str,
    start: Instant,
    wal: u64,
    memtable: u64,
    lock: u64,
    delay: u64,
    process: u64,
}

impl WritePerfContext {
    pub fn new(tag: &'static str) -> WritePerfContext {
        set_perf_level(PerfLevel::EnableTime);
        let ctx = PerfContext::get();
        WritePerfContext {
            tag,
            start: Instant::now(),
            wal: ctx.write_wal_time(),
            memtable: ctx.write_memtable_time(),
            lock: ctx.db_mutex_lock_nanos(),
            delay: ctx.write_delay_time(),
            process: ctx.write_pre_and_post_process_time(),
        }
    }
}

impl Drop for WritePerfContext {
    fn drop(&mut self) {
        let end = PerfContext::get();
        let wal = end.write_wal_time() - self.wal;
        let memtable = end.write_memtable_time() - self.memtable;
        let lock = end.db_mutex_lock_nanos() - self.lock;
        let delay = end.write_delay_time() - self.delay;
        let process = end.write_pre_and_post_process_time() - self.process;

        let elapsed = duration_to_ms(self.start.elapsed());
        if elapsed > 1 {
            info!(
                "{} - {} wal {} memtable {} lock {} delay {} process {}",
                elapsed, self.tag, wal, memtable, lock, delay, process
            );
        }

        STORE_WRITE_TIME
            .with_label_values(&[self.tag, "wal"])
            .observe(wal as f64 / 1000.0);
        STORE_WRITE_TIME
            .with_label_values(&[self.tag, "memtable"])
            .observe(memtable as f64 / 1000.0);
        STORE_WRITE_TIME
            .with_label_values(&[self.tag, "lock"])
            .observe(lock as f64 / 1000.0);
        STORE_WRITE_TIME
            .with_label_values(&[self.tag, "delay"])
            .observe(delay as f64 / 1000.0);
        STORE_WRITE_TIME
            .with_label_values(&[self.tag, "process"])
            .observe(process as f64 / 1000.0);
    }
}
