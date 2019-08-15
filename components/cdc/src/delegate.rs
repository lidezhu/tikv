use std::collections::VecDeque;
use std::fmt;

use futures::sync::mpsc::UnboundedSender;
use kvproto::cdcpb::*;
use kvproto::raft_cmdpb::{
    AdminRequest, AdminResponse, CmdType, RaftResponseHeader, Request, Response,
};
use tikv::storage::mvcc::{Lock, LockType, Write, WriteType};
use tikv::storage::Key;
use tikv_util::collections::HashMap;
use tikv_util::worker::Runnable;
use tikv_util::Either;
use tokio_threadpool::ThreadPool;

use crate::RawEvent;

pub struct Delegate {
    pub region_id: u64,
    pub pending: VecDeque<(u64, Either<Vec<Request>, AdminRequest>)>,
    pub sink: UnboundedSender<ChangeDataEvent>,
    // pub buffer: HashMap<Vec<u8>, >
}

impl Delegate {
    pub fn on_data_requsts(&mut self, index: u64, reqs: Vec<Request>) {
        self.pending.push_back((index, Either::Left(reqs)))
    }
    pub fn on_responses(&mut self, index: u64, header: RaftResponseHeader) {
        while let Some((idx, req)) = self.pending.pop_front() {
            if idx < index {
                warn!("requests gap";
                    "region_id" => self.region_id,
                    "pervious_index" => idx,
                    "current_index" => index);
            // TODO: handle gap
            } else if idx == index {
                if !header.has_error() {
                    match req {
                        Either::Left(requests) => self.sink_data(index, requests),
                        Either::Right(_) => unreachable!(),
                    }
                } else {
                    self.sink_noop(index);
                }
                break;
            } else {
                self.pending.push_front((idx, req));
                break;
            }
        }
    }

    pub fn on_admin_requst(&mut self, index: u64, req: AdminRequest) {
        self.pending.push_back((index, Either::Right(req)))
    }
    pub fn on_admin_response(
        &mut self,
        index: u64,
        header: RaftResponseHeader,
        resp: AdminResponse,
    ) {
    }

    pub fn sink_noop(&self, index: u64) {
        self.sink.unbounded_send(ChangeDataEvent::new()).unwrap();
    }
    pub fn sink_data(&self, index: u64, requests: Vec<Request>) {
        let mut kv: HashMap<Vec<u8>, EventRow> = HashMap::default();
        for mut req in requests {
            if let CmdType::Put = req.cmd_type {
                let mut put = req.take_put();
                match put.cf.as_str() {
                    "write" => {
                        let write = Write::parse(put.get_value()).unwrap();
                        let (op_type, r_type) = match write.write_type {
                            WriteType::Put => (EventRowOpType::Put, EventLogType::Commit),
                            WriteType::Delete => (EventRowOpType::Delete, EventLogType::Commit),
                            WriteType::Rollback => {
                                (EventRowOpType::Unknown, EventLogType::Rollback)
                            }
                            other => {
                                debug!("skip write record";
                                    "write" => ?write.write_type);
                                continue;
                            }
                        };
                        let key = Key::from_encoded(put.take_key());
                        let commit_ts = key.decode_ts().unwrap();

                        let mut row = kv.entry(key.to_raw().unwrap()).or_default();
                        row.commit_ts = commit_ts;
                        row.key = key.to_raw().unwrap();
                        row.op_type = op_type;
                        row.r_type = r_type;
                    }
                    "lock" => {
                        let lock = Lock::parse(put.get_value()).unwrap();
                        let op_type = match lock.lock_type {
                            LockType::Put => EventRowOpType::Put,
                            LockType::Delete => EventRowOpType::Delete,
                            other => {
                                debug!("skip lock record";
                                    "lock" => ?lock.lock_type);
                                continue;
                            }
                        };
                        let key = Key::from_encoded(put.take_key());
                        let start_ts = lock.ts;

                        let mut row = kv.entry(key.to_raw().unwrap()).or_default();
                        row.start_ts = start_ts;
                        row.key = key.to_raw().unwrap();
                        row.op_type = op_type;
                        row.r_type = EventLogType::Prewrite;
                        if let Some(value) = lock.short_value {
                            row.value = value;
                        }
                    }
                    "default" => {
                        let key = Key::from_encoded(put.take_key());

                        let mut row = kv.entry(key.to_raw().unwrap()).or_default();
                        let value = put.get_value();
                        if !value.is_empty() {
                            row.value = value.to_vec();
                        }
                    }
                    other => {
                        panic!("invalid cf {}", other);
                    }
                }
            } else {
                warn!(
                    "skip non-put command";
                    "region_id" => self.region_id,
                    "command" => ?req.cmd_type,
                );
                continue;
            }
        }
        let mut entires = Vec::with_capacity(kv.len());
        for (_, v) in kv {
            entires.push(v);
        }
        let mut event_entries = EventEntries::new();
        event_entries.entries = entires.into();
        self.warp_sink(index, Event_oneof_event::Entries(event_entries));
    }
    pub fn sink_admin(&self, index: u64, request: AdminRequest, response: AdminResponse) {
        self.sink.unbounded_send(ChangeDataEvent::new()).unwrap();
    }

    pub fn warp_sink(&self, index: u64, event: Event_oneof_event) {
        let mut change_data_event = Event::new();
        change_data_event.region_id = self.region_id;
        change_data_event.index = index;
        change_data_event.event = Some(event);
        let mut change_data = ChangeDataEvent::new();
        change_data.mut_events().push(change_data_event);
        self.sink.unbounded_send(change_data).unwrap();
    }
}
