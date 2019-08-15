use futures::sync::mpsc;
use futures::{Future, Sink, Stream};
use grpcio::*;
use kvproto::cdcpb::*;
use kvproto::cdcpb_grpc::*;
use tikv_util::worker::*;

use crate::endpoint::Task;

pub struct Service {
    scheduler: Scheduler<Task>,
}

impl Service {
    pub fn new(scheduler: Scheduler<Task>) -> Service {
        Service { scheduler }
    }
}

impl ChangeData for Service {
    fn event_feed(
        &mut self,
        ctx: RpcContext,
        request: ChangeDataRequest,
        sink: ServerStreamingSink<ChangeDataEvent>,
    ) {
        // TODO: make it a bounded channel.
        let (tx, rx) = mpsc::unbounded();
        if let Err(status) = self
            .scheduler
            .schedule(Task::Register { request, sink: tx })
            .map_err(|e| RpcStatus::new(RpcStatusCode::INVALID_ARGUMENT, Some(format!("{:?}", e))))
        {
            error!("cdc task initiate failed"; "error" => ?status);
            ctx.spawn(sink.fail(status).map_err(|e| {
                error!("cdc failed to send error"; "error" => ?e);
            }));
            return;
        }

        let send_resp = sink.send_all(rx.then(|resp| match resp {
            Ok(resp) => Ok((resp, WriteFlags::default())),
            Err(e) => {
                error!("cdc send failed"; "error" => ?e);
                Err(Error::RpcFailure(RpcStatus::new(
                    RpcStatusCode::UNKNOWN,
                    Some(format!("{:?}", e)),
                )))
            }
        }));
        ctx.spawn(
            send_resp
                .map(|_s /* the sink */| {
                    info!("cdc send half closed");
                })
                .map_err(|e| {
                    error!("cdc send failed"; "error" => ?e);
                }),
        );
    }
}
