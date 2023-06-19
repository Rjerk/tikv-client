// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use crate::{request::Request, Result, SecurityManager};
use async_trait::async_trait;
use derive_new::new;
use grpcio::{CallOption, Environment};
use std::{any::Any, sync::Arc, time::Duration};
use surrealdb_tikv_client_proto::tikvpb::TikvClient;

/// A trait for connecting to TiKV stores.
pub trait KvConnect: Sized + Send + Sync + 'static {
    type KvClient: KvClient + Clone + Send + Sync + 'static;

    fn connect(&self, address: &str) -> Result<Self::KvClient>;
}

#[derive(new, Clone)]
pub struct TikvConnect {
    env: Arc<Environment>,
    security_mgr: Arc<SecurityManager>,
    timeout: Duration,
}

impl KvConnect for TikvConnect {
    type KvClient = KvRpcClient;

    fn connect(&self, address: &str) -> Result<KvRpcClient> {
        self.security_mgr
            .connect(self.env.clone(), address, TikvClient::new)
            .map(|c| KvRpcClient::new(Arc::new(c), self.timeout))
    }
}

#[async_trait]
pub trait KvClient {
    async fn dispatch(&self, req: &dyn Request) -> Result<Box<dyn Any>>;
}

/// This client handles requests for a single TiKV node. It converts the data
/// types and abstractions of the client program into the grpc data types.
#[derive(new, Clone)]
pub struct KvRpcClient {
    rpc_client: Arc<TikvClient>,
    timeout: Duration,
}

#[async_trait]
impl KvClient for KvRpcClient {
    async fn dispatch(&self, request: &dyn Request) -> Result<Box<dyn Any>> {
        request
            .dispatch(
                &self.rpc_client,
                CallOption::default().timeout(self.timeout),
            )
            .await
    }
}
