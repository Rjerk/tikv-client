// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

mod client;
mod errors;
mod request;

#[doc(inline)]
pub use crate::{
    client::{KvClient, KvConnect, TikvConnect},
    errors::{HasError, HasRegionError},
    request::Request,
};
pub use surrealdb_tikv_client_common::{security::SecurityManager, Error, Result};
