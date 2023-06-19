// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

#[doc(inline)]
pub use cluster::{Cluster, Connection};
#[doc(inline)]
pub use surrealdb_tikv_client_common::{security::SecurityManager, Error, Result};

#[macro_use]
extern crate log;

mod cluster;
mod timestamp;
