// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::default;

use derive_new::new;

use crate::proto::metapb;
use crate::region_cache::RegionCache;
use crate::Error;
use crate::Key;
use crate::Result;

/// The ID of a region
pub type RegionId = u64;
/// The ID of a store
pub type StoreId = u64;

/// RegionVerID is a unique ID that can identify a Region at a specific version.
#[derive(Eq, PartialEq, Hash, Clone, Default, Debug)]
pub struct RegionVerId {
    /// The ID of the region
    pub id: RegionId,
    /// Conf change version, auto increment when add or remove peer
    pub conf_ver: u64,
    /// Region version, auto increment when split or merge
    pub ver: u64,
}

// InvalidReason is the reason why a cached region is invalidated.
// The region cache may take different strategies to handle different reasons.
// For example, when a cached region is invalidated due to no leader, region cache
// will always access to a different peer.
#[derive(Clone, PartialEq, Debug, Default)]
enum InvalidReason {
    // Ok indicates the cached region is valid
    #[default]
    Ok,
    // NoLeader indicates it's invalidated due to no leader
    NoLeader,
    // RegionNotFound indicates it's invalidated due to region not found in the store
    RegionNotFound,
    // EpochNotMatch indicates it's invalidated due to epoch not match
    EpochNotMatch,
    // StoreNotFound indicates it's invalidated due to store not found in PD
    StoreNotFound,
    // Other indicates it's invalidated due to other reasons, e.g., the store
    // is removed from the cluster, fail to send requests to the store.
    Other,
}

// Region presents kv region
#[derive(Clone, PartialEq, Debug, Default)]
pub struct Region {
    // raw region meta from PD, immutable after init
    pub meta: metapb::Region,
    pub store: RegionStoreInfo,
    // region TTL in epoch seconds, see checkRegionCacheTTL
    ttl: i64,
    // region need be sync later, see needReloadOnAccess, needExpireAfterTTL
    sync_flags: i32,
    // the reason why the region is invalidated
    invalid_reason: InvalidReason,
}

impl Region {
    fn new(meta: metapb::Region) -> Region {
        Region {
            meta,
            ..Default::default()
        }
    }
}

#[derive(Clone, PartialEq, Debug, Default)]
pub struct RegionStoreInfo {
    // buckets is not accurate and it can change even if the region is not changed.
    // It can be stale and buckets keys can be out of the region range.
    buckets: metapb::Buckets,
    // pendingPeers refers to pdRegion.PendingPeers. It's immutable and can be used to reconstruct pdRegions.
    pub pending_peers: Vec<metapb::Peer>,
    // downPeers refers to pdRegion.DownPeers. It's immutable and can be used to reconstruct pdRegions.
    pub down_peers: Vec<metapb::Peer>,
}

/// Information about a TiKV region and its leader.
///
/// In TiKV all data is partitioned by range. Each partition is called a region.
#[derive(new, Clone, Default, Debug, PartialEq)]
pub struct RegionWithLeader {
    pub region: metapb::Region,
    pub leader: Option<metapb::Peer>,
}

impl Eq for RegionWithLeader {}

impl RegionWithLeader {
    /// 检查当前 Region 的键值范围是否包含指定的 Key
    pub fn contains(&self, key: &Key) -> bool {
        let key: &[u8] = key.into();
        let start_key = &self.region.start_key;
        let end_key = &self.region.end_key;
        key >= start_key.as_slice() && (key < end_key.as_slice() || end_key.is_empty())
    }

    pub fn start_key(&self) -> Key {
        self.region.start_key.to_vec().into()
    }

    pub fn end_key(&self) -> Key {
        self.region.end_key.to_vec().into()
    }

    pub fn range(&self) -> (Key, Key) {
        (self.start_key(), self.end_key())
    }

    pub fn ver_id(&self) -> RegionVerId {
        let region = &self.region;
        let epoch = region.region_epoch.as_ref().unwrap();
        RegionVerId {
            id: region.id,
            conf_ver: epoch.conf_ver,
            ver: epoch.version,
        }
    }

    pub fn id(&self) -> RegionId {
        self.region.id
    }

    pub fn get_store_id(&self) -> Result<StoreId> {
        self.leader
            .as_ref()
            .cloned()
            .ok_or_else(|| Error::LeaderNotFound {
                region_id: self.id(),
            })
            .map(|s| s.store_id)
    }
}
