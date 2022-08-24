// Copyright (c) Aptos
// SPDX-License-Identifier: Apache-2.0

use anyhow::{Context, Result};
use aptos_sdk::types::account_address::AccountAddress;
use aptos_sdk::types::network_address::NetworkAddress;
use serde::Serialize;
use std::{
    net::{SocketAddr, ToSocketAddrs},
    time::Duration,
};

use crate::SingleCheckResult;

// This function takes a NetworkAddress and returns a string representation
// of it if it is a format we can send to NHC. Otherwise we return an error.
pub fn extract_network_address(network_address: &NetworkAddress) -> Result<String> {
    let mut socket_addrs = network_address
        .to_socket_addrs()
        .context("Failed to parse network address as SocketAddr")?;
    let socket_addr = socket_addrs
        .next()
        .ok_or_else(|| anyhow::anyhow!("No socket address found"))?;
    match socket_addr {
        SocketAddr::V4(addr) => Ok(format!("http://{}:{}", addr.ip(), addr.port())),
        SocketAddr::V6(addr) => Err(anyhow::anyhow!(
            "We do not not support IPv6 addresses: {}",
            addr
        )),
    }
}

#[derive(Debug, Serialize)]
pub struct MyBigQueryRow {
    pub account_address: String,
    pub nhc_response_json: String,
    pub ts: Duration,
}

impl From<(AccountAddress, SingleCheckResult, Duration)> for MyBigQueryRow {
    fn from(
        (account_address, single_check_result, ts): (AccountAddress, SingleCheckResult, Duration),
    ) -> Self {
        Self {
            account_address: account_address.to_string(),
            nhc_response_json: serde_json::to_string(&single_check_result)
                .expect("Failed to encode data as JSON"),
            ts,
        }
    }
}
