// Copyright (c) Aptos
// SPDX-License-Identifier: Apache-2.0

use anyhow::{Context, Result};
use once_cell::sync::Lazy;
use std::{
    str::FromStr,
    time::{SystemTime, UNIX_EPOCH},
};
use url::Url;

use aptos_sdk::{
    bcs,
    move_types::{
        identifier::Identifier,
        language_storage::{ModuleId, TypeTag},
    },
    rest_client::PendingTransaction,
    transaction_builder::TransactionBuilder,
    types::{
        chain_id::ChainId,
        transaction::{ScriptFunction, TransactionPayload},
    },
};

//:!:>section_1
// LocalAccount provides helpers around addresses, txn signing, etc.
use aptos_sdk::types::LocalAccount;
//<:!:section_1

//:!:>section_2
// Clients for working with the Aptos API and the faucet.
use aptos_sdk::rest_client::{Client, FaucetClient};
//<:!:section_2

use aptos_sdk::types::account_address::AccountAddress;

static DEVNET_URL: Lazy<Url> =
    Lazy::new(|| Url::from_str("https://fullnode.devnet.aptoslabs.com").unwrap());
static FAUCET_URL: Lazy<Url> =
    Lazy::new(|| Url::from_str("https://faucet.devnet.aptoslabs.com").unwrap());

//:!:>section_7
#[tokio::main]
async fn main() -> Result<()> {
    let rest_client = Client::new(DEVNET_URL.clone());
    let faucet_client = FaucetClient::new(FAUCET_URL.clone(), DEVNET_URL.clone());

    // Create two accounts, Alice and Bob, and fund Alice but not Bob
    let mut alice = LocalAccount::generate(&mut rand::rngs::OsRng);
    let bob = LocalAccount::generate(&mut rand::rngs::OsRng);

    println!("\n=== Addresses ===");
    println!("Alice: {}", alice.address().to_hex_literal());
    println!("Bob: {}", bob.address().to_hex_literal());

    faucet_client
        .fund(alice.address(), 5_000)
        .await
        .context("Failed to fund Alice's account")?;
    faucet_client
        .create_account(bob.address())
        .await
        .context("Failed to fund Bob's account")?;

    println!("\n=== Initial Balances ===");
    println!(
        "Alice: {:?}",
        //:!:>section_2
        rest_client
            .get_account_balance(alice.address())
            .await
            .context("Failed to get Alice's account balance")?
            .inner()
        //<:!:section_2
    );
    println!(
        "Bob: {:?}",
        rest_client
            .get_account_balance(bob.address())
            .await
            .context("Failed to get Bob's account balance")?
            .inner()
    );

    // Have Alice send Bob some coins
    let tx_hash = transfer(&rest_client, &mut alice, bob.address(), 1_000)
        .await
        .context("Failed to submit transaction to transfer coins")?;
    rest_client
        .wait_for_transaction(&tx_hash)
        .await
        .context("Failed when waiting for the transfer transaction")?;

    println!("\n=== Final Balances ===");
    println!(
        "Alice: {:?}",
        rest_client
            .get_account_balance(alice.address())
            .await
            .context("Failed to get Alice's account balance the second time")?.inner()
    );
    println!(
        "Bob: {:?}",
        rest_client
            .get_account_balance(bob.address())
            .await
            .context("Failed to get Bob's account balance the second time")?.inner()
    );

    Ok(())
}
//<:!:section_7

async fn transfer(
    rest_client: &Client,
    from_account: &mut LocalAccount,
    to_account: AccountAddress,
    amount: u64,
) -> Result<PendingTransaction> {
    //:!:>section_2
    let chain_id = rest_client
        .get_index()
        .await
        .context("Failed to get chain id")?
        .inner()
        .ledger_info
        .chain_id;
    let transaction_builder = TransactionBuilder::new(
        TransactionPayload::ScriptFunction(ScriptFunction::new(
            ModuleId::new(AccountAddress::ONE, Identifier::new("coin").unwrap()),
            Identifier::new("transfer").unwrap(),
            vec![TypeTag::from_str("0x1::aptos_coin::AptosCoin").unwrap()],
            vec![
                bcs::to_bytes(&to_account).unwrap(),
                bcs::to_bytes(&amount).unwrap(),
            ],
        )),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 10,
        ChainId::new(chain_id),
    )
    .sender(from_account.address())
    .sequence_number(from_account.sequence_number())
    .max_gas_amount(5000)
    .gas_unit_price(1);
    let signed_txn = from_account.sign_with_transaction_builder(transaction_builder);
    Ok(rest_client
        .submit(&signed_txn)
        .await
        .context("Failed to submit transfer transaction")?
        .into_inner())
    //<:!:section_2
}
