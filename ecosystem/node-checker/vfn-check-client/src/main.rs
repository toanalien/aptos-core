// Copyright (c) Aptos
// SPDX-License-Identifier: Apache-2.0

mod helpers;

use anyhow::{Context, Result};
use aptos_node_checker_lib::EvaluationSummary;
use aptos_sdk::rest_client::Client as AptosClient;
use aptos_sdk::types::account_address::AccountAddress;
use aptos_sdk::types::account_config::CORE_CODE_ADDRESS;
use aptos_sdk::types::network_address::NetworkAddress;
use aptos_sdk::types::on_chain_config::ValidatorSet;
use aptos_sdk::types::validator_info::ValidatorInfo;
use clap::Parser;
use gcp_bigquery_client::model::dataset::Dataset;
use gcp_bigquery_client::model::table::Table;
use gcp_bigquery_client::model::table_data_insert_all_request::TableDataInsertAllRequest;
use gcp_bigquery_client::model::table_field_schema::TableFieldSchema;
use gcp_bigquery_client::model::table_schema::TableSchema;
use gcp_bigquery_client::Client as BigQueryClient;
use helpers::{extract_network_address, MyBigQueryRow};
use log::info;
use reqwest::Client as ReqwestClient;
use serde::Serialize;
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use url::Url;

#[derive(Debug, Serialize)]
pub enum CheckResultFailureCode {
    // The network address in the validator set config cannot be used for
    // querying NHC.
    UnsupportedNetworkAddressType,

    // Something went wrong when sending / receiving the request.
    RequestFlowError,

    // The response from NHC was not a 200, implying a problem with NHC.
    ResponseNot200,

    // The response from NHC couldn't be deserialized.
    CouldNotDeserializeResponse,
}

/// We use this struct to capture when checking one of the nodes failed.
/// We have this struct instead of just using Result so it can be easily
/// serialized to JSON.
#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SingleCheckResult {
    /// The node was successfully checked. Note: The evaulation itself could
    /// indicate, a problem with the node, this just states that we were able
    /// to check the node sucessfully with NHC.
    Success(EvaluationSummary),

    /// Something went wrong with checking the node.
    Failure((String, CheckResultFailureCode)),
}

#[derive(Clone, Debug, clap::ValueEnum)]
pub enum OutputStyle {
    Stdout,
    BigQuery,
}

#[derive(Debug, Parser)]
pub struct BigQueryArgs {
    /// Path to the BigQuery key file.
    #[clap(long, parse(from_os_str))]
    big_query_key_path: PathBuf,

    /// GCP project ID.
    #[clap(long, default_value = "analytics-test-345723")]
    gcp_project_id: String,

    /// BigQuery dataset ID.
    #[clap(long, default_value = "nhc_ait3_1")]
    big_query_dataset_id: String,

    /// BigQuery table ID.
    #[clap(long, default_value = "nhc_response_data")]
    big_query_table_id: String,
}

#[derive(Debug, Parser)]
pub struct Args {
    /// Address of any node (of any type) connected to the network you want
    /// to evaluate.
    #[clap(long)]
    node_address: Url,

    /// Address where NHC is running.
    #[clap(long)]
    nhc_address: Url,

    /// Baseline config to use when talking to NHC.
    #[clap(long)]
    nhc_baseline_config_name: String,

    /// Baseline config to use when talking to NHC.
    #[clap(long, default_value_t = 30)]
    nhc_timeout_secs: u64,

    /// How to output the results.
    #[clap(long, value_enum, default_value = "stdout", case_insensitive = true)]
    output_style: OutputStyle,

    #[clap(flatten)]
    big_query_args: BigQueryArgs,
}

/// Get all the on chain validator info.
async fn get_validator_info(node_address: Url) -> Result<Vec<ValidatorInfo>> {
    let client = AptosClient::new(node_address);
    let response = client
        .get_account_resource_bcs::<ValidatorSet>(CORE_CODE_ADDRESS, "0x1::stake::ValidatorSet")
        .await?;
    let active_validators = response.into_inner().active_validators;
    println!("Active validators: {:#?}", active_validators);
    info!(
        "Pulled {} active validators. First: {}. Last: {}",
        active_validators.len(),
        active_validators.first().unwrap().account_address(),
        active_validators.last().unwrap().account_address()
    );
    Ok(active_validators)
}

/// Check all VFNs from the validator set.
async fn check_vfns(
    nhc_client: &ReqwestClient,
    nhc_address: &Url,
    nhc_baseline_config_name: &str,
    validator_infos: Vec<ValidatorInfo>,
) -> Result<HashMap<AccountAddress, SingleCheckResult>> {
    let mut nhc_responses = HashMap::new();
    for validator_info in validator_infos {
        for address in validator_info
            .config()
            .fullnode_network_addresses()
            .context("Failed to deserialize VFN network addresses")?
        {
            nhc_responses.insert(
                *validator_info.account_address(),
                check_single_vfn(nhc_client, nhc_address, nhc_baseline_config_name, &address).await,
            );
        }
    }
    Ok(nhc_responses)
}

/// Make a query to NHC for a single validator's VFNs. A single validator could
/// have multiple VFN addresses, so we return a single result with a map of
/// results keyed by the address.
async fn check_single_vfn(
    nhc_client: &ReqwestClient,
    nhc_address: &Url,
    nhc_baseline_config_name: &str,
    vfn_address: &NetworkAddress,
) -> SingleCheckResult {
    let mut url = nhc_address.clone();
    url.set_path("/check_node");

    // Get a string representation of the vfn address if possible.
    let vfn_address_string = match extract_network_address(vfn_address) {
        Ok(vfn_address_string) => vfn_address_string,
        Err(e) => {
            return SingleCheckResult::Failure((
                format!("Network address was an unsupported type: {}", e),
                CheckResultFailureCode::UnsupportedNetworkAddressType,
            ));
        }
    };

    // Build up query params.
    let mut params = HashMap::new();
    params.insert("node_url", vfn_address_string);
    params.insert(
        "baseline_configuration_name",
        nhc_baseline_config_name.to_string(),
    );

    // Send the request and parse the response.
    let response = match nhc_client.get(url.clone()).query(&params).send().await {
        Ok(response) => response,
        Err(e) => {
            return SingleCheckResult::Failure((
                format!("Error with request flow to NHC: {:#}", e),
                CheckResultFailureCode::RequestFlowError,
            ));
        }
    };

    // Handle the error case.
    if let Err(e) = response.error_for_status_ref() {
        return SingleCheckResult::Failure((
            format!("{:#}: {:?}", e, response.text().await),
            CheckResultFailureCode::ResponseNot200,
        ));
    };

    match response.json::<EvaluationSummary>().await {
        Ok(evaluation_summary) => SingleCheckResult::Success(evaluation_summary),
        Err(e) => SingleCheckResult::Failure((
            format!("{:#}", e),
            CheckResultFailureCode::CouldNotDeserializeResponse,
        )),
    }
}

async fn write_to_big_query(
    big_query_args: &BigQueryArgs,
    nhc_responses: HashMap<AccountAddress, SingleCheckResult>,
) -> Result<()> {
    let client = BigQueryClient::from_service_account_key_file(
        big_query_args
            .big_query_key_path
            .to_str()
            .context("Big query key path was invalid")?,
    )
    .await;

    // Create the dataset if necessary.
    let dataset = client
        .dataset()
        .create(
            Dataset::new(
                &big_query_args.gcp_project_id,
                &big_query_args.big_query_dataset_id,
            )
            .location("US")
            .friendly_name("NHC AIT3 1"),
        )
        .await
        .context("Failed to create the dataset")?;

    // Create the table if necessary.
    let _table = dataset
        .create_table(
            &client,
            Table::from_dataset(
                &dataset,
                &big_query_args.big_query_table_id,
                TableSchema::new(vec![
                    TableFieldSchema::timestamp("ts"),
                    TableFieldSchema::string("account_address"),
                    // TODO: Consider using a record instead to give it more structure.
                    TableFieldSchema::string("nhc_response_json"),
                ]),
            )
            .friendly_name("NHC response data")
            .description("NHC check responses from vfn-check-client for AIT3 VFN checks"),
        )
        .await
        .context("Failed to create the table")?;

    // Build the request to send to BigQuery.
    let mut insert_request = TableDataInsertAllRequest::new();
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("Failed to get current time")?;
    for (account_address, single_check_result) in nhc_responses {
        insert_request.add_row(
            None,
            MyBigQueryRow::from((account_address, single_check_result, now)),
        )?;
    }

    // Submit the request.
    client
        .tabledata()
        .insert_all(
            &big_query_args.gcp_project_id,
            &big_query_args.big_query_dataset_id,
            &big_query_args.big_query_table_id,
            insert_request,
        )
        .await?;

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let args = Args::parse();

    let nhc_client = ReqwestClient::builder()
        .timeout(Duration::from_secs(args.nhc_timeout_secs))
        .build()
        .unwrap();

    let validator_infos = get_validator_info(args.node_address)
        .await
        .context("Failed to get on chain validator info")?;

    let nhc_responses = check_vfns(
        &nhc_client,
        &args.nhc_address,
        &args.nhc_baseline_config_name,
        validator_infos,
    )
    .await
    .context("Failed to check nodes unexpectedly")?;

    match args.output_style {
        OutputStyle::Stdout => {
            println!(
                "{}",
                serde_json::to_string(&nhc_responses).context("Failed to encode data as JSON")?
            );
        }
        OutputStyle::BigQuery => {
            write_to_big_query(&args.big_query_args, nhc_responses)
                .await
                .context("Failed to write to BigQuery")?;
        }
    }

    Ok(())
}
