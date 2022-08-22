/* eslint-disable no-console */

import dotenv from "dotenv";
dotenv.config();

import { AptosClient, AptosAccount, FaucetClient, Types } from "aptos";
import { aptosCoin } from "./constants";

const NODE_URL = process.env.APTOS_NODE_URL || "https://fullnode.devnet.aptoslabs.com";
const FAUCET_URL = process.env.APTOS_FAUCET_URL || "https://faucet.devnet.aptoslabs.com";

(async () => {
  const client = new AptosClient(NODE_URL);
  const faucetClient = new FaucetClient(NODE_URL, FAUCET_URL);

  // Create accounts.
  const alice = new AptosAccount();
  const bob = new AptosAccount();

  // Print out account addresses.
  console.log("=== Addresses ===");
  console.log(`Alice: ${alice.address()}`);
  console.log(`Alice: ${bob.address()}`);
  console.log("");

  // Fund accounts.
  await faucetClient.fundAccount(alice.address(), 20000);
  await faucetClient.fundAccount(bob.address(), 0);

  // Print out initial balances.
  console.log("=== Initial Balances ===");
  console.log(`Alice: ${await getAccountBalance(client, alice)}`);
  console.log(`Bob: ${await getAccountBalance(client, bob)}`);
  console.log("");

  // Have Alice send Bob some AptosCoins.
  await client.transferCoins(alice, bob, 1_000);

  // Print out intermediate balances.
  console.log("=== Intermediate Balances ===");
  console.log(`Alice: ${await getAccountBalance(client, alice)}`);
  console.log(`Bob: ${await getAccountBalance(client, bob)}`);
  console.log("");

  // Have Alice send Bob some more AptosCoins.
  await client.transferCoins(alice, bob, 1_000);

  // Print out final balances.
  console.log("=== Final Balances ===");
  console.log(`Alice: ${await getAccountBalance(client, alice)}`);
  console.log(`Bob: ${await getAccountBalance(client, bob)}`);
  console.log("");
})();

async function getAccountBalance(client: AptosClient, account: AptosAccount): Promise<string> {
  let resources = await client.getAccountResources(account.address());
  let accountResource = resources.find((r: any) => r.type === aptosCoin)!;
  let balance = (accountResource.data as { coin: { value: string } }).coin.value;
  return balance;
}
