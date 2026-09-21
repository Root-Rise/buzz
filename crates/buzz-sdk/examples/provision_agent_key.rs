//! apiary: provision a self-hosted agent identity — keypair + NIP-OA auth tag.
//!
//! A self-hosted bot (one `buzz-acp` process holding its own key, as opposed to
//! a desktop-managed agent) needs three things in its environment:
//! `BUZZ_PRIVATE_KEY`, `BUZZ_RELAY_URL`, `BUZZ_AUTH_TAG`. This mints the first
//! and the third. The auth tag is bound to the agent pubkey, so it cannot be
//! copied from another agent — each bot needs its own.
//!
//! Usage:
//!   cargo run --release --example provision_agent_key -- <owner_secret_hex_or_nsec> [conditions]
//!
//! Prints a key file (same shape as /etc/buzz/*.key) on stdout.
//! The owner secret is read from argv; call it with the value piped in from a
//! root-only file, never from shell history.

use buzz_sdk::nip_oa;
use nostr::nips::nip19::ToBech32;
use nostr::Keys;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: {} <owner_secret_hex_or_nsec> [conditions]", args[0]);
        std::process::exit(1);
    }

    let owner_keys = Keys::parse(args[1].trim()).expect("invalid owner secret key");
    let conditions = args.get(2).map(|s| s.as_str()).unwrap_or("");

    let agent = Keys::generate();
    let tag_json = nip_oa::compute_auth_tag(&owner_keys, &agent.public_key(), conditions)
        .expect("failed to compute auth tag");

    println!("Public key:  {}", agent.public_key().to_hex());
    println!("Secret key:  {}", agent.secret_key().to_secret_hex());
    println!(
        "npub:        {}",
        agent.public_key().to_bech32().expect("bech32")
    );
    println!("Owner:       {}", owner_keys.public_key().to_hex());
    println!("Auth tag:    {tag_json}");
    println!();
    println!("Set BUZZ_PRIVATE_KEY to the secret key to use this identity.");
}
