//! Substrate Node Template CLI library.
#![warn(missing_docs)]

mod chain_spec;
#[macro_use]
mod service;
mod benchmarking;
mod cli;
mod command;
mod rpc;
mod merge_handler;
mod transaction_group;

fn main() -> sc_cli::Result<()> {
	command::run()
}
