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
mod merge_balances;
mod extra_execute;
mod merge_backend;

fn main() -> sc_cli::Result<()> {
	command::run()
}
