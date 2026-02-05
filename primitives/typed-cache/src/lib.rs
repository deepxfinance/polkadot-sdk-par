#![cfg_attr(not(feature = "std"), no_std)]
mod static_cache;
mod overlayed_changes;
mod traits;
mod rc_wrapper;

pub use rc_wrapper::*;
pub use static_cache::*;
pub use overlayed_changes::*;
pub use traits::*;

#[cfg(feature = "std")]
pub use log::{debug, error as log_error, trace, warn};

/// In no_std we skip logs for state_machine, this macro
/// is a noops.
#[cfg(not(feature = "std"))]
#[macro_export]
macro_rules! warn {
	(target: $target:expr, $message:expr $( , $arg:ident )* $( , )?) => {
		{
			$(
				let _ = &$arg;
			)*
		}
	};
	($message:expr, $( $arg:expr, )*) => {
		{
			$(
				let _ = &$arg;
			)*
		}
	};
}

/// In no_std we skip logs for state_machine, this macro
/// is a noops.
#[cfg(not(feature = "std"))]
#[macro_export]
macro_rules! debug {
	(target: $target:expr, $message:expr $( , $arg:ident )* $( , )?) => {
		{
			$(
				let _ = &$arg;
			)*
		}
	};
}

/// In no_std we skip logs for state_machine, this macro
/// is a noops.
#[cfg(not(feature = "std"))]
#[macro_export]
macro_rules! trace {
	(target: $target:expr, $($arg:tt)+) => {
		()
	};
	($($arg:tt)+) => {
		()
	};
}

/// In no_std we skip logs for state_machine, this macro
/// is a noops.
#[cfg(not(feature = "std"))]
#[macro_export]
macro_rules! log_error {
	(target: $target:expr, $($arg:tt)+) => {
		()
	};
	($($arg:tt)+) => {
		()
	};
}
