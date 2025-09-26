#![cfg_attr(not(feature = "std"), no_std)]
use sp_runtime::app_crypto::sp_core::{H160, H256, U256};
use sp_std::vec::Vec;

sp_api::decl_runtime_apis! {
    pub trait PerpRuntimeApi {
        fn match_perp_orders_for(market_id: u16) -> Vec<(Vec<(H160, u32)>, Vec<(H160, u32)>, Vec<(H160, u32)>, Vec<(H160, u32)>)>;
    }
}
