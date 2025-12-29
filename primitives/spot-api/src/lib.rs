#![cfg_attr(not(feature = "std"), no_std)]
use sp_runtime::app_crypto::sp_core::{H160, H256, U256};
use sp_std::vec::Vec;

sp_api::decl_runtime_apis! {
    pub trait SpotRuntimeApi {
        fn match_spot_orders_for(pair: Option<H256>) -> Vec<(H256, Vec<((H160, U256), Vec<(H160, U256)>)>, Vec<((H160, U256), Vec<(H160, U256)>)>)>;
    }
}
