use frame_support::pallet_prelude::Get;
use sp_core::H160;
use crate::limits::CallLimits;

pub trait OCTAuthority<Id> {
    /// Get authority for oct account
    fn get_authority(oct: Id) -> Id;
}

impl OCTAuthority<H160> for () {
    fn get_authority(oct: H160) -> H160 {
        oct
    }
}