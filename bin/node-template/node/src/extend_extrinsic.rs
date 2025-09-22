use sc_basic_authorship::ExtendExtrinsic;
use node_template_runtime::opaque::UncheckedExtrinsic;
use sp_api::ApiExt;
use sp_spot_api::SpotRuntimeApi;
use sp_runtime::traits::Block as BlockT;

pub struct ExtendTx;

impl ExtendExtrinsic<UncheckedExtrinsic> for ExtendTx {
    fn extend_extrinsic<Block: BlockT, Api: ApiExt<Block> + SpotRuntimeApi<Block>>(_api: &Api, hash: <Block as BlockT>::Hash) -> Vec<(UncheckedExtrinsic, Vec<Vec<u8>>)> {
        let _ = _api.match_spot_orders_for(hash, None);
        Vec::new()
    }
}

