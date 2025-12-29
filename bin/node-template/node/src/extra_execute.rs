use sc_basic_authorship::ExtraExecute;
use sp_api::ApiExt;
use sp_spot_api::SpotRuntimeApi;
use sp_perp_api::PerpRuntimeApi;
use sp_runtime::traits::Block as BlockT;

#[derive(Default)]
pub struct ExtraLogics;

impl<Block: BlockT, Api> ExtraExecute<Block, Api> for ExtraLogics
where
    Api: ApiExt<Block> + SpotRuntimeApi<Block> + PerpRuntimeApi<Block>,
{
    fn extra_execute(_api: &Api, hash: <Block as BlockT>::Hash)
    {
        let _ = _api.match_spot_orders_for(hash, None);
    }
}

