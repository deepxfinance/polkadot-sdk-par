use sc_basic_authorship::ExtendExtrinsic;
use node_template_runtime::opaque::UncheckedExtrinsic;
use sp_api::ApiExt;
use sp_runtime::traits::Block as BlockT;

pub struct ExtendTx;

impl ExtendExtrinsic<UncheckedExtrinsic> for ExtendTx {
    fn extend_extrinsic<Block: BlockT, Api: ApiExt<Block>>(_api: &Api) -> Vec<(UncheckedExtrinsic, Vec<Vec<u8>>)> {
        Vec::new()
    }
}

