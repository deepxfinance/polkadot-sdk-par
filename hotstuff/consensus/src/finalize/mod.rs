pub mod justification;
pub mod types;

use std::marker::PhantomData;
use std::sync::Arc;
use tokio::sync::mpsc::Receiver;
use sc_client_api::Backend;
use sp_api::{BlockT, TransactionFor};
use crate::client::ClientForHotstuff;
use crate::network::{HotstuffNetworkBridge, Network, Syncing};

pub struct Finalizer<
    B: BlockT,
    BE: Backend<B>,
    C: ClientForHotstuff<B, BE>,
    N: Network<B> + Sync + 'static,
    S: Syncing<B> + Sync + 'static,
> {
    client: Arc<C>,
    network: HotstuffNetworkBridge<B, N, S>,
    /// finalize interval.
    interval: usize,
    rx: Receiver<(B::Header, TransactionFor<C, B>)>,
    phantom: PhantomData<BE>
}

impl<B, BE, C, N, S> Finalizer<B, BE, C, N, S>
where
    B: BlockT,
    BE: Backend<B>,
    C: ClientForHotstuff<B, BE>,
    N: Network<B> + Sync + 'static,
    S: Syncing<B> + Sync + 'static,
{
    pub fn new(
        client: Arc<C>,
        network: HotstuffNetworkBridge<B, N, S>,
        interval: Option<usize>,
        rx: Receiver<(B::Header, TransactionFor<C, B>)>,
    ) -> Self {
        Self {
            client,
            network,
            interval: interval.unwrap_or(1).max(1),
            rx,
            phantom: PhantomData
        }
    }

    pub fn storage_changes_root(&mut self, transaction: TransactionFor<C, B>) -> B::Hash {
        // TODO calculate root for transaction.
        B::Hash::default()
    }

    pub async fn run(&mut self) {
        loop {
            if let Some(mission) = self.rx.recv().await {

            }
        }
    }
}
