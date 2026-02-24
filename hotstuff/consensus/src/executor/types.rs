use std::str::FromStr;
use std::time::SystemTime;
use sp_api::{BlockT, HeaderT, ProvideRuntimeApi, TransactionFor};
use codec::{Encode, Decode};
use hotstuff_primitives::digests::CompatibleDigestItem;
use sc_basic_authorship::BlockExecuteInfo;
use sc_consensus::BlockImportParams;
use sp_consensus_slots::Slot;
use sp_runtime::DigestItem;
use sp_timestamp::Timestamp;
use crate::error::ViewNumber;
use crate::message::{BlockCommit, ExtrinsicBlock};

pub type SafeImportMission<B, C> = SafeWrap<ImportMission<B, C>>;

#[derive(Debug, Clone, Copy, Encode, Decode, Eq, PartialEq)]
pub enum ExecuteMode {
    Unchecked,
    Checked,
    Commit,
}

impl FromStr for ExecuteMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "unchecked" => Ok(ExecuteMode::Unchecked),
            "checked" => Ok(ExecuteMode::Checked),
            "commit" => Ok(ExecuteMode::Commit),
            _ => Err(format!("unknown execute mode: {}", s)),
        }
    }
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct PendingBlock<B: BlockT> {
    /// QC of parent block's commit.
    pub parent_commit: BlockCommit<B>,
    /// Current view number
    pub view: ViewNumber,
    /// Current block timestamp which will be set on chain.
    pub timestamp: Timestamp,
}

#[derive(Clone, Debug, Encode, Decode)]
pub enum ExecuteStage<B: BlockT> {
    /// Consensus stage at Prepare and transactions are not verified.
    Unchecked(PendingBlock<B>),
    /// Consensus stage at Prepare succeeded(transactions are verified).
    Checked(PendingBlock<B>),
    /// Block Commit QC generated so that we can just execute(not confirmed to import).
    Commit(BlockCommit<B>),
}

#[derive(Clone, Debug, Encode, Decode)]
pub struct BlockMission<B: BlockT> {
    pub stage: ExecuteStage<B>,
    pub block: ExtrinsicBlock<B>,
    pub extrinsics: Vec<Vec<Vec<B::Extrinsic>>>,
}

impl<B: BlockT> BlockMission<B> {
    pub fn mode(&self) -> ExecuteMode {
        match &self.stage {
            ExecuteStage::Unchecked(_) => ExecuteMode::Unchecked,
            ExecuteStage::Checked(_) => ExecuteMode::Checked,
            ExecuteStage::Commit(_) => ExecuteMode::Commit,
        }
    }

    pub fn view(&self) -> ViewNumber {
        match &self.stage {
            ExecuteStage::Unchecked(b) => b.view,
            ExecuteStage::Checked(b) => b.view,
            ExecuteStage::Commit(b) => b.view(),
        }
    }

    pub fn block_number(&self) -> <B::Header as HeaderT>::Number {
        self.block.number
    }

    pub fn commit_time(&self) -> &Timestamp {
        match &self.stage {
            ExecuteStage::Unchecked(b) => &b.timestamp,
            ExecuteStage::Checked(b) => &b.timestamp,
            ExecuteStage::Commit(b) => b.commit_time(),
        }
    }

    pub fn commit_hash(&self) -> Option<B::Hash> {
        if let ExecuteStage::Commit(b) = &self.stage {
            Some(b.commit_hash())
        } else {
            None
        }
    }

    pub fn block_commit(&self) -> Option<BlockCommit<B>> {
        if let ExecuteStage::Commit(b) = &self.stage {
            Some(b.clone())
        } else {
            None
        }
    }

    pub fn extrinsics_root(&self) -> B::Hash {
        self.block.extrinsics_root
    }

    pub fn parent_commit_hash(&self) -> B::Hash {
        match &self.stage {
            ExecuteStage::Unchecked(b) => b.parent_commit.commit_hash(),
            ExecuteStage::Checked(b) => b.parent_commit.commit_hash(),
            ExecuteStage::Commit(b) => b.parent_commit_hash(),
        }
    }
}


pub struct ImportMission<B: BlockT, C: ProvideRuntimeApi<B>> {
    pub mission: BlockMission<B>,
    pub slot: Slot,
    pub import: BlockImportParams<B, TransactionFor<C, B>>,
    pub info: BlockExecuteInfo<B>,
}

impl<B: BlockT, C: ProvideRuntimeApi<B>> ImportMission<B, C> {
    pub fn new(mission: BlockMission<B>, slot: Slot, import: BlockImportParams<B, TransactionFor<C, B>>, info: BlockExecuteInfo<B>) -> Self {
        Self { mission, slot, import, info }
    }

    pub fn confirm(&mut self, commit: &BlockCommit<B>) -> Result<(), String> {
        let block = self.mission.block.number.clone();
        if commit.view() != self.mission.view() {
            return Err(format!(
                "Confirm block {block} view doesn't match {}/{}",
                commit.view(),
                self.mission.view(),
            ));
        }
        if self.mission.parent_commit_hash() != commit.parent_commit_hash() {
            return Err(format!(
                "Confirm block {block} parent_commit_hash doesn't match {}/{}",
                commit.parent_commit_hash(),
                self.mission.parent_commit_hash(),
            ));
        }
        if let Some(commit_hash) = self.mission.commit_hash() {
            if commit.commit_hash() != commit_hash {
                return Err(format!(
                    "Confirm block {block} commit_hash doesn't match {}/{commit_hash}",
                    commit.commit_hash(),
                ));
            }
        }
        if commit.commit_time() != self.mission.commit_time() {
            return Err(format!(
                "Confirm block {block} commit_time doesn't match {}/{}",
                commit.commit_time(),
                self.mission.commit_time(),
            ));
        }
        // ensure block commit is in block header
        if !self.import.post_digests.iter().rev()
            .any(|log| <DigestItem as CompatibleDigestItem<BlockCommit<B>>>::as_hotstuff_seal(log).is_some())
        {
            let commit_digest_item = <DigestItem as CompatibleDigestItem<BlockCommit<B>>>::hotstuff_seal(commit.clone());
            self.import.post_digests.push(commit_digest_item);
        }
        Ok(())
    }
}
pub enum ExecutorMission<B: BlockT> {
    /// A new block can be executed.
    Execute(SystemTime, BlockMission<B>),
    /// A block can be imported.
    Confirm(SystemTime, ViewNumber, BlockCommit<B>),
}

/// We use this to wrap BlockImportParams since we ensure it is thread safe.
pub struct SafeWrap<T> {
    pub inner: std::cell::RefCell<T>,
}

unsafe impl<T> Sync for SafeWrap<T> {}
unsafe impl<T> Send for SafeWrap<T> {}

impl<T> SafeWrap<T> {
    pub fn new(value: T) -> SafeWrap<T> {
        Self { inner: std::cell::RefCell::new(value) }
    }

    pub fn into_inner(self) -> T {
        self.inner.into_inner()
    }

    pub fn inner(&self) -> core::cell::Ref<T> {
        self.inner.borrow()
    }
}