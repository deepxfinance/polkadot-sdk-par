use std::str::FromStr;
use std::time::SystemTime;
use sp_api::{BlockT, HeaderT, ProvideRuntimeApi, TransactionFor};
use codec::{Encode, Decode};
use sc_basic_authorship::BlockExecuteInfo;
use sc_consensus_slots::StorageChanges;
use sp_consensus_slots::Slot;
use sp_timestamp::Timestamp;
use crate::consensus::error::ViewNumber;
use crate::consensus::message::BlockCommit;

pub type SafeImportMission<B, C> = SafeWrap<ImportMission<B, C>>;

#[derive(Debug, Clone, Copy, Encode, Decode, Eq, PartialEq)]
pub enum ExeStage {
    Unchecked,
    Checked,
    Commit,
}

impl FromStr for ExeStage {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Unchecked" => Ok(ExeStage::Unchecked),
            "Checked" => Ok(ExeStage::Checked),
            "Commit" => Ok(ExeStage::Commit),
            _ => Err(format!("unknown execute mode: {}", s)),
        }
    }
}

#[derive(Clone, Debug, Encode, Decode)]
pub struct BlockMission<B: BlockT> {
    stage: ExeStage,
    info: BlockCommit<B>,
    extrinsics: Vec<Vec<Vec<B::Extrinsic>>>,
    confirmed: bool,
}

impl<B: BlockT> BlockMission<B> {
    pub fn new(stage: ExeStage, info: BlockCommit<B>, extrinsics: Vec<Vec<Vec<B::Extrinsic>>>) -> Self {
        Self { stage, info, extrinsics, confirmed: false }
    }

    pub fn info(&self) -> &BlockCommit<B> {
        &self.info
    }

    pub fn stage(&self) -> ExeStage {
        self.stage
    }

    pub fn confirm(&mut self, commit: BlockCommit<B>) -> Result<(), String> {
        if commit.commit_hash() == self.commit_hash() {
            self.confirmed = true;
            if self.stage != ExeStage::Commit {
                self.info = commit;
                self.stage = ExeStage::Commit
            }
            Ok(())
        } else {
            Err("commit_hash not match".into())
        }
    }

    pub fn confirmed(&self) -> bool {
        self.confirmed
    }

    pub fn extrinsics(&self) -> &Vec<Vec<Vec<B::Extrinsic>>> {
        &self.extrinsics
    }

    pub fn take_extrinsics(&mut self) -> Vec<Vec<Vec<B::Extrinsic>>> {
        core::mem::take(&mut self.extrinsics)
    }

    pub fn view(&self) -> ViewNumber {
        self.info().view()
    }

    pub fn block_number(&self) -> <B::Header as HeaderT>::Number {
        self.info().block_number()
    }

    pub fn commit_time(&self) -> &Timestamp {
        self.info().commit_time()
    }

    pub fn commit_hash(&self) -> B::Hash {
        self.info().commit_hash()
    }

    pub fn commit_info(&self) -> Option<BlockCommit<B>> {
        if self.stage == ExeStage::Commit {
            Some(self.info.clone())
        } else {
            None
        }
    }

    pub fn extrinsics_root(&self) -> B::Hash {
        self.info().extrinsics_root()
    }

    pub fn parent_commit_hash(&self) -> B::Hash {
        self.info().parent_commit_hash()
    }
}

pub struct ImportMission<B: BlockT, C: ProvideRuntimeApi<B>> {
    pub block: B,
    pub storage_changes: StorageChanges<TransactionFor<C, B>, B>,
    pub mission: BlockMission<B>,
    pub slot: Slot,
    pub info: BlockExecuteInfo<B>,
}

impl<B: BlockT, C: ProvideRuntimeApi<B>> ImportMission<B, C> {
    pub fn new(
        block: B,
        storage_changes: StorageChanges<TransactionFor<C, B>, B>,
        mission: BlockMission<B>,
        slot: Slot,
        info: BlockExecuteInfo<B>
    ) -> Self {
        Self { block, storage_changes, mission, slot, info }
    }
}

pub enum ExecutorMission<B: BlockT> {
    /// A new block can be executed.
    Execute(SystemTime, BlockMission<B>),
    /// A block can be imported.
    Confirm(SystemTime, ViewNumber, BlockCommit<B>),
}

impl<B: BlockT> ExecutorMission<B> {
    pub fn execute(time: SystemTime, mission: BlockMission<B>) -> Self {
        Self::Execute(time, mission)
    }

    pub fn confirm(time: SystemTime, view: ViewNumber, commit: BlockCommit<B>) -> Self {
        Self::Confirm(time, view, commit)
    }
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