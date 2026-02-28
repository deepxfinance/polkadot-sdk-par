use std::collections::HashMap;
use std::time::{Duration, SystemTime};
use log::{debug, info, trace};
use sp_api::BlockT;
use sp_runtime::Saturating;
use sp_runtime::traits::{Header, NumberFor};
use sp_timestamp::Timestamp;
use crate::TRACE_LOG_TARGET;
use crate::consensus::error::ViewNumber;
use crate::consensus::message::{BlockCommit, Round};

pub enum TraceStage<B: BlockT> {
    Commit(NumberFor<B>, SystemTime),
    Executed(NumberFor<B>, SystemTime),
    Confirm(NumberFor<B>, SystemTime),
    Imported(NumberFor<B>, SystemTime),
}

impl<B: BlockT> PartialEq for TraceStage<B> {
    fn eq(&self, other: &Self) -> bool {
        self.stage_str() == other.stage_str()
            && self.block() == other.block()
            && self.time() == other.time()
    }
}

impl<B: BlockT> TraceStage<B> {
    pub fn time(&self) -> &SystemTime {
        match self {
            Self::Commit(_, time) => time,
            Self::Executed(_, time) => time,
            Self::Confirm(_, time) => time,
            Self::Imported(_, time) => time,
        }
    }

    pub fn block(&self) -> &NumberFor<B> {
        match self {
            Self::Commit(block, _) => block,
            Self::Executed(block, _) => block,
            Self::Confirm(block, _) => block,
            Self::Imported(block, _) => block,
        }
    }

    pub fn stage_str(&self) -> &str {
        match self {
            Self::Commit(..) => "C",
            Self::Executed(..) => "E",
            Self::Confirm(..) => "F",
            Self::Imported(..) => "I",
        }
    }

    pub fn id(&self) -> String {
        format!("{}:{}", self.stage_str(), self.block())
    }
}

#[derive(Clone)]
pub struct IntervalTrace<B: BlockT> {
    analyze_block_interval: usize,
    tx_count: usize,
    commit: HashMap<NumberFor<B>, (ViewNumber, SystemTime, Timestamp)>,
    execute: HashMap<NumberFor<B>, (ViewNumber, Duration, SystemTime, usize)>,
    confirm: HashMap<NumberFor<B>, (ViewNumber, SystemTime)>,
    import: HashMap<NumberFor<B>, (B::Hash, Duration, SystemTime)>,
}

impl<B: BlockT> IntervalTrace<B> {
    pub fn new(analyze_block_interval: usize) -> Self {
        Self {
            analyze_block_interval,
            tx_count: 0,
            commit: HashMap::new(),
            execute: HashMap::new(),
            confirm: HashMap::new(),
            import: HashMap::new(),
        }
    }

    /// Consensus block N(Confirm block N-1) start when:
    ///     1.Import N-2 finish.
    ///     2.Commit(consensus) N-1 finish.
    pub fn commit_time(&self, block: NumberFor<B>) -> Option<(TraceStage<B>, TraceStage<B>)> {
        let parent_block = block.saturating_sub(1u32.into());
        let grand_parent_block = block.saturating_sub(2u32.into());
        if let Some((_, _, grand_parent_import_time)) = self.import.get(&grand_parent_block) {
            if let Some((_, parent_commit_time, _)) = self.commit.get(&parent_block) {
                let start_at = if grand_parent_import_time >= parent_commit_time {
                    TraceStage::Imported(grand_parent_block, *grand_parent_import_time)
                } else {
                    TraceStage::Commit(parent_block, *parent_commit_time)
                };
                if let Some((_, block_commit_time, _)) = self.commit.get(&block) {
                    let finish_at = TraceStage::Commit(block, *block_commit_time);
                    return Some((start_at, finish_at));
                }
            }
        }
        None
    }

    /// Execute block N start when:
    ///     1.Import N-1 finish.
    ///     2.Commit(consensus) N finish.
    pub fn execute_time(&self, block: NumberFor<B>) -> Option<(TraceStage<B>, Duration, TraceStage<B>)> {
        let parent_block = block.saturating_sub(1u32.into());
        if let Some((_, block_commit_time, _)) = self.commit.get(&block) {
            if let Some((_, _, parent_imported_time)) = self.import.get(&parent_block) {
                let start_at = if parent_imported_time >= block_commit_time {
                    TraceStage::Imported(parent_block, *parent_imported_time)
                } else {
                    TraceStage::Commit(block, *block_commit_time)
                };
                if let Some((_, execute_time, block_executed_time, _)) = self.execute.get(&block) {
                    let finish_at = TraceStage::Executed(block, *block_executed_time);
                    return Some((start_at, *execute_time, finish_at));
                }
            }
        }
        None
    }

    /// Confirm block N(Consensus block N+1) start when:
    ///     1.Import N-1 finish.
    ///     2.Commit(consensus) N finish.
    /// Start rule is same with `execute`
    pub fn confirm_time(&self, block: NumberFor<B>) -> Option<(TraceStage<B>, TraceStage<B>)> {
        let parent_block = block.saturating_sub(1u32.into());
        if let Some((_, block_commit_time, _)) = self.commit.get(&block) {
            if let Some((_, _, parent_imported_time)) = self.import.get(&parent_block) {
                let start_at = if parent_imported_time >= block_commit_time {
                    TraceStage::Imported(parent_block, *parent_imported_time)
                } else {
                    TraceStage::Commit(block, *block_commit_time)
                };
                if let Some((_, block_confirm_time)) = self.confirm.get(&block) {
                    let finish_at = TraceStage::Confirm(block, *block_confirm_time);
                    return Some((start_at, finish_at));
                }
            }
        }
        None
    }

    /// Import block N start when:
    ///     1.Confirm N finish.
    ///     2.Execute N finish.
    pub fn import_time(&self, block: NumberFor<B>) -> Option<(TraceStage<B>, Duration, TraceStage<B>)> {
        if let Some((_, block_confirm_time)) = self.confirm.get(&block) {
            if let Some((_, _, block_execute_time, _)) = self.execute.get(&block) {
                let start_at = if block_confirm_time >= block_execute_time {
                    TraceStage::Confirm(block, *block_confirm_time)
                } else {
                    TraceStage::Executed(block, *block_execute_time)
                };
                if let Some((_, wait, block_import_time)) = self.import.get(&block) {
                    let finish_at = TraceStage::Imported(block, *block_import_time);
                    return Some((start_at, *wait, finish_at));
                }
            }
        }
        None
    }

    pub fn analyze_consensus_process(&self, block: NumberFor<B>) -> Option<String> {
        let commit_info = self.commit_time(block);
        let parent_confirm_info = self.confirm_time(block.saturating_sub(1u32.into()));
        let confirm_info = self.confirm_time(block);
        let mut process = "".to_string();
        let mut full_start = None;
        let mut full_finish = None;

        // handle commit process
        if let Some((start, finish)) = &commit_info {
            full_start = Some(start.time().clone());
            process += &format!("{}", start.id());
            let time = finish.time().duration_since(*start.time()).unwrap_or_default();
            if let Some((parent_confirm_start, parent_confirm_finish)) = &parent_confirm_info {
                let parent_confirm_time = parent_confirm_finish.time().duration_since(*parent_confirm_start.time()).unwrap_or_default();
                process += &format!("-{}({:?})", parent_confirm_finish.id(), parent_confirm_time);
                process += &format!("-{}({:?})", finish.id(), time.saturating_sub(parent_confirm_time));
            } else {
                process += &format!("-{}({:?})", finish.id(), time);
            }
        } else {
            process += &format!("C:{}(*)", block);
        }
        // handle confirm process
        if let Some((start, finish)) = &confirm_info {
            full_finish = Some(finish.time().clone());
            let time = finish.time().duration_since(*start.time()).unwrap_or_default();
            if let Some((_, commit_finish)) = &commit_info {
                if start.time() > commit_finish.time() {
                    let gap_time = start.time().duration_since(*commit_finish.time()).unwrap_or_default();
                    process += &format!("-{}({:?})-{}({:?})", start.id(), gap_time, finish.id(), time);
                } else {
                    process += &format!("-{}({:?})", finish.id(), time);
                }
            }
        } else {
            process += &format!("-F:{}(*)", block);
        }
        if let Some(full_start) = full_start {
            if let Some(full_finish) = full_finish {
                let full_time = full_finish.duration_since(full_start).unwrap_or_default();
                return Some(format!("{full_time:?}({process})"));
            }
        }
        if process.is_empty() {
            None
        } else {
            Some(process)
        }
    }

    pub fn analyze_exe_import_process(&self, block: NumberFor<B>) -> Option<String> {
        let execute_info = self.execute_time(block);
        let import_info = self.import_time(block);
        let mut process = "".to_string();
        let mut full_start = None;
        let mut full_finish = None;

        // handle execute process
        if let Some((start, time, finish)) = &execute_info {
            full_start = Some(start.time().clone());
            let process_time = finish.time().duration_since(*start.time()).unwrap_or_default();
            process += &format!("{}", start.id());
            process += &format!("-{}({:?}/{:?})", finish.id(), time, process_time);
        } else {
            process += &format!("E:{}(*)", block);
        }
        // handle import process
        if let Some((start, wait, finish)) = &import_info {
            full_finish = Some(finish.time().clone());
            let time = finish.time().duration_since(*start.time()).unwrap_or_default();
            if let Some((_, _, execute_finish)) = &execute_info {
                let time_without_wait = if !wait.is_zero() { format!("{:?}/", time.saturating_sub(*wait)) } else { "".into() };
                if start.time() > execute_finish.time() {
                    let gap_time = start.time().duration_since(*execute_finish.time()).unwrap_or_default();
                    process += &format!("-{}({:?})-{}({}{:?})", start.id(), gap_time, finish.id(), time_without_wait, time);
                } else {
                    process += &format!("-{}({}{:?})", finish.id(), time_without_wait, time);
                }
            }
        } else {
            process += &format!("-I:{}(*)", block);
        }
        if let Some(full_start) = full_start {
            if let Some(full_finish) = full_finish {
                let full_time = full_finish.duration_since(full_start).unwrap_or_default();
                return Some(format!("{full_time:?}({process})"));
            }
        }
        if process.is_empty() {
            None
        } else {
            Some(process)
        }
    }

    pub fn on_commit(&mut self, now: SystemTime, commit: BlockCommit<B>) {
        let block = commit.block_number();
        if let Some((pre_view, prev_time, pre_commit_time)) = self.commit.get_mut(&block) {
            if commit.view() > *pre_view {
                *prev_time = now;
                *pre_commit_time = *commit.commit_time();
            }
        } else {
            self.commit.insert(block, (commit.view(), now, *commit.commit_time()));
        }
        trace!(
            target: TRACE_LOG_TARGET,
            "[Commit] {block} QC {}:{}{}",
            commit.round(),
            commit.commit_hash(),
            self.commit
                .get(&block.saturating_sub(1u32.into()))
                .map(|pre| format!(
                    " ({:?} {}ms)",
                    now.duration_since(pre.1).unwrap_or_default(),
                    commit.commit_time().as_millis().saturating_sub(pre.2.as_millis()),
                ))
                .unwrap_or("".into()),
        );
        if self.commit.len() > self.analyze_block_interval + 2 {
            let last_analyze_block = block.saturating_sub((self.analyze_block_interval as u32).into());
            if let Some((last_view, last_time, last_commit_time)) = self.commit.get(&last_analyze_block) {
                info!(
                    target: TRACE_LOG_TARGET,
                    "[AvgCommit] {}ms commit_time {}ms(#{last_analyze_block}->#{block} view {last_view}->{})",
                    now.duration_since(*last_time).unwrap_or_default().as_micros() / self.analyze_block_interval as u128 / 1000,
                    commit.commit_time().as_millis().saturating_sub(last_commit_time.as_millis()) / self.analyze_block_interval as u64,
                    commit.view(),
                );
            }
            let mut remove_block = last_analyze_block.saturating_sub(2u32.into());
            let to_block = block.saturating_sub(2u32.into());
            loop {
                if remove_block >= to_block { break; }
                self.commit.remove(&remove_block);
                remove_block = remove_block.saturating_add(1u32.into());
            }
        }
    }

    pub fn on_executed(&mut self, now: SystemTime, block: NumberFor<B>, view: ViewNumber, time: Duration, txs: usize) {
        let new_tx_count = self.tx_count + txs;
        if let Some((pre_view, prev_time, prev_finish_time, pre_tx_count)) = self.execute.get_mut(&block) {
            if view > *pre_view {
                *prev_time = time;
                *prev_finish_time = now;
                *pre_tx_count = new_tx_count;
            }
        } else {
            self.execute.insert(block, (view, time, now, new_tx_count));
        }
        self.tx_count = new_tx_count;
        trace!(
            target: TRACE_LOG_TARGET,
            "[Execute] {block}{}",
            self.execute
                .get(&block.saturating_sub(1u32.into()))
                .map(|pre| format!(" ({:?})", now.duration_since(pre.2).unwrap_or_default()))
                .unwrap_or("".into()),
        );
        if self.execute.len() > self.analyze_block_interval + 2 {
            let last_analyze_block = block.saturating_sub((self.analyze_block_interval as u32).into());
            if let Some((_, _, last_time, last_tx_count)) = self.execute.get(&last_analyze_block) {
                let total_tx = new_tx_count.saturating_sub(*last_tx_count) as u128;
                let elapsed = now.duration_since(*last_time).unwrap_or_default().as_micros();
                info!(
                    target: TRACE_LOG_TARGET,
                    "[AvgExecute] {}ms tps {}(#{last_analyze_block}->#{block})",
                    elapsed / self.analyze_block_interval as u128 / 1000,
                    total_tx * 1_000_000 / elapsed,
                );
            }
            let mut remove_block = last_analyze_block.saturating_sub(2u32.into());
            let to_block = block.saturating_sub(2u32.into());
            loop {
                if remove_block >= to_block { break; }
                self.execute.remove(&remove_block);
                remove_block = remove_block.saturating_add(1u32.into());
            }
        }
    }

    pub fn on_confirm(&mut self, now: SystemTime, round: &Round, proposal_hash: &B::Hash, block: NumberFor<B>) {
        if let Some((pre_view, prev_time)) = self.confirm.get_mut(&block) {
            if round.view > *pre_view {
                *prev_time = now;
            }
        } else {
            self.confirm.insert(block, (round.view, now));
        }
        trace!(
            target: TRACE_LOG_TARGET,
            "[Confirm] {block} QC {round}:{proposal_hash}{}",
            self.confirm
                .get(&block.saturating_sub(1u32.into()))
                .map(|pre| format!(" ({:?})", now.duration_since(pre.1).unwrap_or_default()))
                .unwrap_or("".into()),
        );
        if let Some(analyze_info) = self.analyze_consensus_process(block) {
            debug!(target: TRACE_LOG_TARGET, "[Con] {block} {analyze_info}");
        }
        if self.confirm.len() > self.analyze_block_interval + 2 {
            let last_analyze_block = block.saturating_sub((self.analyze_block_interval as u32).into());
            if let Some((last_view, last_time)) = self.confirm.get(&last_analyze_block) {
                info!(
                    target: TRACE_LOG_TARGET,
                    "[AvgConfirm] {}ms(#{last_analyze_block}->#{block} view {last_view}->{})",
                    now.duration_since(*last_time).unwrap_or_default().as_micros() / self.analyze_block_interval as u128 / 1000,
                    round.view,
                );
            }
            let mut remove_block = last_analyze_block.saturating_sub(2u32.into());
            let to_block = block.saturating_sub(2u32.into());
            loop {
                if remove_block >= to_block { break; }
                self.confirm.remove(&remove_block);
                remove_block = remove_block.saturating_add(1u32.into());
            }
        }
    }

    pub fn on_import(&mut self, now: SystemTime, wait: Duration, header: B::Header) -> bool {
        let block = header.number();
        if let Some((pre_hash, pre_wait, prev_time)) = self.import.get_mut(&block) {
            if header.hash() == *pre_hash { return false; }
            *pre_wait = wait;
            *prev_time = now;
        } else {
            self.import.insert(*block, (header.hash(), wait, now));
        }
        trace!(
            target: TRACE_LOG_TARGET,
            "[Import] {block}{}",
            self.import
                .get(&block.saturating_sub(1u32.into()))
                .map(|pre| format!(
                    " ({:?}{})",
                    now.duration_since(pre.2).unwrap_or_default(),
                    if !wait.is_zero() { format!(" W {wait:?}") } else { "".into() }
                ))
                .unwrap_or("".into()),
        );
        if let Some(analyze_info) = self.analyze_exe_import_process(*block) {
            debug!(target: TRACE_LOG_TARGET, "[Exe] {block} {analyze_info}");
        }
        if self.import.len() > self.analyze_block_interval + 2 {
            let last_analyze_block = block.saturating_sub((self.analyze_block_interval as u32).into());
            if let Some((_, _, last_time)) = self.import.get(&last_analyze_block) {
                info!(
                    target: TRACE_LOG_TARGET,
                    "[AvgImport] {}ms(#{last_analyze_block}->#{block})",
                    now.duration_since(*last_time).unwrap_or_default().as_micros() / self.analyze_block_interval as u128 / 1000,
                );
            }
            let mut remove_block = last_analyze_block.saturating_sub(2u32.into());
            let to_block = block.saturating_sub(2u32.into());
            loop {
                if remove_block >= to_block { break; }
                self.import.remove(&remove_block);
                remove_block = remove_block.saturating_add(1u32.into());
            }
        }
        true
    }
}