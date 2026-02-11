use std::collections::HashMap;
use std::time::{Duration, SystemTime};
use log::{debug, info};
use sp_api::BlockT;
use sp_runtime::Saturating;
use sp_runtime::traits::{Header, NumberFor};
use crate::TRACE_LOG_TARGET;
use crate::error::ViewNumber;
use crate::message::{BlockCommit, Round};

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
    commit: HashMap<NumberFor<B>, (ViewNumber, SystemTime)>,
    execute: HashMap<NumberFor<B>, (ViewNumber, Duration, SystemTime)>,
    confirm: HashMap<NumberFor<B>, (ViewNumber, SystemTime)>,
    import: HashMap<NumberFor<B>, (B::Hash, SystemTime)>,
}

impl<B: BlockT> IntervalTrace<B> {
    pub fn new(analyze_block_interval: usize) -> Self {
        Self {
            analyze_block_interval,
            commit: HashMap::new(),
            execute: HashMap::new(),
            confirm: HashMap::new(),
            import: HashMap::new(),
        }
    }

    /// Consensus block N start when:
    ///     1.Import N-2 finish.
    ///     2.Commit(consensus) N-1 finish.
    pub fn commit_time(&self, block: NumberFor<B>) -> Option<(TraceStage<B>, TraceStage<B>)> {
        let parent_block = block.saturating_sub(1u32.into());
        let grand_parent_block = block.saturating_sub(2u32.into());
        if let Some((_, grand_parent_import_time)) = self.import.get(&grand_parent_block) {
            if let Some((_, parent_commit_time)) = self.commit.get(&parent_block) {
                let start_at = if grand_parent_import_time >= parent_commit_time {
                    TraceStage::Imported(grand_parent_block, *grand_parent_import_time)
                } else {
                    TraceStage::Commit(parent_block, *parent_commit_time)
                };
                if let Some((_, block_commit_time)) = self.commit.get(&block) {
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
        if let Some((_, block_commit_time)) = self.commit.get(&block) {
            if let Some((_, parent_imported_time)) = self.import.get(&parent_block) {
                let start_at = if parent_imported_time >= block_commit_time {
                    TraceStage::Imported(parent_block, *parent_imported_time)
                } else {
                    TraceStage::Commit(block, *block_commit_time)
                };
                if let Some((_, execute_time, block_executed_time)) = self.execute.get(&block) {
                    let finish_at = TraceStage::Executed(block, *block_executed_time);
                    return Some((start_at, *execute_time, finish_at));
                }
            }
        }
        None
    }

    /// Confirm block N start when:
    ///     1.Import N-1 finish.
    ///     2.Commit(consensus) N finish.
    /// Start rule is same with `execute`
    pub fn confirm_time(&self, block: NumberFor<B>) -> Option<(TraceStage<B>, TraceStage<B>)> {
        let parent_block = block.saturating_sub(1u32.into());
        if let Some((_, block_commit_time)) = self.commit.get(&block) {
            if let Some((_, parent_imported_time)) = self.import.get(&parent_block) {
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
    pub fn import_time(&self, block: NumberFor<B>) -> Option<(TraceStage<B>, TraceStage<B>)> {
        if let Some((_, block_confirm_time)) = self.confirm.get(&block) {
            if let Some((_, _, block_execute_time)) = self.execute.get(&block) {
                let start_at = if block_confirm_time >= block_execute_time {
                    TraceStage::Confirm(block, *block_confirm_time)
                } else {
                    TraceStage::Executed(block, *block_execute_time)
                };
                if let Some((_, block_import_time)) = self.import.get(&block) {
                    let finish_at = TraceStage::Imported(block, *block_import_time);
                    return Some((start_at, finish_at));
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
        let mut gap = None;

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
                    process += &format!("-G({:?})-{}({:?})", gap_time, finish.id(), time);
                    gap = Some(gap_time);
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
                let gap = gap.map(|t| format!("(Gap {t:?})")).unwrap_or_default();
                return Some(format!("{full_time:?}{gap} ({process})"));
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
        let mut gap = None;

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
        if let Some((start, finish)) = &import_info {
            full_finish = Some(finish.time().clone());
            let time = finish.time().duration_since(*start.time()).unwrap_or_default();
            if let Some((_, _, execute_finish)) = &execute_info {
                if start.time() > execute_finish.time() {
                    let gap_time = start.time().duration_since(*execute_finish.time()).unwrap_or_default();
                    process += &format!("-G({:?})-{}({:?})", gap_time, finish.id(), time);
                    gap = Some(gap_time);
                } else {
                    process += &format!("-{}({:?})", finish.id(), time);
                }
            }
        } else {
            process += &format!("-I:{}(*)", block);
        }
        if let Some(full_start) = full_start {
            if let Some(full_finish) = full_finish {
                let full_time = full_finish.duration_since(full_start).unwrap_or_default();
                let gap = gap.map(|t| format!("(Gap {t:?})")).unwrap_or_default();
                return Some(format!("{full_time:?}{gap} ({process})"));
            }
        }
        if process.is_empty() {
            None
        } else {
            Some(process)
        }
    }

    pub fn analyze_block_process(&self, block: NumberFor<B>) -> Option<String> {
        let commit_info = self.commit_time(block);
        let execute_info = self.execute_time(block);
        let confirm_info = self.confirm_time(block);
        let import_info = self.import_time(block);

        let mut process = "".to_string();
        let mut full_start = None;
        let mut full_finish = None;
        let mut block_exe_imp_time = None;

        if let Some((execute_start, _, execute_finish)) = &execute_info {
            let execute_time = execute_finish.time().duration_since(*execute_start.time()).unwrap_or_default();
            if let Some((import_start, import_finish)) = &import_info {
                let import_time = import_finish.time().duration_since(*import_start.time()).unwrap_or_default();
                block_exe_imp_time = Some(execute_time + import_time);
            }
        }

        // handle commit process
        if let Some((start, finish)) = &commit_info {
            let time = finish.time().duration_since(*start.time()).unwrap_or_default();
            process += &format!("{}-{}({:?})", start.id(), finish.id(), time);
            full_start = Some(*start.time());
        } else {
            process += &"C(*)";
        }
        // handle execute process
        if let Some((start, time, finish)) = &execute_info {
            let mut process_time = finish.time().duration_since(*start.time()).unwrap_or_default();
            if let Some((_, commit_finish)) = &commit_info {
                if commit_finish != start {
                    if commit_finish.time() >= start.time() {
                        process += &format!("/{}", start.id());
                    } else {
                        let wait = start.time().duration_since(*commit_finish.time()).unwrap_or_default();
                        process += &format!("-{}({:?})", start.id(), wait);
                        process_time = process_time.saturating_sub(wait);
                    }
                }
            }
            process += &format!("-{}({:?}/{:?})", finish.id(), time, process_time);
        } else {
            process += &format!("-E:{}(*)", block);
        }
        // handle confirm process
        if let Some((start, finish)) = &confirm_info {
            let mut time = finish.time().duration_since(*start.time()).unwrap_or_default();
            if let Some((_, _, execute_finish)) = &execute_info {
                if execute_finish != start {
                    if execute_finish.time() >= start.time() {
                        process += &format!("/{}", start.id());
                    } else {
                        let wait = start.time().duration_since(*execute_finish.time()).unwrap_or_default();
                        process += &format!("-{}({:?})", start.id(), wait);
                        time = time.saturating_sub(wait);
                    }
                }
            }
            process += &format!("-{}({:?})", finish.id(), time);
        } else {
            process += &format!("-F:{}(*)", block);
        }
        // handle import process
        if let Some((start, finish)) = &import_info {
            let mut time = finish.time().duration_since(*start.time()).unwrap_or_default();
            if let Some((_, confirm_finish)) = &confirm_info {
                if confirm_finish != start {
                    if confirm_finish.time() >= start.time() {
                        process += &format!("/{}", start.id());
                    } else {
                        let wait = start.time().duration_since(*confirm_finish.time()).unwrap_or_default();
                        process += &format!("-{}({:?})", start.id(), wait);
                        time = time.saturating_sub(wait);
                    }
                }
            }
            process += &format!("-{}({:?})", finish.id(), time);
            full_finish = Some(*finish.time());
        } else {
            process += &format!("-I:{}(*)", block);
        }
        if let Some(full_start) = full_start {
            if let Some(full_finish) = full_finish {
                let import_interval = self.import
                    .get(&block.saturating_sub(1u32.into()))
                    .map(|pre| format!("{:>12?} ", pre.1.elapsed()))
                    .unwrap_or("             ".into());
                let full_time = full_finish.duration_since(full_start).unwrap_or_default();
                let exe_time_info = if let Some(block_exe_imp_time) = block_exe_imp_time {
                    format!("({:>12?}/{block_exe_imp_time:<12?}) ", full_time.saturating_sub(block_exe_imp_time))
                } else {
                    format!("({full_time:>12?}) ")
                };
                return Some(format!("{import_interval}{exe_time_info}({process})"));
            }
        }
        None
    }

    pub fn on_commit(&mut self, commit: &BlockCommit<B>, parent_round: &Round, parent_proposal: &B::Hash) {
        let now = SystemTime::now();
        let block = commit.block_number();
        if let Some((pre_view, prev_time)) = self.commit.get_mut(&block) {
            if commit.view() > *pre_view {
                *prev_time = now;
            }
        } else {
            self.commit.insert(block, (commit.view(), now));
        }
        let parent_block = block.saturating_sub(1u32.into());
        debug!(
            target: TRACE_LOG_TARGET,
            "[Commit] block {block} by QC {}:{} parent {parent_round}:{parent_proposal}{}",
            commit.round(),
            commit.commit_hash(),
            self.commit
                .get(&parent_block)
                .map(|pre| format!(" ({:?})", pre.1.elapsed()))
                .unwrap_or("".into()),
        );
        if self.commit.len() > self.analyze_block_interval {
            let last_analyze_block = block.saturating_sub((self.analyze_block_interval as u32).into());
            if let Some((last_view, last_time)) = self.commit.get(&last_analyze_block) {
                info!(
                    target: TRACE_LOG_TARGET,
                    "[Commit] Average block time {}ms(block #{last_analyze_block}->#{block} view {last_view}->{})",
                    now.duration_since(*last_time).unwrap_or_default().as_micros() / self.analyze_block_interval as u128 / 1000,
                    commit.view(),
                );
            }
            let mut remove_block = last_analyze_block;
            loop {
                if remove_block >= block { break; }
                self.commit.remove(&remove_block);
                remove_block = remove_block.saturating_add(1u32.into());
            }
        }
    }

    pub fn on_executed(&mut self, block: NumberFor<B>, view: ViewNumber, time: Duration) {
        let now = SystemTime::now();
        if let Some((pre_view, prev_time, prev_finish_time)) = self.execute.get_mut(&block) {
            if view > *pre_view {
                *prev_time = time;
                *prev_finish_time = now;
            }
        } else {
            self.execute.insert(block, (view, time, now));
        }
    }

    pub fn on_confirm(&mut self, round: &Round, proposal_hash: &B::Hash, block: NumberFor<B>) {
        let now = SystemTime::now();
        if let Some((pre_view, prev_time)) = self.confirm.get_mut(&block) {
            if round.view > *pre_view {
                *prev_time = now;
            }
        } else {
            self.confirm.insert(block, (round.view, now));
        }
        let parent_block = block.saturating_sub(1u32.into());
        debug!(
            target: TRACE_LOG_TARGET,
            "[Confirm] block {block} by QC {round}:{proposal_hash}{}",
            self.confirm
                .get(&parent_block)
                .map(|pre| format!(" ({:?})", pre.1.elapsed()))
                .unwrap_or("".into()),
        );
        if let Some(analyze_info) = self.analyze_consensus_process(block) {
            info!(target: TRACE_LOG_TARGET, "[Analyze Con] {block} {analyze_info}");
        }
        if self.confirm.len() > self.analyze_block_interval {
            let last_analyze_block = block.saturating_sub((self.analyze_block_interval as u32).into());
            if let Some((last_view, last_time)) = self.confirm.get(&last_analyze_block) {
                info!(
                    target: TRACE_LOG_TARGET,
                    "[Confirm] Average block time {}ms(block #{last_analyze_block}->#{block} view {last_view}->{})",
                    now.duration_since(*last_time).unwrap_or_default().as_micros() / self.analyze_block_interval as u128 / 1000,
                    round.view,
                );
            }
            let mut remove_block = last_analyze_block;
            loop {
                if remove_block >= block { break; }
                self.confirm.remove(&remove_block);
                remove_block = remove_block.saturating_add(1u32.into());
            }
        }
    }

    pub fn on_import(&mut self, header: &B::Header) -> bool {
        let now = SystemTime::now();
        let block = header.number();
        if let Some((pre_hash, prev_time)) = self.import.get_mut(&block) {
            if header.hash() == *pre_hash { return false; }
            *prev_time = now;
        } else {
            self.import.insert(*block, (header.hash(), now));
        }
        let parent_block = block.saturating_sub(1u32.into());
        debug!(
            target: TRACE_LOG_TARGET,
            "[Imported] block {block}{}",
            self.import
                .get(&parent_block)
                .map(|pre| format!(" ({:?})", pre.1.elapsed()))
                .unwrap_or("".into()),
        );
        if let Some(analyze_info) = self.analyze_exe_import_process(*block) {
            info!(target: TRACE_LOG_TARGET, "[Analyze Exe] {block} {analyze_info}");
        }
        if self.import.len() > self.analyze_block_interval {
            let last_analyze_block = block.saturating_sub((self.analyze_block_interval as u32).into());
            if let Some((_, last_time)) = self.import.get(&last_analyze_block) {
                info!(
                    target: TRACE_LOG_TARGET,
                    "[Imported] Average block time {}ms(block #{last_analyze_block}->#{block})",
                    now.duration_since(*last_time).unwrap_or_default().as_micros() / self.analyze_block_interval as u128 / 1000,
                );
            }
            let mut remove_block = last_analyze_block;
            loop {
                if remove_block >= *block { break; }
                self.import.remove(&remove_block);
                remove_block = remove_block.saturating_add(1u32.into());
            }
        }
        true
    }
}