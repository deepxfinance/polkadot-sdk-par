use std::collections::HashMap;
use std::time::{Duration, Instant};
use codec::{Decode, Encode};
use sp_runtime::traits::{Block as BlockT, Header as HeaderT};

pub struct InfoRecorder<B: BlockT> {
    pub number: <B::Header as HeaderT>::Number,
    pub max_time: Duration,
    pub block_start: Option<Instant>,
    pub finalize_start: Option<Instant>,
    pub group: GroupInfo,
    pub inherent: Option<ThreadExecutionInfo>,
    /// Record all thread execute info(single thread number should be 0).
    pub threads: HashMap<usize, ThreadExecutionInfo>,
    pub merge: Option<MergeInfo>,
    pub native: bool,
}

impl<B: BlockT> InfoRecorder<B> {
    pub fn new() -> Self {
        InfoRecorder {
            number: 0u32.into(),
            max_time: Default::default(),
            block_start: None,
            finalize_start: None,
            inherent: None,
            group: Default::default(),
            threads: HashMap::new(),
            merge: None,
            native: true,
        }
    }

    pub fn number(mut self, number: <B::Header as HeaderT>::Number) -> Self {
        self.number = number;
        self
    }

    pub fn max_time(mut self, time: Duration) -> Self {
        self.max_time = time;
        self
    }
    
    pub fn start(mut self) -> Self {
        self.block_start = Some(Instant::now());
        self
    }

    pub fn set_group_info(&mut self, info: GroupInfo) {
        self.group = info;
    }
    
    pub fn finalize_start(&mut self) {
        self.finalize_start = Some(Instant::now());
    }
    
    pub fn finalize(mut self) -> BlockExecuteInfo<B> {
        BlockExecuteInfo {
            number: self.number,
            max_time: self.max_time,
            finalize: self.finalize_start.take().expect("Finalize time not initialized").elapsed(),
            time: self.block_start.take().expect("BlockStart time not initialized").elapsed(),
            group: self.group,
            inherent: self.inherent.unwrap_or_default(),
            threads: self.threads,
            merge: self.merge.unwrap_or_default(),
            native: self.native,
        }
    }

    pub fn inherent_finish(&mut self, info: ThreadExecutionInfo) {
        self.inherent = Some(info);
    }

    pub fn thread_finish(&mut self, info: ThreadExecutionInfo) {
        self.threads.insert(info.thread, info);
    }

    pub fn merge_finish(&mut self, info: MergeInfo) {
        self.merge = Some(info);
    }
}

#[derive(Debug, Clone)]
pub struct BlockExecuteInfo<B: BlockT> {
    pub number: <B::Header as HeaderT>::Number,
    pub max_time: Duration,
    /// block finalize time(finalize + build).
    pub finalize: Duration,
    /// full block execute time.
    pub time: Duration,
    /// group transactions info from pool, not necessary.
    pub group: GroupInfo,
    pub inherent: ThreadExecutionInfo,
    /// Record all thread execute info(single thread number should be 0).
    pub threads: HashMap<usize, ThreadExecutionInfo>,
    pub merge: MergeInfo,
    pub native: bool,
}

impl<B: BlockT> BlockExecuteInfo<B> {
    pub fn set_group_info(&mut self, group: GroupInfo) {
        self.group = group;
    }

    /// Return each thread applied transactions.
    /// First should be inherent(must exist)
    /// Thread 0 should at last(must exist).
    pub fn groups(&self) -> Vec<u32> {
        let mut group_infos: Vec<_> = self.threads.values().cloned().collect();
        group_infos.sort_by(|a, b| a.thread.cmp(&b.thread));
        if self.threads.get(&0).is_some() {
            // move thread 0 to last.
            let thread_zero = group_infos.remove(0);
            group_infos.push(thread_zero);
        } else {
            // insert default empty thread 0.
            group_infos.push(ThreadExecutionInfo::default());
        }
        let group_infos = [vec![self.inherent.clone()], group_infos].concat();
        group_infos.iter().map(|info| info.applied as u32).collect()
    }

    pub fn avg_time(&self, with_extend: bool) -> Duration {
        if !self.threads.is_empty() {
            let times: Vec<_> = self.threads
                .iter()
                .map(|(_, info)| info.avg_time(with_extend))
                .filter(|t| t > &Duration::default())
                .collect();
            times.iter().sum::<Duration>() / times.len() as u32
        } else {
            Default::default()
        }
    }

    pub fn group_info(&self) -> String {
        self.group.info()
    }

    pub fn time_info(&self, limit_time: bool) -> String {
        let limit_time = if limit_time { format!("/{}", self.max_time.as_millis()) } else { "".to_string() };
        format!(
            "{}{limit_time} ms ({}({}) {}({}) {} {})ms",
            self.time.as_millis(),
            self.group.time.as_millis(),
            self.group.wait.as_millis(),
            self.merge.mth_time.as_millis(),
            self.merge.extra_merge_time.as_millis(),
            self.threads.get(&0).map(|i| i.time.as_millis()).unwrap_or_default(),
            self.finalize.as_millis(),
        )
    }

    pub fn tx_info(&self) -> String {
        let (single_applied, single_invalid) = self.threads
            .get(&0)
            .map(|i| (i.applied, i.rollback + i.stale_or_future))
            .unwrap_or_default();
        let mut total_applied = 0;
        let mut total_invalid = 0;
        self.threads
            .values()
            .for_each(|i| {
                total_applied += i.applied;
                total_invalid += i.rollback;
                total_invalid += i.stale_or_future;
            });
        let total_extrinsic = total_applied + total_invalid + self.inherent.applied;
        let mth_applied = total_applied.saturating_sub(single_applied);
        let mth_invalid = total_invalid.saturating_sub(single_invalid);
        format!("extrinsics [({mth_applied}/{mth_invalid})/({single_applied}/{single_invalid})/{total_extrinsic} native: {}]", self.native)
    }
}

#[derive(Debug, Default, Clone)]
pub struct GroupInfo {
    /// Total transaction number(groups and single).
    pub tx_count: usize,
    /// Raw groups number for parallel groups(then it is merged to `groups` by `thread_limit`)  .
    pub raw_groups: usize,
    /// Final groups length.
    pub groups: usize,
    /// Time for get transactions full process.
    pub time: Duration,
    /// time for waiting transaction pool response before start groups.
    pub wait: Duration,
    /// time for groups sort.
    pub sort: Duration,
}

impl GroupInfo {
    pub fn new(tx_count: usize, groups: usize) -> Self {
        Self {
            tx_count,
            raw_groups: groups,
            groups,
            time: Default::default(),
            wait: Default::default(),
            sort: Default::default(),
        }
    }

    pub fn info(&self) -> String {
        format!(
            "Group {} txs in {} ms(wait {} micros, sort {} micros)({} -> {} groups)",
            self.tx_count,
            self.time.as_millis(),
            self.wait.as_micros(),
            self.sort.as_micros(),
            self.raw_groups,
            self.groups,
        )
    }
}

#[derive(Debug, Default, Clone)]
pub struct ThreadExecutionInfo {
    pub thread: usize,
    /// executed and applied transactions.
    pub applied: usize,
    /// rollback transactions.
    pub rollback: usize,
    /// check nonce failed transactions.
    pub stale_or_future: usize,
    /// thread execute time
    pub time: Duration,
    /// executed extended transaction.
    pub extend: Option<Box<ThreadExecutionInfo>>,
}

impl ThreadExecutionInfo {
    pub fn new(thread: usize) -> Self {
        Self { thread, ..Default::default() }
    }

    pub fn avg_time(&self, with_extend: bool) -> Duration {
        if self.applied == 0 {
            Default::default()
        } else {
            // raw each transaction apply time
            let mut avg_tme = Duration::from_micros(self.time.as_micros() as u64 / self.applied as u64);
            if with_extend {
                // add `extend_time / applied(not extend_applied)`.
                let extend_avg = self.extend.as_ref().map(|ext| ext.time / self.applied as u32).unwrap_or_default();
                avg_tme += extend_avg;
            }
            avg_tme
        }
    }

    pub fn extend_avg_time(&self) -> Duration {
        if self.applied == 0 {
            Default::default()
        } else {
            self.extend.as_ref().map(|ext| ext.avg_time(false)).unwrap_or_default()
        }
    }
}

#[derive(Debug, Default, Clone, Copy, Encode, Decode)]
pub struct MergeInfo {
    /// Average merge time(merge time / transaction number).Just for estimate time.
    pub avg_merge_micros: u128,
    pub mth_time: Duration,
    /// Record merge_time since all parallel threads execute finished.
    pub extra_merge_time: Duration
}
