use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use sc_transaction_pool_api::TransactionPool;
use sp_runtime::traits::{Block as BlockT, Header as HeaderT};

pub struct InfoRecorder<B: BlockT> {
    pub parent_hash: Option<B::Hash>,
    pub number: <B::Header as HeaderT>::Number,
    pub hash: Option<B::Hash>,
    pub state_hash: Option<B::Hash>,
    pub max_time: Duration,
    pub block_start: Option<Instant>,
    pub finalize_start: Option<Instant>,
    pub group: GroupInfo,
    pub inherent: Option<ThreadExecutionInfo<B>>,
    pub thread_order_count: usize,
    /// Record all thread execute info(single thread number should be 0).
    pub threads: HashMap<usize, ThreadExecutionInfo<B>>,
    pub merge: Option<MergeInfo>,
    pub native: bool,
}

impl<B: BlockT> InfoRecorder<B> {
    pub fn new() -> Self {
        InfoRecorder {
            parent_hash: None,
            number: 0u32.into(),
            hash: None,
            state_hash: None,
            max_time: Default::default(),
            block_start: None,
            finalize_start: None,
            inherent: None,
            group: Default::default(),
            thread_order_count: 0,
            threads: HashMap::new(),
            merge: None,
            native: true,
        }
    }

    pub fn parent_hash(mut self, parent_hash: B::Hash) -> Self {
        self.parent_hash = Some(parent_hash);
        self
    }

    pub fn number(mut self, number: <B::Header as HeaderT>::Number) -> Self {
        self.number = number;
        self
    }

    pub fn set_state_hash(&mut self, state_hash: B::Hash) {
        self.state_hash = Some(state_hash);
    }

    pub fn set_hash(&mut self, hash: B::Hash) {
        self.hash = Some(hash);
    }

    pub fn set_thread(&mut self, thread: ThreadExecutionInfo<B>) {
        self.threads.insert(self.thread_order_count, thread);
        self.thread_order_count += 1;
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
            parent_hash: self.parent_hash.expect("Finalize parent_hash not initialized"),
            number: self.number,
            hash: self.hash.expect("Finalize hash not initialized"),
            state_hash: self.state_hash.expect("Finalize state_hash not initialized"),
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

    pub fn inherent_finish(&mut self, info: ThreadExecutionInfo<B>) {
        self.inherent = Some(info);
    }

    pub fn thread_finish(&mut self, info: ThreadExecutionInfo<B>) {
        self.threads.insert(info.thread, info);
    }

    pub fn merge_finish(&mut self, info: MergeInfo) {
        self.merge = Some(info);
    }
}

#[derive(Debug, Clone)]
pub struct BlockExecuteInfo<B: BlockT> {
    pub parent_hash: B::Hash,
    pub number: <B::Header as HeaderT>::Number,
    pub hash: B::Hash,
    pub state_hash: B::Hash,
    pub max_time: Duration,
    /// block finalize time(finalize + build).
    pub finalize: Duration,
    /// full block execute time.
    pub time: Duration,
    /// group transactions info from pool, not necessary.
    pub group: GroupInfo,
    pub inherent: ThreadExecutionInfo<B>,
    /// Record all thread execute info(single thread number should be 0).
    /// Key is insert order, not thread number.
    pub threads: HashMap<usize, ThreadExecutionInfo<B>>,
    pub merge: MergeInfo,
    pub native: bool,
}

impl<B: BlockT> BlockExecuteInfo<B> {
    pub fn set_group_info(&mut self, group: GroupInfo) {
        self.group = group;
    }

    pub fn is_empty_block(&self) -> bool {
        if self.threads.is_empty() {
            return true;
        }
        for info in self.threads.values() {
            if info.total > 0 {
                return false;
            }
        }
        true
    }

    /// Return each thread applied transactions.
    /// First should be inherent(must exist)
    /// Thread 0 should at last(must exist).
    pub fn groups(&self) -> Vec<u32> {
        let mut group_infos: Vec<_> = self.threads.values().cloned().collect();
        group_infos.sort_by(|a, b| a.thread.cmp(&b.thread));
        if group_infos.first().map(|t| t.thread == 0).unwrap_or(false) {
            // move thread 0 to last.
            let thread_zero = group_infos.remove(0);
            group_infos.push(thread_zero);
        } else {
            // insert default empty thread 0.
            group_infos.push(ThreadExecutionInfo::default());
        }
        let group_infos = [vec![self.inherent.clone()], group_infos].concat();
        group_infos.iter().map(|info| info.total as u32).collect()
    }

    pub fn avg_time(&self) -> (Duration, usize) {
        if !self.threads.is_empty() {
            let mut weight = 0;
            let times: Vec<_> = self.threads
                .iter()
                .map(|(_, info)| info.avg_time())
                .filter(|(t, _)| t > &Duration::default())
                .map(|(t, w)| { weight += w; t })
                .collect();
            if !times.is_empty() {
                return (times.iter().sum::<Duration>() / times.len() as u32, weight);
            }
        }
        (Default::default(), 0)
    }

    pub fn applied(&self) -> usize {
        self.single_applied().unwrap_or_default() + self.mth_applied().1
    }

    pub fn mth_applied(&self) -> (usize, usize) {
        let mut mth_threads = 0usize;
        let mth_applied = self.threads
            .iter()
            .filter(|(_, info)| info.thread > 0)
            .map(|(_, info)| {
                mth_threads += 1;
                info.total
            })
            .sum::<usize>();
        (mth_threads, mth_applied)
    }

    pub fn single_applied(&self) -> Option<usize> {
        self.threads.iter()
            .find(|(_, info)| info.thread == 0)
            .map(|(_, info)| if info.time != Default::default() {
                Some(info.total)
            } else {
                None
            })?
    }

    pub fn group_info(&self) -> String {
        self.group.info()
    }

    pub fn threads(&self) -> usize {
        self.threads.iter().filter(|(_, info)| info.thread > 0).count()
    }

    pub fn threads_debug(&self, limit_thread_time: bool) -> Vec<String> {
        self.threads
            .iter()
            .filter(|(_, ti)| ti.rounds.len() > 0)
            .map(|(_, ti)| ti.thread_debug(limit_thread_time))
            .collect()
    }

    pub fn time_info(&self, limit_time: bool) -> String {
        let limit_time = if limit_time { format!("/{}", self.max_time.as_millis()) } else { "".to_string() };
        let mut group_info = "".to_string();
        if self.group.time > Default::default() {
            group_info = format!(
                "G{}{} ",
                self.group.time.as_millis(),
                if self.group.wait > Default::default() {
                    format!("(W{})", self.group.wait.as_millis())
                } else {
                    "".into()
                }
            );
        }
        let mut merge_info = "".to_string();
        if self.merge.mth_time > Default::default() {
            merge_info = format!(
                "M{}{} ",
                self.merge.mth_time.as_millis(),
                if self.merge.extra_merge_time > Default::default() {
                    format!("(E{})", self.merge.extra_merge_time.as_millis())
                } else {
                    "".into()
                }
            );
        }
        let mut single_info = "".to_string();
        let single_thread_time = self.threads
            .iter()
            .find(|(_, info)| info.thread == 0)
            .map(|(_, info)| info.time)
            .unwrap_or_default();
        if single_thread_time > Default::default() {
            single_info = format!("S{} ", single_thread_time.as_millis());
        }
        format!(
            "{}{limit_time}ms({group_info}{merge_info}{single_info}F{})ms",
            self.time.as_millis(),
            self.finalize.as_millis(),
        )
    }

    pub fn time_debug(&self, limit_time: bool) -> String {
        let limit_time = if limit_time { format!("/{:?}", self.max_time) } else { "".to_string() };
        let mut group_info = "".to_string();
        if self.group.time > Default::default() {
            group_info = format!(
                "G{:?}{} ",
                self.group.time,
                if self.group.wait > Default::default() {
                    format!("(W{:?})", self.group.wait)
                } else {
                    "".into()
                }
            );
        }
        let mut merge_info = "".to_string();
        if self.merge.mth_time > Default::default() {
            merge_info = format!(
                "M{:?}{} ",
                self.merge.mth_time,
                if self.merge.extra_merge_time > Default::default() {
                    format!("(E{:?})", self.merge.extra_merge_time)
                } else {
                    "".into()
                }
            );
        }
        let mut single_info = "".to_string();
        let single_thread_time = self.threads
            .iter()
            .find(|(_, info)| info.thread == 0)
            .map(|(_, info)| info.time)
            .unwrap_or_default();
        if single_thread_time > Default::default() {
            single_info = format!("S{:?} ", single_thread_time);
        }
        format!(
            "{:?}{limit_time}({group_info}{merge_info}{single_info}F{:?})",
            self.time,
            self.finalize,
        )
    }

    pub fn tx_info(&self) -> String {
        let (mut single_applied, single_invalid) = self.threads
            .iter()
            .find(|(_, info)| info.thread == 0)
            .map(|(_, i)| (i.total, i.invalid + i.future_or_exhausted))
            .unwrap_or_default();
        single_applied = single_applied.saturating_sub(single_invalid);
        let mut total_applied = 0;
        let mut total_invalid = 0;
        self.threads
            .values()
            .for_each(|i| {
                total_applied += i.total;
                total_invalid += i.invalid;
                total_invalid += i.future_or_exhausted;
            });
        total_applied = total_applied.saturating_sub(total_invalid);
        let total_extrinsic = total_applied + total_invalid + self.inherent.total;
        let mth_applied = total_applied.saturating_sub(single_applied);
        let mth_invalid = total_invalid.saturating_sub(single_invalid);
        format!("extrinsics [({mth_applied}/{mth_invalid})/({single_applied}/{single_invalid})/{total_extrinsic} native: {}]", self.native)
    }

    pub fn info(&self, limit_time: bool) -> String {
        format!(
            "Prepared {} [{}] [hash: {:?}; parent_hash: {}; {}, threads {}]",
            self.number,
            self.time_info(limit_time),
            self.hash,
            self.parent_hash,
            self.tx_info(),
            self.threads(),
        )
    }

    pub fn debug(&self, limit_time: bool) -> String {
        format!(
            "Prepared {} [{}] [hash: {:?}; parent_hash: {}; {}, threads {}]",
            self.number,
            self.time_debug(limit_time),
            self.hash,
            self.parent_hash,
            self.tx_info(),
            self.threads(),
        )
    }
}

#[derive(Debug, Clone, Default)]
pub struct GroupTxInput {
    /// Max time to wait for pool ready.
    pub wait_pool: Duration,
    /// Time limit for full time getting transaction from pool.
    pub max_time: Duration,
    /// Group max threads target.
    pub thread_limit: usize,
    /// linear transaction limit based on time(groups(max) + single).
    pub linear_tx_limit: usize,
    /// Minimal single thread transaction number(if exists).
    pub single_tx_min: usize,
    /// Limit total transactions get from pool.
    pub total_tx_limit: usize,
    /// Limit block size when group transactions.
    pub block_size_limit: usize,
}

impl GroupTxInput {
    pub fn info(&self) -> String {
        format!(
            "thread {} linear {} single {} total {}",
            self.thread_limit,
            self.linear_tx_limit,
            self.single_tx_min,
            self.total_tx_limit,
        )
    }
}

pub struct GroupTxOutput<A: TransactionPool> {
    /// Parallel execute transactions.
    pub groups: Vec<Vec<(usize, Arc<<A as TransactionPool>::InPoolTransaction>)>>,
    /// Tailing single thread execute transactions.
    pub single: Vec<(usize, Arc<<A as TransactionPool>::InPoolTransaction>)>,
    pub info: GroupInfo,
}

#[derive(Debug, Default, Clone)]
pub struct GroupInfo {
    pub input: GroupTxInput,
    /// Total transaction number(groups and single).
    pub tx_count: usize,
    /// Filtered transaction
    pub filtered: usize,
    /// Raw transactions number before sorted/filtered by limits.
    pub raw_tx_count: usize,
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
    pub fn info(&self) -> String {
        if self.tx_count == 0 && self.filtered == 0 {
            return "".to_string();
        }
        let filter_info = if self.filtered > 0 {
            format!("(filtered {})", self.filtered)
        } else {
            "".to_string()
        };
        format!(
            "Group {}({}) tx in {}ms(W{}μs S{}μs){}(groups {}->{})(Input: {})",
            self.tx_count,
            self.raw_tx_count,
            self.time.as_millis(),
            self.wait.as_micros(),
            self.sort.as_micros(),
            filter_info,
            self.raw_groups,
            self.groups,
            self.input.info()
        )
    }
}

#[derive(Debug, Clone)]
pub struct ThreadExecutionInfo<B: BlockT> {
    pub thread: usize,
    /// Thread time limit.
    pub time_limit: Duration,
    /// total transactions.
    pub total: usize,
    /// failed but valid transactions.
    pub future_or_exhausted: usize,
    /// failed an invalid transactions.
    pub invalid: usize,
    /// thread execute time.
    pub time: Duration,
    /// thread extra execution time.
    pub extend_time: Duration,
    /// Finish thread time.
    pub finish_time: Duration,
    /// transaction hashes(future transactions are not filtered).
    pub transactions: Vec<B::Hash>,

    /// More exact execution information
    ///
    /// Transaction execution batch rounds.
    pub rounds: Vec<usize>,
    /// Exact times by round([execute_time, read_time, read_backend_time, write_time] nanos).
    /// If feature `dev-time` not enabled, only `execute_time` is valid.
    pub round_times: Vec<[u128; 4]>,
    /// Thread end reason.
    pub end_reason: String,
}

impl<B: BlockT> Default for ThreadExecutionInfo<B> {
    fn default() -> Self {
        Self {
            thread: 0,
            time_limit: Default::default(),
            total: 0,
            invalid: 0,
            future_or_exhausted: 0,
            time: Default::default(),
            extend_time: Default::default(),
            finish_time: Default::default(),
            transactions: vec![],
            rounds: vec![],
            round_times: vec![],
            end_reason: "".to_string(),
        }
    }
}

impl<B: BlockT> ThreadExecutionInfo<B> {
    pub fn new(thread: usize) -> Self {
        Self { thread, ..Default::default() }
    }

    pub fn applied(&self) -> usize {
        self.total.saturating_sub(self.invalid).saturating_sub(self.future_or_exhausted)
    }

    pub fn avg_time(&self) -> (Duration, usize) {
        let applied = self.applied();
        if applied == 0 {
            (Default::default(), 0)
        } else {
            // raw each transaction apply time
            let avg_tme = Duration::from_micros(self.time.as_micros() as u64 / applied as u64);
            let weight = applied;
            (avg_tme, weight)
        }
    }

    pub fn round_info(&self) -> String {
        #[cfg(feature = "dev-time")]
        {
            let round_with_times = self.rounds.iter().zip(self.round_times.iter()).map(|(txs, time)| {
                let tx_num = (*txs).max(1);
                let io_time = time[1] + time[3];
                let avg = time[0] as usize / tx_num;
                let avg_io = io_time as usize / tx_num;
                let avg_rb = time[2] as usize / tx_num;
                let avg_w = time[3] as usize / tx_num;
                (*txs, avg, avg.saturating_sub(avg_io), avg_io, avg_rb, avg_w)
            }).collect::<Vec<_>>();
            return format!("([(num, avg, exe, io, rb, wr)]: {round_with_times:?})")
        }
        #[cfg(not(feature = "dev-time"))]
        {
            let round_with_times = self.rounds
                .iter()
                .zip(self.round_times.iter())
                .map(|(txs, time)| (*txs, time[0] as usize / (*txs).max(1)))
                .collect::<Vec<_>>();
            return format!("([(num, avg)]: {round_with_times:?})")
        }
    }

    pub fn thread_debug(&self, limit_time: bool) -> String {
        let limit_millis_info = if limit_time { format!("/{:?}", self.time_limit) } else { "".to_string() };
        let invalid_info = if self.invalid > 0 || self.future_or_exhausted > 0 {
            format!("({}/{})", self.invalid, self.future_or_exhausted)
        } else {
            "".to_string()
        };
        let mut thread_name = "single".to_string();
        if self.thread > 0 {
            thread_name = format!("{}", self.thread);
        }
        format!(
            "Thread {thread_name} {:?}(E{:?} F{:?}){} {}/{}{invalid_info} {} rounds({}){}.",
            self.time,
            self.extend_time,
            self.finish_time,
            limit_millis_info,
            self.applied(),
            self.total,
            self.end_reason,
            self.rounds.len(),
            self.round_info(),
        )
    }
}

#[derive(Debug, Default, Clone)]
pub struct MergeInfo {
    /// Average merge time(merge time / transaction number).Just for estimate time.
    pub avg_merge_micros: u128,
    /// Full multi threads time from `mth_start` to `mth_merge_finish`.
    pub mth_time: Duration,
    /// Record merge_time since all parallel threads execute finished.
    pub extra_merge_time: Duration,
    /// Record all merge details(each merge with (target_threads, source_threads, total time, merge StorageTransactionCache time))
    pub records: Vec<(Vec<usize>, Vec<usize>, Duration, Duration)>,
}

impl MergeInfo {
    pub fn print_records(&self) -> Vec<String> {
        self.records.iter().map(|(to, from, time, stc_time)| format!(
            "Threads {to:?} and {from:?} in {time:?}(STC {stc_time:?})"
        ))
            .collect()
    }
}
