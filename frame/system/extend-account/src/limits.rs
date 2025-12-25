use frame_support::pallet_prelude::Get;

pub trait CallLimits {
    /// Free interval(milliseconds) for transaction call.
    fn free_interval() -> u64;
    /// Valid call time range between (now - from, now + to).
    fn time_range() -> (u64, u64);
    /// Set account's time nonce window size.
    fn window_size() -> u32;
}

impl CallLimits for () {
    /// Default free interval is 10 seconds.
    fn free_interval() -> u64 { 10000 }

    /// Default range is (now - 1 hour, now + 1 hour)
    fn time_range() -> (u64, u64) {
        (3_600_000, 3_600_000)
    }

    /// Default account's time nonce window 100.
    fn window_size() -> u32 {
        100
    }
}

impl<Free: Get<u64>, From: Get<u64>, To: Get<u64>, Window: Get<u32>> CallLimits for (Free, From, To, Window) {
    fn free_interval() -> u64 {
        Free::get()
    }

    fn time_range() -> (u64, u64) {
        (Free::get(), To::get())
    }

    fn window_size() -> u32 {
        Window::get()
    }
}
