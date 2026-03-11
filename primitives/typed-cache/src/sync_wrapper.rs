use codec::{Decode, Encode};
use downcast_rs::{impl_downcast, DowncastSync};
use sp_std::fmt::{self, Debug, Formatter};
use sp_std::ops::{Deref, DerefMut};
use sp_std::sync::Arc;
use sp_std::boxed::Box;
use parking_lot::RwLock;
#[cfg(not(feature = "std"))]
use sp_std::vec::Vec;
use crate::TStorageOverlay;

pub trait RawValue: DowncastSync {
    fn clone_ref(&self) -> Box<dyn RawValue>;
    fn clone_box(&self) -> Box<dyn RawValue>;
    fn muted(&self) -> bool;
    fn set_muted(&self, muted: bool);
    fn exists(&self) -> bool;
    fn kill(&self) -> bool;
    fn put_raw(&self, value: Vec<u8>, cache: bool) -> Result<(), codec::Error>;
    fn get_raw(&self, cache: bool) -> Option<Vec<u8>>;
}

impl_downcast!(sync RawValue);

impl Clone for Box<dyn RawValue> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}

pub type AnyRc = Box<dyn RawValue>;

#[derive(Clone)]
pub enum StorageValue {
    Raw(RcT<Vec<u8>>),
    Any(Box<dyn RawValue>),
}

impl Default for StorageValue {
    fn default() -> Self {
        Self::new_raw(None, false)
    }
}

impl Debug for StorageValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Raw(rct) => f.debug_struct("Raw")
                .field("muted", &rct.muted())
                .field("value", &rct.get_raw(true).unwrap())
                .finish(),
            Self::Any(rct) => f.debug_struct("Any")
                .field("muted", &rct.muted())
                .field("value", &rct.get_raw(true).unwrap())
                .finish()
        }
    }
}

impl StorageValue {
    pub fn new_raw(value: Option<Vec<u8>>, muted: bool) -> StorageValue {
        Self::Raw(RcT::new(value, muted))
    }

    pub fn new_rct<T: TStorageOverlay>(value: Option<T>, muted: bool) -> StorageValue {
        Self::Any(Box::new(RcT::new(value, muted)))
    }

    pub fn clone_ref(&self) -> Self {
        match self {
            Self::Raw(r) => Self::Raw(r.clone_ref()),
            Self::Any(r) => Self::Any(r.clone_ref()),
        }
    }

    pub fn muted(&self) -> bool {
        match self {
            Self::Raw(rct) => rct.muted(),
            Self::Any(rct) => rct.muted(),
        }
    }

    pub fn exists(&self) -> bool {
        match self {
            Self::Raw(rct) => rct.exists(),
            Self::Any(rct) => rct.exists(),
        }
    }

    pub fn set_muted(&self, muted: bool) {
        match self {
            Self::Raw(rct) => {
                rct.0.write().muted = muted;
            },
            Self::Any(rct) => rct.set_muted(muted)
        }
    }

    pub fn kill(&self) -> bool {
        match self {
            Self::Raw(rct) => rct.kill(),
            Self::Any(rct) => rct.kill(),
        }
    }

    pub fn put_raw(&self, value: Vec<u8>, cache: bool) -> Result<(), codec::Error> {
        match self {
            Self::Raw(rct) => {
                rct.mutate(|v| *v = Some(value));
                Ok(())
            },
            Self::Any(rct) => rct.put_raw(value, cache),
        }
    }

    pub fn get_raw(&self, cache: bool) -> Option<Vec<u8>> {
        match self {
            Self::Raw(rct) => rct.clone_inner(),
            Self::Any(rct) => rct.get_raw(cache),
        }
    }

    pub fn downcast<T: TStorageOverlay>(self) -> RcT<T> {
        match self {
            Self::Raw(raw) => {
                RcT::<T>::new(
                    raw.take_value()
                        .map(|v| Decode::decode(&mut v.as_slice()).expect("invalid Raw value")),
                    raw.muted()
                )
            },
            Self::Any(any) => {
                *any.downcast::<RcT<T>>().map_err(|_| "invalid T").unwrap()
            }
        }
    }

    pub fn downcast_mut<T: TStorageOverlay>(&mut self) -> &mut RcT<T> {
        match self {
            Self::Raw(raw) => {
                let rct = RcT::<T>::new(
                    raw.take_value()
                        .map(|v| Decode::decode(&mut v.as_slice()).expect("invalid Raw value")),
                    raw.muted()
                );
                *self = Self::Any(Box::new(rct));
                self.downcast_mut()
            },
            Self::Any(any) => {
                any.downcast_mut::<RcT<T>>().ok_or("invalid T").unwrap()
            }
        }
    }

    pub fn get_ref<T: TStorageOverlay>(&mut self) -> RcT<T> {
        match self {
            Self::Raw(raw) => {
                let rct = RcT::<T>::new(
                    raw.take_value()
                        .map(|v| Decode::decode(&mut v.as_slice()).expect("invalid Raw value")),
                    raw.muted()
                );
                *self = Self::Any(Box::new(rct));
                self.get_ref()
            },
            Self::Any(any) => {
                *any.clone_ref().downcast::<RcT<T>>().map_err(|_| "invalid T").unwrap()
            }
        }
    }

    pub fn get_muted_ref<T: TStorageOverlay>(&mut self) -> Option<RcT<T>> {
        if !self.muted() { return None; }
        Some(self.get_ref())
    }

    pub fn get_muted_t<T: TStorageOverlay>(&self) -> Option<T> {
        if !self.muted() { return None; }
        self.get_t()
    }

    pub fn get_t<T: TStorageOverlay>(&self) -> Option<T> {
        match self {
            Self::Raw(raw) => {
                raw.map(|v| v.as_ref()
                    .map(|raw| Decode::decode(&mut raw.as_slice()).expect("invalid Raw value"))
                )
            },
            Self::Any(any) => {
                any.downcast_ref::<RcT<T>>().ok_or("invalid T").unwrap().clone_inner()
            }
        }
    }

    pub fn take_t<T: TStorageOverlay>(&mut self) -> Option<T> {
        match self {
            Self::Raw(raw) => {
                let res = raw.take_value()
                    .map(|raw| Decode::decode(&mut raw.as_slice()).expect("invalid Raw value"));
                *self = Self::new_rct::<T>(None, true);
                res
            },
            Self::Any(any) => {
                any.downcast_mut::<RcT<T>>().ok_or("invalid T").unwrap().take_value()
            }
        }
    }
}

impl<T: AsRef<[u8]>> From<T> for StorageValue {
    fn from(value: T) -> Self {
        Self::new_raw(Some(value.as_ref().to_vec()), false)
    }
}

impl PartialEq<Self> for StorageValue {
    fn eq(&self, other: &Self) -> bool {
        self.get_raw(false) == other.get_raw(false)
    }
}

impl Eq for StorageValue {}

#[derive(Clone, Default)]
pub struct MutT<T> {
    inner: Option<T>,
    raw: Option<Vec<u8>>,
    muted: bool,
}

impl<T: Debug> Debug for MutT<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Debug::fmt(&self.inner, f)
    }
}

impl<T> Deref for MutT<T> {
    type Target = Option<T>;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<T> DerefMut for MutT<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

/// A value reference wrapper.
///
/// ERROR: `DO NOT` use this reference cross `Transactional`.
/// Including transfer `input ot` or `output from` `Transactional` code block.
/// If you did, you will find the value is not changed as expected.
pub struct RcT<T>(Arc<RwLock<MutT<T>>>);

impl<T: TStorageOverlay> RawValue for RcT<T> {
    fn clone_ref(&self) -> Box<dyn RawValue> {
        Box::new(self.clone_ref())
    }

    fn clone_box(&self) -> Box<dyn RawValue> {
        Box::new(self.clone())
    }

    fn muted(&self) -> bool {
        self.muted()
    }

    fn set_muted(&self, muted: bool) {
        self.0.write().muted = muted;
    }

    fn exists(&self) -> bool {
        let mut_t = self.0.read();
        mut_t.inner.is_some() || mut_t.raw.is_some()
    }

    fn kill(&self) -> bool {
        self.mutate(|v| v.take().is_some())
    }

    fn put_raw(&self, value: Vec<u8>, cache: bool) -> Result<(), codec::Error> {
        self.put_raw(value, cache)
    }

    fn get_raw(&self, cache: bool) -> Option<Vec<u8>> {
        self.get_raw(cache)
    }
}

impl<T: Debug> Debug for RcT<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Debug::fmt(&self.0.read().inner, f)
    }
}

impl<T> Deref for RcT<T> {
    type Target = Arc<RwLock<MutT<T>>>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Default clone all inner data.
impl<T: Clone> Clone for RcT<T> {
    fn clone(&self) -> Self {
        RcT(Arc::new(RwLock::new(self.0.read().clone())))
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct Never;

impl<T> RcT<T> {
    pub fn new(t: Option<T>, muted: bool) -> Self {
        RcT(Arc::new(RwLock::new(MutT { inner: t, raw: None, muted })))
    }

    pub fn take_value(&self) -> Option<T> {
        self.mutate(|v| v.take())
    }

    /// Mark storage as not changed.
    ///
    /// `Invalid` for `no_std` environment.
    pub fn clear_muted(&self) {
        self.0.write().muted = false;
    }

    /// Return inner state if the value is changed.
    /// Only valid at this crate for inner value change detect.
    pub(crate) fn muted(&self) -> bool {
        self.0.read().muted
    }

    /// Apply a closure for inner value reference.
    pub fn map<O>(&self, f: impl FnOnce(&Option<T>) -> O) -> O {
        f(&self.0.read().inner)
    }

    /// Mutate inner optional `Value` withing input closure.
    ///
    /// Notice!!!
    /// The inner `Value` is considered as changed no matter if you actually changed it.
    pub fn mutate<O>(&self, f: impl FnOnce(&mut Option<T>) -> O) -> O {
        self.try_mutate(|inner| Result::<O, Never>::Ok(f(inner)))
            .expect("Typed RcT mutate is not expected to return Error")
    }

    /// Mutate inner optional `Value` withing input closure.
    ///
    /// Notice!!!
    /// The result correctness is not ensured!
    /// `DO NOT` change inner `Value` if your closure return `Error`
    pub fn try_mutate<R, E>(&self, f: impl FnOnce(&mut Option<T>) -> Result<R, E>) -> Result<R, E> {
        let mut mut_inner = self.0.write();
        let res = f(&mut mut_inner.inner);
        if res.is_ok() {
            mut_inner.raw.take();
            mut_inner.muted = true;
        }
        res
    }

    pub fn clone_ref(&self) -> Self {
        RcT(self.0.clone())
    }

    pub fn into_inner(self) -> Result<Option<T>, u32> {
        let rc_count = Arc::strong_count(&self.0);
        if rc_count > 1 {
            Err(rc_count as u32)
        } else {
            Ok(Arc::into_inner(self.0).map(|r| r.into_inner().inner).unwrap_or_default())
        }
    }
}

impl<T: Default> RcT<T> {
    /// Apply a closure for inner value reference.
    pub fn map_value_query<O>(&self, f: impl FnOnce(&T) -> O) -> O {
        if let Some(value) = self.0.read().inner.as_ref() {
            f(value)
        } else {
            f(&T::default())
        }
    }

    /// Mutate inner `Value` withing input closure.
    ///
    /// Notice!!!
    /// The inner `Value` is considered as changed no matter if you actually changed it.
    /// `DO NOT` use this function if the storage type is not `ValueQuery`
    pub fn mutate_value_query<O>(&mut self, f: impl FnOnce(&mut T) -> O) -> O {
        self.try_mutate_value_query(|inner| Result::<O, Never>::Ok(f(inner)))
            .expect("Typed RcT mutate_value_query is not expected to return Error")
    }

    /// Mutate inner `Value` withing input closure.
    ///
    /// Notice!!!
    /// The result correctness is not ensured!
    /// `DO NOT` use this function if the storage type is not `ValueQuery`
    /// `DO NOT` change inner `Value` if your closure return `Error`
    pub fn try_mutate_value_query<R, E>(&mut self, f: impl FnOnce(&mut T) -> Result<R, E>) -> Result<R, E> {
        let mut mut_inner = self.0.write();
        let res = if let Some(inner) = mut_inner.as_mut() {
            f(inner)
        } else {
            let mut tmp = T::default();
            let res = f(&mut tmp);
            if res.is_ok() {
                mut_inner.inner = Some(tmp);
            }
            res
        };

        if res.is_ok() {
            mut_inner.raw.take();
            mut_inner.muted = true;
        }
        res
    }
}

impl<T: Clone> RcT<T> {
    pub fn clone_inner(&self) -> Option<T> {
        self.0.read().inner.clone()
    }
}

impl<T: Decode> RcT<T> {
    pub fn put_raw(&self, value: Vec<u8>, cache: bool) -> Result<(), codec::Error> {
        let mut mut_inner = self.0.write();
        // `inner` is the main data, we `MUST` update it.
        mut_inner.inner = Some(T::decode(&mut value.as_slice())?);
        if cache {
            mut_inner.raw = Some(value);
        }
        mut_inner.muted = true;
        Ok(())
    }
}

impl<T: Encode> RcT<T> {
    pub fn get_raw(&self, cache: bool) -> Option<Vec<u8>> {
        let mut mut_inner = self.0.write();
        if let Some(raw_value) = &mut_inner.raw {
            Some(raw_value.clone())
        } else if let Some(raw_value) = mut_inner.inner.as_ref().map(|t| t.encode()) {
            if cache { mut_inner.raw = Some(raw_value.clone()); }
            Some(raw_value)
        } else {
            None
        }
    }
}
