use codec::{Decode, Encode, Error};
use downcast_rs::{impl_downcast, Downcast};
use sp_std::fmt::{self, Debug, Formatter};
use sp_std::hash::{Hash, Hasher};
use sp_std::ops::{Deref, DerefMut};
use sp_std::cell::RefCell;
use sp_std::rc::Rc;
use sp_std::boxed::Box;
#[cfg(not(feature = "std"))]
use sp_std::vec::{self, Vec};
use crate::TStorageOverlay;

pub trait RawValue: Downcast {
    fn clone_ref(&self) -> Box<dyn RawValue>;
    fn clone_box(&self) -> Box<dyn RawValue>;
    fn muted(&self) -> bool;
    fn set_muted(&mut self, muted: bool);
    fn exists(&self) -> bool;
    fn kill(&mut self) -> bool;
    fn put_raw(&self, value: Vec<u8>, cache: bool) -> Result<(), codec::Error>;
    fn get_raw(&self, cache: bool) -> Option<Vec<u8>>;
    fn take_raw(&mut self) -> Option<Vec<u8>>;
}

impl_downcast!(RawValue);

impl Clone for Box<dyn RawValue> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}

impl RawValue for Vec<u8> {
    fn clone_ref(&self) -> Box<dyn RawValue> {
        unimplemented!("Not support for Vec<u8>")
    }

    fn clone_box(&self) -> Box<dyn RawValue> {
        Box::new(self.clone())
    }

    fn muted(&self) -> bool {
        unimplemented!("Not support for Vec<u8>")
    }

    fn set_muted(&mut self, _muted: bool) {
        unimplemented!("Not support for Vec<u8>")
    }

    fn exists(&self) -> bool {
        true
    }

    fn kill(&mut self) -> bool {
        unimplemented!("Not support for Vec<u8>")
    }

    fn put_raw(&self, _value: Vec<u8>, _cache: bool) -> Result<(), Error> {
        unimplemented!("Not support for Vec<u8>")
    }

    fn get_raw(&self, _cache: bool) -> Option<Vec<u8>> {
        Some(self.clone())
    }
    
    fn take_raw(&mut self) -> Option<Vec<u8>> {
        let res = self.clone();
        self.clear();
        Some(res)
    }
}

pub type AnyRc = Box<dyn RawValue>;

#[derive(Clone)]
pub enum StorageValue {
    Raw(RcT<Vec<u8>>),
    Any(Box<dyn RawValue>),
}

unsafe impl Sync for StorageValue {}
unsafe impl Send for StorageValue {}

impl Hash for StorageValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.get_raw(true).hash(state);
    }
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

impl AsRef<[u8]> for StorageValue {
    fn as_ref(&self) -> &[u8] {
        panic!("TypedStorageValue not support `AsRef<[u8]>`")
    }
}

impl StorageValue {
    pub fn new_raw(value: Option<Vec<u8>>, muted: bool) -> StorageValue {
        Self::Raw(RcT::new(value, muted))
    }

    pub fn new_rct<T: TStorageOverlay>(value: Option<T>, muted: bool) -> StorageValue {
        Self::Any(Box::new(RcT::new(value, muted)))
    }

    pub fn is_raw(&self) -> bool {
        match self {
            Self::Any(_) => false,
            Self::Raw(_) => true,
        }
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

    pub fn set_muted(&mut self, muted: bool) {
        match self {
            Self::Raw(rct) => {
                rct.0.borrow_mut().muted = muted;
            },
            Self::Any(rct) => rct.set_muted(muted)
        }
    }

    pub fn kill(&mut self) -> bool {
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
    
    pub fn take_raw(&mut self) -> Option<Vec<u8>> {
        match self {
            Self::Raw(rct) => rct.take_value(),
            Self::Any(rct) => rct.take_raw(),
        }
    }

    pub fn get_option_encoded(&self, cache: bool) -> Vec<u8> {
        match self {
            Self::Raw(rct) => rct.clone_inner(),
            Self::Any(rct) => rct.get_raw(cache),
        }
            .map(|v| [vec![1], v].concat())
            .unwrap_or(vec![0])
    }
    
    pub fn downcast<T: TStorageOverlay>(self) -> RcT<T> {
        match self {
            Self::Raw(mut raw) => {
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
        self.downcast_mut().clone_ref()
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

impl From<&Vec<u8>> for StorageValue {
    fn from(value: &Vec<u8>) -> Self {
        Self::new_raw(Some(value.clone()), false)
    }
}

impl From<Vec<u8>> for StorageValue {
    fn from(value: Vec<u8>) -> Self {
        Self::new_raw(Some(value), false)
    }
}

impl From<&[u8]> for StorageValue {
    fn from(value: &[u8]) -> Self {
        Self::new_raw(Some(value.to_vec()), false)
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
pub struct RcT<T>(Rc<RefCell<MutT<T>>>);

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

    fn set_muted(&mut self, muted: bool) {
        self.0.borrow_mut().muted = muted;
    }

    fn exists(&self) -> bool {
        RefCell::borrow(&self.0).inner.is_some() || RefCell::borrow(&self.0).raw.is_some()
    }

    fn kill(&mut self) -> bool {
        self.mutate(|v| v.take().is_some())
    }

    fn put_raw(&self, value: Vec<u8>, cache: bool) -> Result<(), codec::Error> {
        self.put_raw(value, cache)
    }

    fn get_raw(&self, cache: bool) -> Option<Vec<u8>> {
        self.get_raw(cache)
    }

    fn take_raw(&mut self) -> Option<Vec<u8>> {
        self.take_value().map(|v| v.encode())
    }
}

impl<T: Debug> Debug for RcT<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Debug::fmt(&RefCell::borrow(&self.0).inner, f)
    }
}

impl<T> Deref for RcT<T> {
    type Target = Rc<RefCell<MutT<T>>>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Default clone all inner data.
impl<T: Clone> Clone for RcT<T> {
    fn clone(&self) -> Self {
        RcT(Rc::new(RefCell::new(RefCell::borrow(&self.0).clone())))
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct Never;

impl<T> RcT<T> {
    pub fn new(t: Option<T>, muted: bool) -> Self {
        RcT(Rc::new(RefCell::new(MutT { inner: t, raw: None, muted })))
    }

    pub fn take_value(&mut self) -> Option<T> {
        self.mutate(|v| v.take())
    }

    /// Mark storage as not changed.
    ///
    /// `Invalid` for `no_std` environment.
    pub fn clear_muted(&mut self) {
        self.0.borrow_mut().muted = false;
    }

    /// Return inner state if the value is changed.
    /// Only valid at this crate for inner value change detect.
    pub(crate) fn muted(&self) -> bool {
        RefCell::borrow(&self.0).muted
    }

    /// Apply a closure for inner value reference.
    pub fn map<O>(&self, f: impl FnOnce(&Option<T>) -> O) -> O {
        f(&RefCell::borrow(&self.0).deref().inner)
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
        let mut mut_inner = self.0.borrow_mut();
        let res = f(&mut mut_inner.deref_mut().inner);
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
        let rc_count = Rc::strong_count(&self.0);
        if rc_count > 1 {
            Err(rc_count as u32)
        } else {
            Ok(Rc::into_inner(self.0).map(|r| r.into_inner().inner).unwrap_or_default())
        }
    }
}

impl<T: Default> RcT<T> {
    /// Apply a closure for inner value reference.
    pub fn map_value_query<O>(&self, f: impl FnOnce(&T) -> O) -> O {
        if let Some(value) = RefCell::borrow(&self.0).deref().inner.as_ref() {
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
        let mut mut_inner = self.0.borrow_mut();
        let res = if let Some(inner) = mut_inner.as_mut() {
            f(inner)
        } else {
            let mut tmp = T::default();
            let res = f(&mut tmp);
            if res.is_ok() {
                mut_inner.deref_mut().inner = Some(tmp);
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
        RefCell::borrow(&self.0).inner.clone()
    }
}

impl<T: Decode> RcT<T> {
    pub fn put_raw(&self, value: Vec<u8>, cache: bool) -> Result<(), codec::Error> {
        let mut mut_inner = self.0.borrow_mut();
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
        let mut mut_inner = self.0.borrow_mut();
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
