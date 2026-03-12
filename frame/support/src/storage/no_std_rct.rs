use codec::{Decode, Encode};
use sp_std::boxed::Box;
use sp_std::fmt::{self, Debug};
use sp_std::ops::{Deref, DerefMut};
use sp_std::cell::RefCell;
use sp_std::rc::Rc;
use sp_std::vec::Vec;
use typed_cache::{RawValue, TStorageOverlay};
use crate::Never;

pub struct MutT<T> {
    inner: Option<T>,
    raw: Option<Vec<u8>>,
    key: Vec<u8>,
}

impl<T: Encode + Decode> Clone for MutT<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner
                .as_ref()
                .map(|v| Decode::decode(&mut v.encode().as_slice()).unwrap()),
            raw: self.raw.clone(),
            key: self.key.clone(),
        }
    }
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

/// Wrapper for type, if `borrow_mut` is called, second value should be true.
pub struct RcT<T>(Rc<RefCell<MutT<T>>>);

impl<T: Debug> Debug for RcT<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Debug::fmt(&self.0.borrow().inner, f)
    }
}

impl<T> Deref for RcT<T> {
    type Target = Rc<RefCell<MutT<T>>>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Default clone all inner data.
impl<T: Encode + Decode> Clone for RcT<T> {
    fn clone(&self) -> Self {
        RcT(Rc::new(RefCell::new(RefCell::borrow(&self.0).clone())))
    }
}

impl<T: Clone> RcT<T> {
    pub fn clone_inner(&self) -> Option<T> {
        self.0.borrow().inner.clone()
    }
}

impl<T: TStorageOverlay> RawValue for RcT<T> {
    fn clone_ref(&self) -> Box<dyn RawValue> {
        Box::new(self.clone_ref())
    }

    fn clone_box(&self) -> Box<dyn RawValue> {
        Box::new(self.clone())
    }

    fn len(&self) -> usize {
        self.len()
    }

    fn muted(&self) -> bool {
        unimplemented!("no muted for no_std RcT");
    }

    fn set_muted(&mut self, _muted: bool) {
        unimplemented!("no need set_muted for no_std RcT");
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

impl<T> RcT<T> {
    pub fn new(key: &[u8], t: Option<T>) -> Self {
        RcT(Rc::new(RefCell::new(MutT { inner: t, raw: None, key: key.to_vec() })))
    }

    pub fn clear_muted(&mut self) {}

    /// Apply a closure for inner value reference.
    pub fn map<O>(&self, f: impl FnOnce(&Option<T>) -> O) -> O {
        f(&self.0.borrow().deref().inner)
    }

    pub fn clone_ref(&self) -> Self {
        RcT(self.0.clone())
    }

    pub fn into_inner(self) -> Result<Option<T>, u32> {
        let rc_count = Rc::strong_count(&self.0);
        if rc_count > 1 {
            Err(rc_count as u32)
        } else {
            Ok(Rc::into_inner(self.0).map(|r| r.into_inner().inner).unwrap_or(None))
        }
    }
}

impl<T: Default> RcT<T> {
    /// Apply a closure for inner value reference.
    pub fn map_value_query<O>(&self, f: impl FnOnce(&T) -> O) -> O {
        if let Some(value) = self.0.borrow().deref().inner.as_ref() {
            f(value)
        } else {
            f(&T::default())
        }
    }

}

impl<T: Encode + Default> RcT<T> {
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
        if mut_inner.inner.is_some() {
            let res = f(mut_inner.inner.as_mut().unwrap());
            if res.is_ok() {
                mut_inner.raw.take();
                super::unhashed::put_raw(&mut_inner.key, &mut_inner.inner.as_ref().map(|v| v.encode()).unwrap());
            }
            res
        } else {
            let mut tmp = T::default();
            let res = f(&mut tmp);
            if res.is_ok() {
                mut_inner.raw.take();
                super::unhashed::put_raw(&mut_inner.key, &tmp.encode());
                mut_inner.inner = Some(tmp);
            }
            res
        }
    }
}

impl<T: Encode> RcT<T> {
    pub fn take_value(&mut self) -> Option<T> {
        self.mutate(|v| v.take())
    }

    /// Mutate inner optional `Value` withing input closure.
    ///
    /// Notice!!!
    /// The inner `Value` is considered as changed no matter if you actually changed it.
    pub fn mutate<O>(&mut self, f: impl FnOnce(&mut Option<T>) -> O) -> O {
        self.try_mutate(|inner| Result::<O, Never>::Ok(f(inner)))
            .expect("RcT mutate is not expected to return Error")
    }

    /// Mutate inner optional `Value` withing input closure.
    ///
    /// Notice!!!
    /// The result correctness is not ensured!
    /// `DO NOT` change inner `Value` if your closure return `Error`
    pub fn try_mutate<R, E>(&mut self, f: impl FnOnce(&mut Option<T>) -> Result<R, E>) -> Result<R, E> {
        let mut mut_inner = self.0.borrow_mut();
        let res = f(&mut mut_inner.deref_mut().inner);
        if res.is_ok() {
            mut_inner.raw.take();
            if mut_inner.inner.is_some() {
                super::unhashed::put_raw(&mut_inner.key, &mut_inner.inner.as_ref().map(|v| v.encode()).unwrap());
            } else {
                super::unhashed::kill(&mut_inner.key);
            }
        }
        res
    }

    pub fn get_raw(&self, cache: bool) -> Option<Vec<u8>> {
        let mut mut_inner = self.0.borrow_mut();
        if let Some(raw_value) = mut_inner.inner.as_ref().map(|t| t.encode()) {
            if cache {
                mut_inner.raw = Some(raw_value.clone());
            }
            Some(raw_value)
        } else {
            None
        }
    }

    pub fn len(&self) -> usize {
        let mut mut_inner = self.0.borrow_mut();
        if let Some(raw) = mut_inner.raw.as_ref() {
            raw.len()
        } else {
            mut_inner.inner.as_ref().map(|t| t.encode().len()).unwrap_or_default()
        }
    }
}

impl<T: Decode> RcT<T> {
    pub fn put_raw(&self, value: Vec<u8>, cache: bool) -> Result<(), codec::Error> {
        let mut mut_inner = self.0.borrow_mut();
        // `inner` is the main data, we `MUST` update it.
        mut_inner.inner = Some(T::decode(&mut value.as_slice())?);
        super::unhashed::put_raw(&mut_inner.key, &value);
        if cache {
            mut_inner.raw = Some(value);
        }
        Ok(())
    }
}
