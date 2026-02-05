use codec::Encode;
use sp_std::fmt::{self, Debug};
use sp_std::ops::{Deref, DerefMut};
use sp_std::cell::RefCell;
use sp_std::rc::Rc;
use sp_std::vec::Vec;
use crate::Never;

#[derive(Clone)]
pub struct MutT<T> {
    inner: T,
    key: Vec<u8>,
}

impl<T: Debug> Debug for MutT<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Debug::fmt(&self.inner, f)
    }
}

impl<T> Deref for MutT<T> {
    type Target = T;
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
impl<T: Clone> Clone for RcT<T> {
    fn clone(&self) -> Self {
        RcT(Rc::new(RefCell::new(self.0.borrow().clone())))
    }
}

impl<T: Clone> RcT<T> {
    pub fn clone_inner(&self) -> T {
        self.0.borrow().inner.clone()
    }
}

impl<T> RcT<T> {
    pub fn new(key: &[u8], t: T) -> Self {
        RcT(Rc::new(RefCell::new(MutT { inner: t, key: key.to_vec() })))
    }

    pub fn clone_ref(&self) -> Self {
        RcT(self.0.clone())
    }

    pub fn into_inner(self) -> Result<Option<T>, u32> {
        let rc_count = Rc::strong_count(&self.0);
        if rc_count > 1 {
            Err(rc_count as u32)
        } else {
            Ok(Rc::into_inner(self.0).map(|r| r.into_inner().inner))
        }
    }
}

impl<T: Encode + Debug> RcT<Option<T>> {
    /// Mutate inner `Value` withing input closure.
    /// The inner value `SHOULD` be changed in your closure.
    pub fn mutate<O>(&mut self, f: impl FnOnce(&mut Option<T>) -> O) -> O {
        self.try_mutate(|inner| Result::<O, Never>::Ok(f(inner)))
            .expect("RcT mutate is not expected to return Error")
    }

    /// Mutate inner `Value` withing input closure.
    /// Notice!!! The result correctness is not ensured!
    /// `DO NOT` change `Value` if your closure return `Error`
    pub fn try_mutate<R, E>(&mut self, f: impl FnOnce(&mut Option<T>) -> Result<R, E>) -> Result<R, E> {
        let mut mut_inner = self.0.borrow_mut();
        let res = f(&mut mut_inner.deref_mut().inner);
        if res.is_ok() {
            if mut_inner.inner.is_some() {
                super::unhashed::put_raw(&mut_inner.key, &mut_inner.inner.as_ref().map(|v| v.encode()).unwrap());
            } else {
                super::unhashed::kill(&mut_inner.key);
            }
        }
        res
    }
}
