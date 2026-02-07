use sp_std::fmt::{self, Debug};
use sp_std::ops::{Deref, DerefMut};
use sp_std::cell::RefCell;
use sp_std::rc::Rc;

#[derive(Clone)]
pub struct MutT<T> {
    inner: Option<T>,
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

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct Never;

impl<T> RcT<T> {
    pub fn new(t: Option<T>, muted: bool) -> Self {
        RcT(Rc::new(RefCell::new(MutT { inner: t, muted })))
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
        self.0.borrow().muted
    }

    /// Apply a closure for inner value reference.
    pub fn map<O>(&self, f: impl FnOnce(&Option<T>) -> O) -> O {
        f(&self.0.borrow().deref().inner)
    }

    /// Mutate inner optional `Value` withing input closure.
    ///
    /// Notice!!!
    /// The inner `Value` is considered as changed no matter if you actually changed it.
    pub fn mutate<O>(&mut self, f: impl FnOnce(&mut Option<T>) -> O) -> O {
        self.try_mutate(|inner| Result::<O, Never>::Ok(f(inner)))
            .expect("Typed RcT mutate is not expected to return Error")
    }

    /// Mutate inner optional `Value` withing input closure.
    ///
    /// Notice!!!
    /// The result correctness is not ensured!
    /// `DO NOT` change inner `Value` if your closure return `Error`
    pub fn try_mutate<R, E>(&mut self, f: impl FnOnce(&mut Option<T>) -> Result<R, E>) -> Result<R, E> {
        let mut mut_inner = self.0.borrow_mut();
        let res = f(&mut mut_inner.deref_mut().inner);
        mut_inner.muted = res.is_ok();
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
        if let Some(value) = self.0.borrow().deref().inner.as_ref() {
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

        mut_inner.muted = res.is_ok();
        res
    }
}

impl<T: Clone> RcT<T> {
    pub fn clone_inner(&self) -> Option<T> {
        self.0.borrow().inner.clone()
    }
}
