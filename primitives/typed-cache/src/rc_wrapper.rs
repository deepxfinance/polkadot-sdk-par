use sp_std::fmt::{self, Debug};
use sp_std::ops::{Deref, DerefMut};
use sp_std::cell::RefCell;
use sp_std::rc::Rc;

#[derive(Clone)]
pub struct MutT<T> {
    inner: T,
    muted: bool,
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
    pub fn new(t: T, muted: bool) -> Self {
        RcT(Rc::new(RefCell::new(MutT { inner: t, muted })))
    }

    /// Mutate inner `Value` withing input closure.
    /// The inner value `SHOULD` be changed in your closure.
    pub fn muted(&self) -> bool {
        self.0.borrow().muted
    }

    /// Mutate inner `Value` withing input closure.
    /// Notice!!! The result correctness is not ensured!
    /// `DO NOT` change inner `Value` if your closure return `Error`
    pub fn mutate<O>(&mut self, f: impl FnOnce(&mut T) -> O) -> O {
        self.try_mutate(|inner| Result::<O, Never>::Ok(f(inner)))
            .expect("Typed RcT mutate is not expected to return Error")
    }

    pub fn try_mutate<R, E>(&mut self, f: impl FnOnce(&mut T) -> Result<R, E>) -> Result<R, E> {
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
            Ok(Rc::into_inner(self.0).map(|r| r.into_inner().inner))
        }
    }
}

impl<T: Clone> RcT<T> {
    pub fn clone_inner(&self) -> T {
        self.0.borrow().inner.clone()
    }
}
