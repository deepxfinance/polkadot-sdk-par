pub trait TStorage: Clone + 'static {}

impl<S: Clone + 'static> TStorage for S {}

#[cfg(not(feature = "sync"))]
pub trait TStorageOverlay: TStorage + codec::FullCodec {}

#[cfg(not(feature = "sync"))]
impl<S: TStorage + codec::FullCodec> TStorageOverlay for S {}

#[cfg(feature = "sync")]
pub trait TStorageOverlay: TStorage + codec::FullCodec + Sync + Send {}

#[cfg(feature = "sync")]
impl<S: TStorage + codec::FullCodec + Sync + Send> TStorageOverlay for S {}

/// Trait for value transfer.
pub trait QueryTransfer<V> {
    type Query;

    /// Convert an optional value retrieved from storage to the type queried.
    fn from_optional_value_to_query(v: Option<V>) -> Self::Query;

    /// Convert an optional value retrieved from storage to the type queried.
    fn mut_from_optional_value_to_query<M, R, E>(v: &mut Option<V>, m: M) -> (Result<R, E>, Option<V>)
    where
        M: FnOnce(&mut Self::Query) -> Result<R, E>;

    /// Convert a query to an optional value into storage.
    fn from_query_to_optional_value(v: Self::Query) -> Option<V>;
}

pub struct OptionQT;

impl<V> QueryTransfer<V> for OptionQT {
    type Query = Option<V>;
    fn from_optional_value_to_query(v: Option<V>) -> Self::Query {
        v
    }

    fn mut_from_optional_value_to_query<M, R, E>(v: &mut Option<V>, m: M) -> (Result<R, E>, Option<V>)
    where
        M: FnOnce(&mut Self::Query) -> Result<R, E>
    {
        (m(v), None)
    }

    fn from_query_to_optional_value(v: Self::Query) -> Option<V> {
        v
    }
}

impl<V> QueryTransfer<V> for () {
    type Query = V;

    fn from_optional_value_to_query(v: Option<V>) -> Self::Query {
        v.expect("Default QueryTransfer should not be None")
    }

    fn mut_from_optional_value_to_query<M, R, E>(v: &mut Option<V>, m: M) -> (Result<R, E>, Option<V>)
    where
        M: FnOnce(&mut Self::Query) -> Result<R, E>
    {
        match v {
            Some(v) => (m(v), None),
            None => panic!("Default QueryTransfer should not be None"),
        }
    }

    fn from_query_to_optional_value(v: Self::Query) -> Option<V> {
        Some(v)
    }
}
