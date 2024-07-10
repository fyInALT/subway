use alloy_primitives::{Address, TxKind};
use std::fmt::Display;

// TODO: maybe it's more quick to use hex address for compare.
/// Note: this type is similar to [`TxKind`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ToAddress {
    Create,
    Call(Address),
}

impl Display for ToAddress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Create => write!(f, "create"),
            Self::Call(s) => write!(f, "{}", s),
        }
    }
}

impl From<TxKind> for ToAddress {
    fn from(tx: TxKind) -> Self {
        Self::from(&tx)
    }
}

impl From<&TxKind> for ToAddress {
    fn from(tx: &TxKind) -> Self {
        match tx {
            TxKind::Call(addr) => Self::Call(*addr),
            TxKind::Create => Self::Create,
        }
    }
}

impl From<&Address> for ToAddress {
    fn from(to: &Address) -> Self {
        Self::from(*to)
    }
}

impl From<Address> for ToAddress {
    fn from(to: Address) -> Self {
        Self::Call(to)
    }
}
