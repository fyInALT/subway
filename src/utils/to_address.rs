use crate::extensions::whitelist::WhiteAddress;
use alloy_primitives::{Address, TxKind};
use serde::{Deserialize, Deserializer};
use std::fmt::Display;

/// The normalized `to` address:
/// - Create: this call is contract deploy.
/// - Call: this call is contract call.
///
/// Note: this type is similar to [`TxKind`] but different in serde parts.
#[derive(Deserialize, Debug, Clone, Eq, PartialEq)]
#[serde(untagged)]
pub enum ToAddress {
    #[serde(deserialize_with = "deserialize_create")]
    Create,
    Call(Address),
    // TODO: enable any call but disable create.
    // AnyCall,
}

/// Helper function to deserialize boxed blobs
fn deserialize_create<'de, D>(deserializer: D) -> Result<(), D::Error>
where
    D: Deserializer<'de>,
{
    let s = <String>::deserialize(deserializer)?;
    if &s == "create" || &s == "Create" {
        Ok(())
    } else {
        Err(serde::de::Error::custom("invalid `to` address"))
    }
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
        Self::Call(*to)
    }
}

impl From<Address> for ToAddress {
    fn from(to: Address) -> Self {
        Self::from(&to)
    }
}

impl WhiteAddress {
    /// Check if this is a white address.
    pub fn satisfy(&self, from: &Address, to: &ToAddress) -> bool {
        self.satisfy_from_address(from) && self.satisfy_to_address(to)
    }

    pub fn satisfy_from_address(&self, from: &Address) -> bool {
        self.from.is_none() || self.from.as_ref() == Some(from)
    }

    pub fn satisfy_to_address(&self, to: &ToAddress) -> bool {
        self.to.is_none() || self.to.as_ref() == Some(to)
    }
}
