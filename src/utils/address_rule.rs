use crate::utils::ToAddress;
use alloy_primitives::Address;
use serde::{Deserialize, Deserializer};

use glob::{MatchOptions, Pattern};

const OPT: MatchOptions = MatchOptions {
    case_sensitive: false,
    require_literal_separator: false,
    require_literal_leading_dot: false,
};

/// When an address is None, it means any address.
#[derive(Deserialize, Debug, Clone)]
pub struct AddressRule {
    /// Should check the address if Some.
    pub from: Option<FromAddressRule>,
    /// Should check the address if Some.
    pub to: Option<ToAddressRule>,
}

/// Note: The order is important.
#[derive(Deserialize, Debug, Clone)]
#[serde(untagged)]
pub enum ToAddressRule {
    #[serde(deserialize_with = "deserialize_create")]
    Create,
    #[serde(deserialize_with = "deserialize_any_address")]
    AnyAddress,
    Address(Address),
    #[serde(deserialize_with = "deserialize_address_glob")]
    AddressGlob(Pattern),
}

/// Note: The order is important.
#[derive(Deserialize, Debug, Clone)]
#[serde(untagged)]
pub enum FromAddressRule {
    #[serde(deserialize_with = "deserialize_any_address")]
    AnyAddress,
    Address(Address),
    #[serde(deserialize_with = "deserialize_address_glob")]
    AddressGlob(Pattern),
}

impl FromAddressRule {
    pub fn satisfy(&self, addr: &Address) -> bool {
        match self {
            Self::AnyAddress => true,
            Self::Address(address) => address == addr,
            Self::AddressGlob(pattern) => pattern.matches_with(addr.to_string().as_str(), OPT),
        }
    }
}

impl ToAddressRule {
    pub fn satisfy(&self, addr: &ToAddress) -> bool {
        match (self, addr) {
            (Self::Create, ToAddress::Create) => true,
            (Self::AnyAddress, ToAddress::Call(_addr)) => true,
            (Self::Address(addr), ToAddress::Call(addr2)) => addr == addr2,
            (Self::AddressGlob(pattern), ToAddress::Call(addr)) => pattern.matches_with(addr.to_string().as_str(), OPT),
            _ => false,
        }
    }
}

impl AddressRule {
    /// Check if the address satisfied.
    pub fn satisfy(&self, from: &Address, to: &ToAddress) -> bool {
        let b = self.satisfy_from_address(from);
        let b2 = self.satisfy_to_address(to);

        b && b2
    }

    pub fn satisfy_from_address(&self, from: &Address) -> bool {
        if let Some(rule) = &self.from {
            rule.satisfy(from)
        } else {
            true
        }
    }

    pub fn satisfy_to_address(&self, to: &ToAddress) -> bool {
        if let Some(rule) = &self.to {
            rule.satisfy(to)
        } else {
            true
        }
    }
}

fn deserialize_address_glob<'de, D>(deserializer: D) -> Result<Pattern, D::Error>
where
    D: Deserializer<'de>,
{
    let s = <String>::deserialize(deserializer)?;

    Pattern::new(&s).map_err(|err| serde::de::Error::custom(format!("invalid `to` glob syntax: {}", err)))
}

fn deserialize_create<'de, D>(deserializer: D) -> Result<(), D::Error>
where
    D: Deserializer<'de>,
{
    let s = <String>::deserialize(deserializer)?;
    if &s == "create" || &s == "Create" {
        Ok(())
    } else {
        Err(serde::de::Error::custom("invalid `to` field"))
    }
}

fn deserialize_any_address<'de, D>(deserializer: D) -> Result<(), D::Error>
where
    D: Deserializer<'de>,
{
    let s = <String>::deserialize(deserializer)?;
    if &s == "any_address" || &s == "anyAddress" || &s == "any" {
        Ok(())
    } else {
        Err(serde::de::Error::custom("invalid `to` field"))
    }
}

#[allow(unused)]
fn deserialize_string_address<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    let s = <Address>::deserialize(deserializer)?;

    Ok(s.to_string())
}
