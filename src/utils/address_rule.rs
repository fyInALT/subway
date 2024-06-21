use crate::utils::ToAddress;
use alloy_primitives::Address;
use serde::Deserialize;

/// When an address is None, it means any address.
#[derive(Deserialize, Debug, Clone)]
pub struct AddressRule {
    /// Should check the address if Some.
    pub from: Option<Address>,
    /// Should check the address if Some.
    pub to: Option<ToAddress>,
}

impl AddressRule {
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
