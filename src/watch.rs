//! Immutable hot-path representation of configured storage coordinates.

use std::{collections::BTreeMap, sync::Arc};

use alloy_primitives::{Address, B256, U256, map::HashMap};
use smallvec::SmallVec;

use crate::config::WatchConfig;

const SLOT_HASH_INDEX_THRESHOLD: usize = 8;

/// Monotonic identifier of one atomically activated watch set.
pub type Generation = u64;

/// Dense dictionary entry advertised to consumers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WatchKey {
    /// Dense index into a projection's values array.
    pub key_id: u32,
    /// Stable operator-defined name.
    pub id: Arc<str>,
    /// Ethereum account address.
    pub address: Address,
    /// Physical storage key.
    pub slot: B256,
}

/// Slot lookup grouped under a single address.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SlotWatch {
    /// Physical storage key.
    pub slot: U256,
    /// Projection index to update when the slot changes.
    pub key_id: u32,
}

/// All watched slots belonging to one account.
#[derive(Clone, Debug)]
pub struct AddressWatch {
    /// Account address.
    pub address: Address,
    /// Slots sorted by physical key for deterministic layout.
    pub slots: Box<[SlotWatch]>,
    /// Fast reverse lookup for large per-account watch sets; small sets use binary search.
    pub slot_to_key: HashMap<U256, u32>,
}

/// Immutable configuration used directly by block validation.
#[derive(Clone, Debug)]
pub struct WatchSet {
    generation: Generation,
    keys: Box<[WatchKey]>,
    addresses: Box<[AddressWatch]>,
    address_to_index: HashMap<Address, usize>,
}

impl WatchSet {
    /// Compiles user configuration into a dense dictionary and address-grouped lookup table.
    pub fn compile(generation: Generation, configured: &[WatchConfig]) -> Self {
        let keys: Vec<_> = configured
            .iter()
            .enumerate()
            .map(|(index, key)| WatchKey {
                key_id: index as u32,
                id: Arc::from(key.id.as_str()),
                address: key.address,
                slot: key.slot,
            })
            .collect();

        let mut grouped = BTreeMap::<Address, Vec<SlotWatch>>::new();
        for key in &keys {
            grouped.entry(key.address).or_default().push(SlotWatch {
                slot: U256::from_be_slice(key.slot.as_slice()),
                key_id: key.key_id,
            });
        }
        let addresses = grouped
            .into_iter()
            .map(|(address, mut slots)| {
                slots.sort_unstable_by_key(|slot| slot.slot);
                let slot_to_key = if slots.len() > SLOT_HASH_INDEX_THRESHOLD {
                    slots.iter().map(|slot| (slot.slot, slot.key_id)).collect()
                } else {
                    HashMap::default()
                };
                AddressWatch {
                    address,
                    slots: slots.into_boxed_slice(),
                    slot_to_key,
                }
            })
            .collect::<Vec<_>>();
        let address_to_index = addresses
            .iter()
            .enumerate()
            .map(|(index, watch)| (watch.address, index))
            .collect();

        Self {
            generation,
            keys: keys.into_boxed_slice(),
            addresses: addresses.into_boxed_slice(),
            address_to_index,
        }
    }

    /// Active configuration generation.
    #[inline]
    pub const fn generation(&self) -> Generation {
        self.generation
    }

    /// Dictionary in projection order.
    #[inline]
    pub fn keys(&self) -> &[WatchKey] {
        &self.keys
    }

    /// Address-grouped lookup table used on the validation thread.
    #[inline]
    pub fn addresses(&self) -> &[AddressWatch] {
        &self.addresses
    }

    /// Returns the configured slots for `address`, if that account is watched.
    #[inline]
    pub fn address(&self, address: &Address) -> Option<&AddressWatch> {
        self.address_to_index
            .get(address)
            .map(|&index| &self.addresses[index])
    }

    /// Number of values in every complete projection.
    #[inline]
    pub fn len(&self) -> usize {
        self.keys.len()
    }

    /// Returns true when no values are configured.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    /// Returns whether `configured` describes this exact dictionary in the same dense order.
    ///
    /// Generation is deliberately ignored: this is used to suppress no-op config reloads before
    /// allocating a new generation.
    pub fn matches_config(&self, configured: &[WatchConfig]) -> bool {
        self.keys.len() == configured.len()
            && self
                .keys
                .iter()
                .zip(configured)
                .all(|(active, configured)| {
                    active.id.as_ref() == configured.id.as_str()
                        && active.address == configured.address
                        && active.slot == configured.slot
                })
    }
}

/// Absolute post-state update for one dense projection key.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SlotChange {
    /// Dense projection index.
    pub key_id: u32,
    /// Candidate post-state value.
    pub new_value: U256,
}

/// Stack-backed container for the overwhelmingly common small update set.
pub type BlockChanges = SmallVec<[SlotChange; 4]>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compiles_dense_keys_and_groups_addresses() {
        let a = Address::with_last_byte(1);
        let b = Address::with_last_byte(2);
        let set = WatchSet::compile(
            7,
            &[
                WatchConfig {
                    id: "a.2".into(),
                    address: a,
                    slot: B256::with_last_byte(2),
                },
                WatchConfig {
                    id: "b.1".into(),
                    address: b,
                    slot: B256::with_last_byte(1),
                },
                WatchConfig {
                    id: "a.1".into(),
                    address: a,
                    slot: B256::with_last_byte(1),
                },
            ],
        );

        assert_eq!(set.generation(), 7);
        assert_eq!(set.keys()[2].id.as_ref(), "a.1");
        assert_eq!(set.addresses().len(), 2);
        assert_eq!(set.addresses()[0].slots[0].key_id, 2);
        assert_eq!(set.addresses()[0].slots[1].key_id, 0);
    }

    #[test]
    fn matches_only_the_same_dictionary_and_order() {
        let configured = [
            WatchConfig {
                id: "first".into(),
                address: Address::with_last_byte(1),
                slot: B256::with_last_byte(1),
            },
            WatchConfig {
                id: "second".into(),
                address: Address::with_last_byte(2),
                slot: B256::with_last_byte(2),
            },
        ];
        let set = WatchSet::compile(7, &configured);

        assert!(set.matches_config(&configured));

        let mut reordered = configured.clone();
        reordered.swap(0, 1);
        assert!(!set.matches_config(&reordered));

        let mut renamed = configured.clone();
        renamed[0].id = "renamed".into();
        assert!(!set.matches_config(&renamed));
    }
}
