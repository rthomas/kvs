#![deny(missing_docs)]

//! An in-memory Key Value store.

use std::collections::HashMap;

/// In-memory string based Key-Value store
#[derive(Default)]
pub struct KvStore {
    store: HashMap<String, String>,
}

impl KvStore {
    /// Create a new KvStore instance.
    pub fn new() -> KvStore {
        KvStore {
            store: HashMap::new(),
        }
    }

    /// Get the value associated with the provided key, or None otherwise.
    pub fn get(&self, key: String) -> Option<String> {
        match self.store.get(&key) {
            Some(s) => Some(s.to_owned()),
            None => None,
        }
    }

    /// Set a value for a given key, overriding a previously set value if it exists.
    pub fn set(&mut self, key: String, val: String) {
        self.store.insert(key, val);
    }

    /// Remnove a key and value from the store.
    pub fn remove(&mut self, key: String) {
        self.store.remove(&key);
    }
}
