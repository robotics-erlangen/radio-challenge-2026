use std::borrow::Borrow;
use std::collections::HashMap;
use std::hash::Hash;

/// A map where each value can be retrieved by two independent keys.
/// Lookups for a primary key are faster than those for a secondary key.
#[derive(Debug, Clone)]
pub struct DualHashMap<K1, K2, V> {
    sec_to_prim: HashMap<K2, K1>,
    prim_to_val: HashMap<K1, (K2, V)>,
}

impl<K1, K2, V> Default for DualHashMap<K1, K2, V>
where
    K1: Eq + Hash + Clone,
    K2: Eq + Hash + Clone,
{
    fn default() -> Self {
        Self::new()
    }
}

#[allow(dead_code)]
impl<K1, K2, V> DualHashMap<K1, K2, V>
where
    K1: Eq + Hash + Clone,
    K2: Eq + Hash + Clone,
{
    pub fn new() -> Self {
        Self {
            sec_to_prim: HashMap::new(),
            prim_to_val: HashMap::new(),
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            sec_to_prim: HashMap::with_capacity(capacity),
            prim_to_val: HashMap::with_capacity(capacity),
        }
    }

    /// Inserts a value mapped to both `k1` and `k2`.
    /// If either key already exists, their respective previous entries are removed.
    pub fn insert(&mut self, k1: K1, k2: K2, value: V) {
        // Remove any related old entries from both maps. Just inserting into both maps could leave orphaned entries.
        self.remove_prim(&k1);
        self.remove_sec(&k2);

        self.sec_to_prim.insert(k2.clone(), k1.clone());
        self.prim_to_val.insert(k1, (k2, value));
    }

    /// Returns a reference to the value corresponding to the primary key `k1`.
    pub fn get_prim<Q>(&self, k1: &Q) -> Option<(&K2, &V)>
    where
        K1: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.prim_to_val.get(k1).map(|(k2, v)| (k2, v))
    }

    /// Returns a reference to the value corresponding to the secondary key `k2`.
    pub fn get_sec<Q>(&self, k2: &Q) -> Option<(&K1, &V)>
    where
        K2: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let k1 = self.sec_to_prim.get(k2)?;
        self.prim_to_val.get(k1).map(|(_, v)| (k1, v))
    }

    /// Returns a mutable reference to the value corresponding to the primary key `k1`.
    pub fn get_prim_mut<Q>(&mut self, k1: &Q) -> Option<(&K2, &mut V)>
    where
        K1: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.prim_to_val.get_mut(k1).map(|(k2, v)| (&*k2, v))
    }

    /// Returns a mutable reference to the value corresponding to the secondary key `k2`.
    pub fn get_sec_mut<Q>(&mut self, k2: &Q) -> Option<(&K1, &mut V)>
    where
        K2: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let k1 = self.sec_to_prim.get(k2)?;
        self.prim_to_val.get_mut(k1).map(|(_, v)| (k1, v))
    }

    /// Removes the entry corresponding to the primary key `k1`, returning its previous value and secondary key.
    pub fn remove_prim<Q>(&mut self, k1: &Q) -> Option<(K2, V)>
    where
        K1: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        if let Some((k2, v)) = self.prim_to_val.remove(k1) {
            self.sec_to_prim.remove(&k2);
            return Some((k2, v));
        }
        None
    }

    /// Removes the entry corresponding to the secondary key `k2`, returning its previous value and primary key.
    pub fn remove_sec<Q>(&mut self, k2: &Q) -> Option<(K1, V)>
    where
        K2: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        if let Some(k1) = self.sec_to_prim.remove(k2)
            && let Some((_, v)) = self.prim_to_val.remove(&k1)
        {
            return Some((k1, v));
        }
        None
    }

    /// Retains only the elements specified by the predicate.
    pub fn retain<F>(&mut self, mut f: F)
    where
        F: FnMut(&K1, &K2, &mut V) -> bool,
    {
        self.prim_to_val.retain(|k1, (k2, v)| {
            let keep = f(k1, k2, v);
            if !keep {
                self.sec_to_prim.remove(k2);
            }
            keep
        });
    }

    /// Returns `true` if the map contains a value for the specified primary key `k1`.
    pub fn contains_prim<Q>(&self, k1: &Q) -> bool
    where
        K1: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.prim_to_val.contains_key(k1)
    }

    /// Returns `true` if the map contains a value for the secondary key `k2`.
    pub fn contains_sec<Q>(&self, k2: &Q) -> bool
    where
        K2: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.sec_to_prim.contains_key(k2)
    }

    /// Returns the number of elements in the map.
    pub fn len(&self) -> usize {
        self.prim_to_val.len()
    }

    /// Returns `true` if the map contains no elements.
    pub fn is_empty(&self) -> bool {
        self.prim_to_val.is_empty()
    }

    /// Clears the map, removing all entries.
    pub fn clear(&mut self) {
        self.sec_to_prim.clear();
        self.prim_to_val.clear();
    }

    // ==========================================

    /// An iterator visiting all entries `(&K1, &K2, &V)` in arbitrary order.
    pub fn iter(&self) -> impl Iterator<Item = (&'_ K1, &'_ K2, &'_ V)> {
        self.prim_to_val.iter().map(|(k1, (k2, v))| (k1, k2, v))
    }

    /// An iterator visiting all entries `(&K1, &K2, &mut V)` in arbitrary order, with a mutable reference to the value.
    pub fn iter_mut(&mut self) -> impl Iterator<Item = (&'_ K1, &'_ K2, &'_ mut V)> {
        // The reborrow is needed because the keys can only be modified through insert/remove
        self.prim_to_val
            .iter_mut()
            .map(|(k1, (k2, v))| (k1, &*k2, v))
    }

    /// An iterator visiting all key pairs `(&K1, &K2)` in arbitrary order.
    pub fn keys(&self) -> impl Iterator<Item = (&'_ K1, &'_ K2)> {
        self.prim_to_val.iter().map(|(k1, (k2, _))| (k1, k2))
    }

    /// An iterator visiting all values in arbitrary order.
    pub fn values(&self) -> impl Iterator<Item = &'_ V> {
        self.prim_to_val.iter().map(|(_k1, (_k2, v))| v)
    }

    /// An iterator visiting all values mutably in arbitrary order.
    pub fn values_mut(&mut self) -> impl Iterator<Item = &'_ mut V> {
        self.prim_to_val.iter_mut().map(|(_k1, (_k2, v))| v)
    }
}
