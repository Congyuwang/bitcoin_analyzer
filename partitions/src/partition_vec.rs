//! A [disjoint-sets/union-find] implementation of a vector partitioned in sets.
//!
//! [disjoint-sets/union-find]: https://en.wikipedia.org/wiki/Disjoint-set_data_structure

use metadata::Metadata;
use std::cmp::Ordering;
use std::fmt::Formatter;

#[derive(Clone)]
pub struct PartitionVec<T> {
    data: Vec<T>,
    meta: Vec<Metadata>,
}

impl<T> PartitionVec<T> {
    #[inline]
    pub fn new() -> Self {
        Self {
            data: Vec::new(),
            meta: Vec::new(),
        }
    }

    #[inline]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            data: Vec::with_capacity(capacity),
            meta: Vec::with_capacity(capacity),
        }
    }

    pub fn union(&mut self, first_index: usize, second_index: usize) {
        let i = self.find(first_index);
        let j = self.find(second_index);

        if i == j {
            return;
        }

        // We swap the values of the links.
        let link_i = self.meta[i].link();
        let link_j = self.meta[j].link();
        self.meta[i].set_link(link_j);
        self.meta[j].set_link(link_i);

        // We add to the tree with the highest rank.
        match Ord::cmp(&self.meta[i].rank(), &self.meta[j].rank()) {
            Ordering::Less => {
                self.meta[i].set_parent(j);
            }
            Ordering::Equal => {
                // We add the first tree to the second tree.
                self.meta[i].set_parent(j);
                // The second tree becomes larger.
                let old_rank = self.meta[j].rank().clone();
                self.meta[j].set_rank(old_rank + 1);
            }
            Ordering::Greater => {
                self.meta[j].set_parent(i);
            }
        }
    }

    #[inline]
    pub fn is_singleton(&self, index: usize) -> bool {
        self.meta[index].link() == index
    }

    pub fn find(&mut self, index: usize) -> usize {
        // If the node is its own parent we have found the root.
        if self.meta[index].parent() == index {
            index
        } else {
            // This method is recursive so each parent on the way to the root is updated.
            let root = self.find(self.meta[index].parent());

            // We update the parent to the root for a lower tree.
            self.meta[index].set_parent(root);

            root
        }
    }

    #[inline]
    pub fn find_final(&self, mut index: usize) -> usize {
        while index != self.meta[index].parent() {
            index = self.meta[index].parent();
        }
        index
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        usize::min(self.data.capacity(), self.meta.capacity())
    }

    #[inline]
    pub fn push(&mut self, elem: T) {
        let old_len = self.len();

        self.data.push(elem);
        self.meta.push(Metadata::new(old_len));
    }


    #[inline]
    pub fn reserve(&mut self, additional: usize) {
        self.data.reserve(additional);
        self.meta.reserve(additional);
    }

    #[inline]
    pub fn shrink_to_fit(&mut self) {
        self.data.shrink_to_fit();
        self.meta.shrink_to_fit();
    }

    #[inline]
    pub fn clear(&mut self) {
        self.data.clear();
        self.meta.clear();
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

}

impl<T> std::fmt::Debug for PartitionVec<T> where T: std::fmt::Debug {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut builder = f.debug_list();
        builder.entry(&format_args!("PartitionVec of length {}", &self.len()));
        builder.finish()
    }
}
