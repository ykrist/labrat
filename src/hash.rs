use siphasher::sip::SipHasher;
use std::hash::{Hasher, Hash};

trait ConsistentHash {
    fn compute_hash(&self) -> u64 {
        let mut hasher = SipHasher::new_with_keys(0xdeadbeef, 0xf00dbabe);
        self.write(&mut hasher);
        hasher.finish()
    }

    fn write(&self, hasher: &mut SipHasher);
}

impl<T: Hash> ConsistentHash for T {
    fn write(&self, hasher: &mut SipHasher) {
        self.hash(hasher)
    }
}

impl ConsistentHash for f64 {
    fn write(&self, hasher: &mut SipHasher) {
        self.to_ne_bytes().hash(hasher)
    }
}
