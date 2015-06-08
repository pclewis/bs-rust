use std::error::Error;

pub trait Bitset<T: Error>
{
    fn add(&self, set_name: &str, key: usize, values: &[usize]) -> Result< (), T >;
    fn union(&self, set_name: &str, keys: &[usize]) -> Result< Vec<usize>, T >;
    fn intersect(&self, set_name: &str, keys: &[usize]) -> Result< Vec<usize>, T >;
}


mod redis;
pub use self::redis::RedisBitset;
