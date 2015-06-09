pub trait Bitset<T>
{
    fn add(&mut self, set_name: &str, key: usize, values: &[usize]) -> Result< (), T >;
    fn union(&mut self, set_name: &str, keys: &[usize]) -> Result< Vec<usize>, T >;
    fn intersect(&mut self, set_name: &str, keys: &[usize]) -> Result< Vec<usize>, T >;
    fn list(&mut self, set_name: &str, key: usize) -> Result< Vec<usize>, T >;
}


mod redis;
pub use self::redis::RedisBitset;

mod judy;
pub use self::judy::JudyBitset;
