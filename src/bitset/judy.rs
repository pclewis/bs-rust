use super::Bitset;
extern crate judy;
use self::judy::Judy1;
use std::collections::HashMap;

pub struct JudyBitset
{
    sets : HashMap< String, Judy1 >,
}

impl Bitset<String> for JudyBitset
{
    fn add(&mut self, set_name: &str, key: usize, values: &[usize]) -> Result< (), String >
    {
        let key_name = format!("{}-{}", set_name, key);
        if !self.sets.contains_key(&key_name) {
            self.sets.insert( key_name.to_owned(), Judy1::new() );
        }
        let mut j = self.sets.get_mut(&key_name).unwrap();
        for v in values {
            j.set(*v as u64);
        }
        return Ok( () );
    }
    fn union(&mut self, set_name: &str, keys: &[usize]) -> Result< Vec<usize>, String >
    {
        unimplemented!();
    }
    fn intersect(&mut self, set_name: &str, keys: &[usize]) -> Result< Vec<usize>, String >
    {
        unimplemented!();
    }
}

impl JudyBitset {
    fn new() -> JudyBitset
    {
        return JudyBitset{ sets: HashMap::new() };
    }
}
