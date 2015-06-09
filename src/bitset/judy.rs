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
        let mut result = Judy1::new();
        for input in keys.iter().filter_map( |k| self.sets.get(&format!("{}-{}", set_name, k)) ) {
            for v in input.iter() {
                result.set( v );
            }
        }
        return Ok( result.iter().map(|it|it as usize).collect() );
    }

    fn intersect(&mut self, set_name: &str, keys: &[usize]) -> Result< Vec<usize>, String >
    {
        let mut result = Judy1::new();
        let mut inputs : Vec<&Judy1> = keys.iter().filter_map( |k| self.sets.get(&format!("{}-{}", set_name, k)) ).collect();
        if inputs.len() < keys.len() {
            return Ok( vec![0] )
        }

        inputs.sort_by( |a,b| a.len().cmp(&b.len()) );
        let mut iters : Vec<_> = inputs.iter().map( |i| i.iter() ).collect();

        'outer: loop {
            match iters[0].next() {
                None => break,
                Some(tgt) => {
                    'iters: for iter in iters.iter_mut().skip(1) {
                        'inner: loop {
                            match iter.next() {
                                Some(n) if n == tgt => continue,
                                Some(n) if n > tgt  => break 'iters,
                                Some(_)             => break 'inner,
                                None                => break 'outer
                            }
                        }
                        result.set(tgt);
                    }
                }
            }

        }
        return Ok( result.iter().map(|it|it as usize).collect() );
    }
}

impl JudyBitset {
    pub fn new() -> JudyBitset
    {
        return JudyBitset{ sets: HashMap::new() };
    }
}
