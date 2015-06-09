use redis::{Connection,RedisError,RedisResult,Commands};
use super::Bitset;

pub struct RedisBitset<'a>
{
    conn: & 'a Connection,
}

fn key_str(set_name: &str, key: usize) -> String
{
    return format!("{}-{}", set_name, key);
}

fn key_strs(set_name: &str, keys: &[usize]) -> Vec<String>
{
    return keys.iter().map( |k| format!("{}-{}", set_name, k) ).collect();
}

impl<'a> Bitset<RedisError> for RedisBitset<'a>
{
    fn add(&mut self, set_name: &str, key: usize, values: &[usize]) -> RedisResult< () >
    {
        return self.conn.sadd(key_str(set_name,key), values);
    }

    fn union(&mut self, set_name: &str, keys: &[usize]) -> RedisResult< Vec<usize> >
    {
        return self.conn.sunion( key_strs(set_name, keys) );
    }

    fn intersect(&mut self, set_name: &str, keys: &[usize]) -> RedisResult< Vec<usize> >
    {
        return self.conn.sinter( key_strs(set_name, keys) );
    }

    fn list(&mut self, set_name: &str, key: usize) -> RedisResult<Vec<usize>>
    {
        return self.conn.smembers( key_str(set_name, key) );
    }
}

impl<'a> RedisBitset<'a>
{
    pub fn new(conn: &Connection) -> RedisBitset
    {
        return RedisBitset{ conn: conn }
    }
}
