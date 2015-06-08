use redis::{Connection,RedisError,RedisResult,Commands};
use super::Bitset;

pub struct RedisBitset<'a>
{
    conn: & 'a Connection,
}

impl<'a> Bitset<RedisError> for RedisBitset<'a>
{
    fn add(&self, set_name: &str, key: usize, values: &[usize]) -> RedisResult< () >
    {
        let key_str = format!("{}-{}", set_name, key);
        // need let to make types explicit
        let res : RedisResult<()> = self.conn.sadd(key_str, values);
        res
    }

    fn union(&self, set_name: &str, keys: &[usize]) -> RedisResult< Vec<usize> >
    {
        let key_strs : Vec<String> = keys.iter().map( |k| format!("{}-{}", set_name, k) ).collect();
        let res : RedisResult<Vec<usize>> = self.conn.sunion( key_strs );
        res
    }

    fn intersect(&self, set_name: &str, keys: &[usize]) -> RedisResult< Vec<usize> >
    {
        let key_strs : Vec<String> = keys.iter().map( |k| format!("{}-{}", set_name, k) ).collect();
        let res : RedisResult<Vec<usize>> = self.conn.sinter( key_strs );
        res
    }
}

impl<'a> RedisBitset<'a>
{
    pub fn new(conn: &Connection) -> RedisBitset
    {
        return RedisBitset{ conn: conn }
    }
}
