#[macro_use] extern crate log;
extern crate redis;
extern crate rustc_serialize;
use redis::{RedisResult, Connection, Commands};
use rustc_serialize::json;
use std::env;
use std::io::prelude::*;
use std::fs;
use std::fs::File;
use std::path::Path;
use std::io;

#[derive(RustcDecodable)]
#[allow(dead_code)]
struct Track
{
    artist: String,
    timestamp: String,
    similars: Vec<(String, f64)>,
    tags: Vec<(String, u8)>,
    track_id: String,
    title: String
}


fn redis_connection() -> RedisResult<Connection>
{
    let client = try!(redis::Client::open("redis://127.0.0.1/"));
    return client.get_connection();
}

fn get_or_incr( conn: &Connection, key: &str, counter_key: &str ) -> u64
{
    match conn.get(key) {
        Ok(v) => v,
        Err(_) => {
            let id = conn.incr(counter_key, 1).unwrap();
            let _ : () = conn.set(key, id).unwrap();
            id
        }
    }
}

fn load_track(conn: &Connection, filename: &str)
{
    let mut f = File::open(filename).unwrap();
    let mut s = String::new();
    f.read_to_string(&mut s).ok();
    let track: Track = json::decode(&s).unwrap();
    let int_track_id = get_or_incr(&conn, &track.track_id, "last_track_id");
    let track_key = format!("track-tags-{}", int_track_id);
    debug!( "Artist: {}, title: {}", track.artist, track.title );
    for tag in track.tags {
        let int_tag_id = get_or_incr(&conn, &tag.0, "last_tag_id");
        let tag_key = format!("tag-tracks-{}", int_tag_id);
        let _ : () = conn.sadd( &track_key as &str, int_tag_id ).unwrap();
        let _ : () = conn.sadd( &tag_key as &str, int_track_id ).unwrap();
    }
}

fn walk_dir<F>(path: &Path, cb: &mut F) -> io::Result<()>
    where F : Fn(&Path) -> ()
{
    let meta = fs::metadata(path).unwrap();
    if meta.is_dir() {
        for entry in try!(fs::read_dir(path)) {
            let entry = try!(entry);
            try!(walk_dir(entry.path().as_path(), cb));
        }
    } else if meta.is_file() {
        cb(&path);
    }
    Ok(())
}

fn list_tags(conn: &Connection, max: usize)
{
    println!("Listing {} tags", if max > 0 { format!("top {}", max) } else { "all".into() } );

    let tags : Vec<String> = redis::cmd("KEYS").arg("tag-tracks-*").query(conn).unwrap();
    let mut tags_counts : Vec<(&str, usize)> = tags.iter().map( |t| { let c:usize = conn.scard(&t as &str).unwrap(); (t as &str, c) } ).collect();
    tags_counts.sort_by(|a,b| b.1.cmp(&a.1)); // sort descending
    for (i, &(t,c)) in tags_counts.iter().enumerate() {
        if max > 0 && i > max {
            break;
        } else {
            println!("{} {}", t, c);
        }
    }

}

fn usage(program_name: &str)
{
    println!("Usage:");
    println!("\t{} <command> <args>", program_name);
    println!("");
    println!("Commands:");
    println!("\t{:20} {}", "import PATH...", "import tracks");
    println!("\t{:20} {}", "tags [N]", "list N biggest tags (if omitted, list all)");
}

fn main()
{
    let conn = redis_connection().unwrap();
    let mut args = env::args();
    let prog_name = args.next().unwrap();
    match args.next().unwrap_or("".into()).as_ref() {
        "import" => {
            for arg in args {
                debug!("Reading {}", arg);
                walk_dir( Path::new(&arg), &mut |f| load_track(&conn, f.to_str().unwrap()) ).ok().expect("Error reading path");
            }
        },
        "tags" => {
            let max = args.next().and_then(|v|v.parse().ok()).unwrap_or(0);
            list_tags(&conn, max);
        },
        _ => usage(&prog_name)
    }

}
