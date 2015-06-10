#[macro_use] extern crate log;
extern crate env_logger;
extern crate redis;
extern crate rustc_serialize;
extern crate time;
extern crate term;
use redis::{RedisResult,  Connection, Commands};
use rustc_serialize::json;
use std::env;
use std::io::prelude::*;
use std::fs;
use std::fs::File;
use std::path::Path;
use std::io;
use std::str;
use std::collections::{HashMap,HashSet};
use term::Terminal;
mod bitset;
use self::bitset::{Bitset,RedisBitset,JudyBitset};

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

#[derive(RustcDecodable)]
#[derive(RustcEncodable)]
struct AllTracks
{
    track_ids: Vec<String>,
    tag_names: Vec<String>,
    track_tags: Vec<HashSet<u64>>,
    tag_tracks: Vec<HashSet<u64>>
}

struct AllTrackBuilder
{
    track_ids: HashMap<String, u64>,
    next_track_int: u64,
    tag_names: HashMap<String, u64>,
    next_tag_int: u64,
    all_tracks: AllTracks
}

fn get_or_incr2( id_map: &mut HashMap<String, u64>,
    set_list: &mut Vec<HashSet<u64>>,
    id_list: &mut Vec<String>,
    id: &str,
    next_int: &mut u64) -> u64
{
    match id_map.get(id) {
        Some(&v) => return v,
        None => {
            let v = *next_int;
            *next_int += 1;
            id_map.insert(id.to_owned(), v);
            let set = HashSet::new();
            set_list.push(set);
            id_list.push( id.to_owned() );
            return v;
        }
    }
}

fn load_track2(atb: &mut AllTrackBuilder, filename: &str)
{
    debug!("Reading {}", filename);
    let mut f = File::open(filename).unwrap();
    let mut s = String::new();
    f.read_to_string(&mut s).ok();
    let decode = json::decode(&s);
    if decode.is_err() {
        println!("Error parsing {}", filename);
        return;
    }
    let track: Track = decode.unwrap();

    let int_track_id = get_or_incr2(&mut atb.track_ids,
        &mut atb.all_tracks.track_tags,
        &mut atb.all_tracks.track_ids,
        &track.track_id,
        &mut atb.next_track_int);

    debug!( "Artist: {}, title: {}", track.artist, track.title );
    for tag in track.tags {
        let int_tag_id = get_or_incr2(&mut atb.tag_names,
            &mut atb.all_tracks.tag_tracks,
            &mut atb.all_tracks.tag_names,
            &tag.0,
            &mut atb.next_tag_int);

        atb.all_tracks.track_tags.get_mut(int_track_id as usize).unwrap().insert( int_tag_id );
        atb.all_tracks.tag_tracks.get_mut(int_tag_id as usize).unwrap().insert( int_track_id );
    }
}

fn transform_tracks(paths: &[&str])
{
    let mut f = File::create("test.json").unwrap();
    let mut atb : AllTrackBuilder = AllTrackBuilder{
        track_ids: HashMap::new(),
        tag_names: HashMap::new(),
        next_track_int: 0,
        next_tag_int: 0,
        all_tracks: AllTracks {
            track_ids: Vec::new(),
            tag_names: Vec::new(),
            track_tags: Vec::new(),
            tag_tracks: Vec::new(),
        }
    };
    for path in paths {
        debug!("Reading {}", path);
        walk_dir( Path::new(&path),
            &mut |f| load_track2(&mut atb, f.to_str().unwrap())
                    ).ok().expect("Error reading path");
    }

    let data = json::encode(&atb.all_tracks).unwrap();
    f.write( data.as_bytes() );

}


fn redis_connection() -> RedisResult<Connection>
{
    let client = try!(redis::Client::open("redis://127.0.0.1/"));
    return client.get_connection();
}

fn get_or_incr( conn: &Connection, key: &str, counter_key: &str ) -> usize
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

fn load_track<T>(conn: &Connection, bs: &mut Bitset<T>, filename: &str)
{
    let mut f = File::open(filename).unwrap();
    let mut s = String::new();
    f.read_to_string(&mut s).ok();
    let track: Track = json::decode(&s).unwrap();
    let int_track_id = get_or_incr(&conn, &track.track_id, "last_track_id");
    debug!( "Artist: {}, title: {}", track.artist, track.title );
    for tag in track.tags {
        let int_tag_id = get_or_incr(&conn, &tag.0, "last_tag_id");
        bs.add( "track-tags", int_track_id, &[int_tag_id] ).ok().expect("a");
        bs.add( "tag-tracks", int_tag_id, &[int_track_id] ).ok().expect("b");
    }
}

// Had to change Fn to FnMut to "borrow data mutably in a captured outer variable". TODO: figure out what any of that means and why.
fn walk_dir<F>(path: &Path, cb: &mut F) -> io::Result<()>
    where F : FnMut(&Path) -> ()
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

fn colorize_ns<T:Write>(t: &mut Terminal<T>, ns: u64)
{
    let s = format!("{:15}", ns);
    let colors = [term::color::BRIGHT_RED, term::color::BRIGHT_YELLOW, term::color::BRIGHT_GREEN, term::color::BRIGHT_BLUE, term::color::BRIGHT_BLUE];
    for (chars,color) in s.as_bytes().chunks(3).zip(colors.iter()) {
        t.fg(*color).unwrap();
        write!( t, "{}", str::from_utf8(chars).unwrap() ).unwrap();
    }
}

macro_rules! time
{
    ($desc: expr, $($expr: expr),*) => {
        {
            let start = time::precise_time_ns();
            let result = $( $expr )*;
            let end = time::precise_time_ns();
            let mut t = term::stdout().unwrap();
            write!(t, "{:20}: ", $desc ).unwrap();
            colorize_ns(&mut *t, end - start);
            t.reset().unwrap();
            writeln!(t, " ns").unwrap();
            result
        }
    }
}

fn bench1(conn: &Connection, n: usize)
{
    println!("Sorting tracks by size..");
    let tracks : Vec<String> = redis::cmd("KEYS").arg("track-tags-*").query(conn).unwrap();
    let mut tracks_counts : Vec<(&str, usize)> = time!( "fetch", tracks.iter().map( |t| { let c:usize = conn.scard(&t as &str).unwrap(); (t as &str, c) } ).collect() );
    time!( "sort", tracks_counts.sort_by(|a,b| b.1.cmp(&a.1)) ); // sort descending
    let top_n : Vec<&str> = tracks_counts.iter().take(n).map(|f| f.0).collect();
    println!("Start.. {}", top_n.len());
    let tags : Vec<usize> = time!( "union track-tags", conn.sunion(top_n).unwrap() );
    println!("Got {} tags", tags.len() );
    let tag_names : Vec<String> = tags.iter().map( |t| format!("tag-tracks-{}", t) ).collect();
    let tracks : Vec<usize> = time!( "union tag-tracks", conn.sunion(tag_names).unwrap() );
    println!("Got {} tracks", tracks.len() );
    let track_names : Vec<String> = tracks.iter().map( |t| format!("track-tags-{}", t) ).collect();
    let final_tags : Vec<usize> = time!( "inter track-tags", conn.sinter( track_names ).unwrap() );
    println!("Final tags:");
    for tag in final_tags {
        println!("{}", tag);
    }
}

fn bench2(conn: &Connection, n: usize)
{
    println!("Sorting tracks by size..");
    let tracks : Vec<String> = redis::cmd("KEYS").arg("track-tags-*").query(conn).unwrap();
    let mut tracks_counts : Vec<(&str, usize)> = time!( "fetch", tracks.iter().map( |t| { let c:usize = conn.scard(&t as &str).unwrap(); (t as &str, c) } ).collect() );
    time!( "sort", tracks_counts.sort_by(|a,b| b.1.cmp(&a.1)) ); // sort descending
    let top_n : Vec<&str> = tracks_counts.iter().take(n).map(|f| f.0).collect();
    let top_n_keys : Vec<usize> = top_n.iter().map( |t| t.split("-").nth(2).unwrap().parse().unwrap() ).collect();
    println!("Copying {} sets to judy", top_n.len());
    let mut judy = JudyBitset::new();
    // for track in top_n doesn't work because we use top_n later. TODO: understand why
    for &track in top_n.iter() {
        let track_num : usize = track.split("-").nth(2).unwrap().parse().unwrap();
        let members : Vec<usize> = conn.smembers(track).unwrap();
        judy.add( "track-tags", track_num, &members ).unwrap();
    }

    let tags : Vec<usize> = time!( "union track-tags", judy.union("track-tags", &top_n_keys).unwrap() );
    println!("Got {} tags", tags.len() );
    let tag_names : Vec<String> = tags.iter().map( |t| format!("tag-tracks-{}", t) ).collect();

    println!("Copying {} sets to judy", tags.len() );
    for (tag, &num) in tag_names.iter().zip(&tags) {
        let members : Vec<usize> = conn.smembers(tag as &str).unwrap();
        judy.add("tag-tracks", num, &members).unwrap();
    }

    let tracks : Vec<usize> = time!( "union tag-tracks", judy.union("tag-tracks", &tags).unwrap() );
    println!("Got {} tracks", tracks.len() );
    let track_names : Vec<String> = tracks.iter().map( |t| format!("track-tags-{}", t) ).collect();

    println!("Copying {} sets to judy", tracks.len() );
    for (track, &num) in track_names.iter().zip(&tracks) {
        let members : Vec<usize> = conn.smembers(track as &str).unwrap();
        judy.add("track-tags", num, &members).unwrap();
    }

    let final_tags : Vec<usize> = time!( "inter track-tags", judy.intersect("track-tags", &tracks).unwrap() );
    println!("Final tags:");
    for tag in final_tags {
        println!("{}", tag);
    }
}

fn usage(program_name: &str)
{
    println!("Usage:");
    println!("\t{} <command> <args>", program_name);
    println!("");
    println!("Commands:");
    println!("\t{:20} {}", "transform PATH...", "do stuff");
    println!("\t{:20} {}", "import PATH...", "import tracks");
    println!("\t{:20} {}", "tags [N]", "list N biggest tags (if omitted, list all)");
    println!("\t{:20} {}", "bench1 N", "union all tags from top N tracks, get all tracks with those tags, then intersect the tags");
    println!("\t{:20} {}", "bench2 N", "copy redis to judy, then do bench1 with judy");
}

fn main()
{
    env_logger::init().unwrap();
    let conn = redis_connection().unwrap();
    let mut args = env::args();
    let prog_name = args.next().unwrap();
    let mut bs = RedisBitset::new(&conn);
    match args.next().unwrap_or("".into()).as_ref() {
        "transform" => {
            let paths : Vec<String> = args.collect();
            let prefs : Vec<&str> = paths.iter().map(|s|s.as_ref()).collect();
            transform_tracks(&prefs);
        }
        "import" => {
            for arg in args {
                debug!("Reading {}", arg);
                time!("Import tracks",
                      walk_dir( Path::new(&arg),
                                &mut |f| load_track(&conn, &mut bs, f.to_str().unwrap()))
                      ).ok().expect("Error reading path");
            }
        },
        "tags" => {
            let max = args.next().and_then(|v|v.parse().ok()).unwrap_or(0);
            list_tags(&conn, max);
        },
        "bench1" => {
            let n = args.next().and_then(|v|v.parse().ok()).unwrap();
            bench1(&conn, n);
        }
        "bench2" => {
            let n = args.next().and_then(|v|v.parse().ok()).unwrap();
            bench2(&conn, n);
        }
        _ => usage(&prog_name)
    }
}
