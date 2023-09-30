use std::fs;
use serde::{Deserialize, Serialize};
use sqlx::MySqlPool;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let entries = fs::read_dir("jsons").expect("Could not read directory");
    println!("CREATING POOL...");
    let pool = MySqlPool::connect("mysql://admin:password@localhost/spot")
        .await
        .expect("Failed to create database pool");
    let mut counter: u32 = 0;

    for entry in entries {
        let path = entry.expect("Path not extracted").path();
        let file = fs::File::open(path).expect("Can't open file");

        println!("SERIALIZING...");
        let plays: Vec<Play> = serde_json::from_reader(file).expect("Could not map plays");

        println!("ITERATING...");
        for play in plays {
            println!("INSERTING {}...", counter);
            sqlx::query(
                "INSERT INTO plays (
                ts, username, platform, ms_played, conn_country,
                ip_addr_decrypted, user_agent_decrypted, master_metadata_track_name,
                master_metadata_album_artist_name, master_metadata_album_album_name,
                spotify_track_uri, episode_name, episode_show_name, spotify_episode_uri,
                reason_start, reason_end, shuffle, skipped, offline, offline_timestamp, incognito_mode
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
                .bind(play.ts).bind(play.username).bind(play.platform)
                .bind(play.ms_played).bind(play.conn_country).bind(play.ip_addr_decrypted)
                .bind(play.user_agent_decrypted).bind(play.master_metadata_track_name)
                .bind(play.master_metadata_album_artist_name).bind(play.master_metadata_album_album_name)
                .bind(play.spotify_track_uri).bind(play.episode_name).bind(play.episode_show_name)
                .bind(play.spotify_episode_uri).bind(play.reason_start).bind(play.reason_end)
                .bind(play.shuffle).bind(play.skipped).bind(play.offline)
                .bind(play.offline_timestamp).bind(play.incognito_mode)
                .execute(&pool)
                .await
                .expect("Failed to execute SQL query");
            counter += 1;
        }
    }

    Ok(())
}

#[derive(Debug, Serialize, Deserialize)]
struct Play {
    ts: Option<String>,
    username: Option<String>,
    platform: Option<String>,
    ms_played: Option<u64>,
    conn_country: Option<String>,
    ip_addr_decrypted: Option<String>,
    user_agent_decrypted: Option<String>,
    master_metadata_track_name: Option<String>,
    master_metadata_album_artist_name: Option<String>,
    master_metadata_album_album_name: Option<String>,
    spotify_track_uri: Option<String>,
    episode_name: Option<String>,
    episode_show_name: Option<String>,
    spotify_episode_uri: Option<String>,
    reason_start: Option<String>,
    reason_end: Option<String>,
    shuffle: Option<bool>,
    skipped: Option<bool>,
    offline: Option<bool>,
    offline_timestamp: Option<u64>,
    incognito_mode: Option<bool>,
}
