import os
import csv
import time
import librosa
import spotipy
import yt_dlp
import numpy as np
import pandas as pd
import unittest
from datetime import datetime
from spotipy.oauth2 import SpotifyOAuth, SpotifyClientCredentials
from concurrent.futures import ThreadPoolExecutor, as_completed
from yt_dlp import YoutubeDL

# === Debug Utility ===
def debug(msg):
    print(f"[DEBUG] {msg}")

# === Spotify Credentials Rotation ===
SPOTIFY_CREDENTIALS = [
    {"client_id": "15adf67aec934fe792bee0d467742326", "client_secret": "d03b2411aad24b8e80f3257660f9f10f"},
    {"client_id": "241765db513d43218e1e996b7d13d73f", "client_secret": "0fb1d0f0eed44f2e98d0e022335dd9e1"},
    {"client_id": "56bfb61f27234852826fd13e813174e6", "client_secret": "401f40941cba4f5bb2a0274f9fb34df2"}
]

def get_spotify_client(index=0):
    creds = SPOTIFY_CREDENTIALS[index % len(SPOTIFY_CREDENTIALS)]
    debug(f"🔄 Switching to Spotify client #{index % len(SPOTIFY_CREDENTIALS)}")
    auth_manager = SpotifyClientCredentials(client_id=creds["client_id"], client_secret=creds["client_secret"])
    return spotipy.Spotify(auth_manager=auth_manager)

sp = get_spotify_client()

# === OAuth for extended scope access ===
auth_sp = spotipy.Spotify(auth_manager=SpotifyOAuth(
    client_id=SPOTIFY_CREDENTIALS[0]["client_id"],
    client_secret=SPOTIFY_CREDENTIALS[0]["client_secret"],
    redirect_uri="http://127.0.0.1:8888/callback",
    scope="user-read-recently-played user-library-read"
))

# === Folder Setup ===
os.makedirs("csvs", exist_ok=True)
os.makedirs("audio_files", exist_ok=True)
os.makedirs("songs_by_year", exist_ok=True)

# === Movie Files by Language ===
movie_files = {
    "telugu": "movies_by_language/telugu_movies.csv",
    "hindi": "movies_by_language/hindi_movies.csv",
    "kannada": "movies_by_language/kannada_movies.csv",
    "tamil": "movies_by_language/tamil_movies.csv"
}

# === Fetch Album and Tracks ===
def fetch_album_and_tracks(title, lang, year, max_retries=3, base_delay=5):
    global sp
    query = f"{title} {lang} {year}"
    debug(f"🎧 Searching Spotify for album: {query}")
    for attempt in range(max_retries):
        try:
            results = sp.search(q=query, type='album', limit=1)
            if results['albums']['items']:
                album = results['albums']['items'][0]
                album_id = album['id']
                debug(f"✅ Found album: {album['name']} ({album_id})")
                tracks = sp.album_tracks(album_id)['items']
                debug(f"🎵 Found {len(tracks)} track(s).")
                return [{
                    "Spotify ID": t['id'],
                    "Title": t['name'],
                    "Artist": ", ".join(a['name'] for a in t['artists']),
                    "Album": album['name'],
                    "Release Date": album['release_date'],
                    "Popularity": 0,
                    "movie_title": title,
                    "language": lang,
                    "year": year
                } for t in tracks]
            else:
                debug("⚠️ No album found on Spotify.")
        except Exception as e:
            debug(f"❌ Spotify search failed (attempt {attempt+1}): {e}")
            time.sleep(base_delay * (2 ** attempt))
            sp = get_spotify_client(attempt)
    return []

# === YouTube Search ===
def get_youtube_url(title, artist):
    query = f"{title} {artist} official audio"
    debug(f"🔎 Searching YouTube for: {query}")
    try:
        ydl_opts = {
            'quiet': True,
            'noplaylist': True,
            'default_search': 'ytsearch1',
            'extract_flat': True,
        }
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            result = ydl.extract_info(query, download=False)
            if 'entries' in result and result['entries']:
                url = f"https://www.youtube.com/watch?v={result['entries'][0]['id']}"
                debug(f"🔗 YouTube video found: {url}")
                return url
    except Exception as e:
        debug(f"❌ YouTube search failed: {e}")
    return None

# === Audio Download ===
def download_audio(youtube_url, filename):
    debug(f"📥 Downloading audio for: {youtube_url}")
    out_path = f"audio_files/{filename}.wav"
    ...
    try:
        with YoutubeDL(ydl_opts) as ydl:
            debug(f"🎬 yt-dlp download started for {filename}")
            ydl.download([youtube_url])
            debug(f"✅ Download completed: {out_path}")
    except Exception as e:
        debug(f"❌ Download failed: {e}")
        return None
    return out_path

# === Feature Extraction ===
def extract_audio_features(file_path):
    try:
        debug(f"🧬 Extracting audio features from: {file_path}")
        y, sr = librosa.load(file_path, sr=22050)
        features = {
            "tempo": librosa.beat.beat_track(y=y, sr=sr)[0],
            "loudness": np.mean(librosa.feature.rms(y=y)),
            "key": librosa.feature.chroma_stft(y=y, sr=sr).mean(),
            "danceability": np.mean(librosa.feature.spectral_contrast(y=y, sr=sr)),
            "energy": np.mean(librosa.feature.mfcc(y=y, sr=sr, n_mfcc=1)),
            "speechiness": np.mean(librosa.feature.mfcc(y=y, sr=sr, n_mfcc=2)),
            "instrumentalness": np.mean(librosa.feature.zero_crossing_rate(y=y))
        }
        debug(f"📊 Extracted: {features}")
        return features
    except Exception as e:
        debug(f"❌ Feature extraction failed: {e}")
        return None

# === Process One Song ===
def process_song(song):
    debug(f"\n🎶 Processing: {song['Title']} by {song['Artist']}")
    youtube_url = get_youtube_url(song["Title"], song["Artist"])
    if not youtube_url:
        debug("⚠️ Skipping (YouTube URL not found)")
        return None

    audio_path = download_audio(youtube_url, song["Spotify ID"])
    if not audio_path:
        debug("⚠️ Skipping (Download failed)")
        return None

    features = extract_audio_features(audio_path)
    if os.path.exists(audio_path):
        os.remove(audio_path)
        debug(f"🧹 Cleaned up temp audio file: {audio_path}")

    if not features:
        debug("⚠️ Skipping (Feature extraction failed)")
        return None

    debug("✅ Song processed successfully.\n")
    return {**song, **features}

# === Process All Movies ===
def process_all_movies():
    for lang, file_path in movie_files.items():
        if not os.path.exists(file_path):
            debug(f"🚫 File not found: {file_path}")
            continue
        df = pd.read_csv(file_path)
        if not {'Title', 'Release Date', 'Language'}.issubset(df.columns):
            debug(f"❌ Required columns missing in {file_path}")
            continue

        for _, row in df.iterrows():
            title, release, language = row['Title'], row['Release Date'], row['Language']
            try:
                year = pd.to_datetime(release, errors='coerce', dayfirst=True).year
                if pd.isna(year):
                    continue
            except:
                continue

            debug(f"\n📁 Processing movie: {title} ({language}, {year})")
            songs = fetch_album_and_tracks(title, language, year)
            with ThreadPoolExecutor(max_workers=4) as executor:
                futures = [executor.submit(process_song, song) for song in songs]
                for future in as_completed(futures):
                    result = future.result()
                    if result:
                        folder_path = f"songs_by_year/{year}/{language}"
                        os.makedirs(folder_path, exist_ok=True)
                        target_csv = os.path.join(folder_path, "features.csv")
                        write_header = not os.path.exists(target_csv)
                        with open(target_csv, "a", newline="", encoding="utf-8") as f:
                            writer = csv.writer(f)
                            if write_header:
                                writer.writerow(csv_columns)
                            writer.writerow([result.get(col, "N/A") for col in csv_columns])
                        debug(f"💾 Written to CSV: {target_csv}")

# === CSV Columns ===
csv_columns = ["Spotify ID", "Title", "Artist", "Album", "Release Date", "Popularity",
               "tempo", "loudness", "key", "danceability", "energy", "speechiness", "instrumentalness",
               "movie_title", "language", "year"]

# === Main Entrypoint ===
if __name__ == "__main__":
    debug("🎬 Starting song processing pipeline...")
    process_all_movies()
    debug("✅ All songs processed.")

    # === Tests ===
    class TestMusicPipeline(unittest.TestCase):
        def test_fetch_album_and_tracks(self):
            debug("🧪 Testing: fetch_album_and_tracks()")
            result = fetch_album_and_tracks("Pushpa", "telugu", 2021)
            self.assertIsInstance(result, list)

        def test_youtube_search(self):
            debug("🧪 Testing: get_youtube_url()")
            url = get_youtube_url("Srivalli", "Sid Sriram")
            self.assertTrue(url is None or url.startswith("https://"))

        def test_extract_audio_features(self):
            debug("🧪 Testing: extract_audio_features()")
            dummy_path = "audio_files/dummy.wav"
            librosa.output.write_wav(dummy_path, np.zeros(22050), sr=22050)
            features = extract_audio_features(dummy_path)
            self.assertIsInstance(features, dict)
            os.remove(dummy_path)

    unittest.main(argv=[''], exit=False)
