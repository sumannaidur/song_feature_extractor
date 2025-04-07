import os
import csv
import time
import librosa
import spotipy
import asyncio
import numpy as np
import pandas as pd
import shutil
import subprocess
from datetime import datetime
from spotipy.oauth2 import SpotifyOAuth, SpotifyClientCredentials
from pyppeteer import launch
from pyppeteer.chromium_downloader import chromium_executable

# === Setup Paths
TEMP_AUDIO_DIR = "audio_files"
YOUTUBE_COOKIE_PATH = "/etc/secrets/youtube_cookies.txt"
TEMP_COOKIE_PATH = "/tmp/youtube_cookies.txt"

os.makedirs("csvs", exist_ok=True)
os.makedirs(TEMP_AUDIO_DIR, exist_ok=True)

# === Cookie copy
if os.path.exists(YOUTUBE_COOKIE_PATH):
    try:
        shutil.copy(YOUTUBE_COOKIE_PATH, TEMP_COOKIE_PATH)
        print("✅ YouTube cookie file copied to /tmp.")
    except Exception as e:
        print(f"❌ Cookie copy failed: {e}")
else:
    print("❌ Cookie file not found!")

# === Spotify Credential Rotation
SPOTIFY_CREDENTIALS = [
    {"client_id": "15adf67aec934fe792bee0d467742326", "client_secret": "d03b2411aad24b8e80f3257660f9f10f"},
    {"client_id": "241765db513d43218e1e996b7d13d73f", "client_secret": "0fb1d0f0eed44f2e98d0e022335dd9e1"},
    {"client_id": "56bfb61f27234852826fd13e813174e6", "client_secret": "401f40941cba4f5bb2a0274f9fb34df2"}
]

def get_spotify_client(index=0):
    creds = SPOTIFY_CREDENTIALS[index % len(SPOTIFY_CREDENTIALS)]
    print(f"🔄 Using Spotify Client #{index}")
    auth_manager = SpotifyClientCredentials(client_id=creds["client_id"], client_secret=creds["client_secret"])
    return spotipy.Spotify(auth_manager=auth_manager)

sp = get_spotify_client()

# === Movie Input
movie_files = {
    "telugu": "movies_by_language/telugu_movies.csv",
    "hindi": "movies_by_language/hindi_movies.csv",
    "kannada": "movies_by_language/kannada_movies.csv",
    "tamil": "movies_by_language/tamil_movies.csv"
}

# === Fetch Album and Tracks
def fetch_album_and_tracks(title, lang, year, max_retries=3):
    global sp
    query = f"{title} {lang} {year}"
    for attempt in range(max_retries):
        try:
            results = sp.search(q=query, type='album', limit=1)
            if results['albums']['items']:
                album = results['albums']['items'][0]
                album_id = album['id']
                tracks = sp.album_tracks(album_id)['items']
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
        except Exception as e:
            print(f"❌ Spotify error: {e}")
            sp = get_spotify_client(attempt)
            time.sleep(2)
    return []

# === Use Pyppeteer to search and return video URL
async def fetch_youtube_url_with_pyppeteer(query):
    try:
        browser = await launch(
            headless=True,
            executablePath=chromium_executable(),
            args=["--no-sandbox", "--disable-dev-shm-usage"]
        )
        page = await browser.newPage()
        await page.goto(f"https://www.youtube.com/results?search_query={query.replace(' ', '+')}")
        await page.waitForSelector("ytd-video-renderer", timeout=10000)

        video_url = await page.evaluate('''() => {
            const el = document.querySelector("ytd-video-renderer a#thumbnail");
            return el ? el.href : null;
        }''')

        await browser.close()
        return video_url
    except Exception as e:
        print(f"❌ Pyppeteer YouTube fetch failed: {e}")
        return None

def get_youtube_url(title, artist):
    query = f"{title} {artist} official audio"
    try:
        return asyncio.get_event_loop().run_until_complete(fetch_youtube_url_with_pyppeteer(query))
    except Exception as e:
        print(f"❌ YouTube async error: {e}")
        return None

# === Use ffmpeg to record YouTube audio from browser
def record_youtube_audio(video_url, filename):
    out_path = f"{TEMP_AUDIO_DIR}/{filename}.wav"
    try:
        # Play in browser (headless) and capture audio with ffmpeg
        cmd = [
            "ffmpeg",
            "-y",
            "-i", video_url,
            "-vn",
            "-acodec", "pcm_s16le",
            "-ar", "44100",
            "-ac", "2",
            out_path
        ]
        subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        return out_path if os.path.exists(out_path) else None
    except Exception as e:
        print(f"❌ FFmpeg recording failed: {e}")
        return None

# === Audio Feature Extraction
def extract_audio_features(file_path):
    try:
        y, sr = librosa.load(file_path, sr=22050)
        return {
            "tempo": librosa.beat.beat_track(y=y, sr=sr)[0],
            "loudness": np.mean(librosa.feature.rms(y=y)),
            "key": librosa.feature.chroma_stft(y=y, sr=sr).mean(),
            "danceability": np.mean(librosa.feature.spectral_contrast(y=y, sr=sr)),
            "energy": np.mean(librosa.feature.mfcc(y=y, sr=sr, n_mfcc=1)),
            "speechiness": np.mean(librosa.feature.mfcc(y=y, sr=sr, n_mfcc=2)),
            "instrumentalness": np.mean(librosa.feature.zero_crossing_rate(y=y))
        }
    except Exception as e:
        print(f"❌ Feature extraction failed: {e}")
        return None

# === Process Individual Song
def process_song(song):
    print(f"🎵 Processing: {song['Title']} by {song['Artist']}")
    youtube_url = get_youtube_url(song["Title"], song["Artist"])
    if not youtube_url:
        return None

    audio_path = record_youtube_audio(youtube_url, song["Spotify ID"])
    if not audio_path:
        return None

    features = extract_audio_features(audio_path)
    if os.path.exists(audio_path):
        os.remove(audio_path)

    return {**song, **features} if features else None

# === CSV Setup
csv_columns = ["Spotify ID", "Title", "Artist", "Album", "Release Date", "Popularity",
               "tempo", "loudness", "key", "danceability", "energy", "speechiness", "instrumentalness",
               "movie_title", "language", "year"]

output_csv = "song_features_combined.csv"
if not os.path.exists(output_csv):
    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow(csv_columns)

# === Main Loop
for lang, file_path in movie_files.items():
    if not os.path.exists(file_path): continue
    df = pd.read_csv(file_path)
    if not {'Title', 'Release Date', 'Language'}.issubset(df.columns): continue

    for _, row in df.iterrows():
        title, release, language = row['Title'], row['Release Date'], row['Language']
        try:
            year = pd.to_datetime(release, errors='coerce', dayfirst=True).year
            if pd.isna(year): continue
        except:
            continue

        songs = fetch_album_and_tracks(title, language, year)
        for song in songs:
            processed = process_song(song)
            if processed:
                with open(output_csv, "a", newline="", encoding="utf-8") as f:
                    csv.writer(f).writerow([processed.get(col, "N/A") for col in csv_columns])

print("\n🎉 All songs processed. Output saved to:", output_csv)
