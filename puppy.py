import os
import csv
import time
import librosa
import shutil
import spotipy
import numpy as np
import pandas as pd
import subprocess
from datetime import datetime
from spotipy.oauth2 import SpotifyClientCredentials
import asyncio
from pyppeteer import launch
# === Setup Paths and Folders
os.makedirs("csvs", exist_ok=True)
os.makedirs("audio_files", exist_ok=True)
os.makedirs("songs_by_year", exist_ok=True)

# === Spotify Credentials Rotation
SPOTIFY_CREDENTIALS = [
    {"client_id": "15adf67aec934fe792bee0d467742326", "client_secret": "d03b2411aad24b8e80f3257660f9f10f"},
    {"client_id": "241765db513d43218e1e996b7d13d73f", "client_secret": "0fb1d0f0eed44f2e98d0e022335dd9e1"},
    {"client_id": "56bfb61f27234852826fd13e813174e6", "client_secret": "401f40941cba4f5bb2a0274f9fb34df2"}
]

def get_spotify_client(index=0):
    creds = SPOTIFY_CREDENTIALS[index % len(SPOTIFY_CREDENTIALS)]
    print(f"🔄 Switching to Spotify client #{index + 1}")
    auth = SpotifyClientCredentials(client_id=creds["client_id"], client_secret=creds["client_secret"])
    return spotipy.Spotify(auth_manager=auth)

sp = get_spotify_client()

# === Input files
movie_files = {
    "telugu": "movies_by_language/telugu_movies.csv",
    "hindi": "movies_by_language/hindi_movies.csv",
    "kannada": "movies_by_language/kannada_movies.csv",
    "tamil": "movies_by_language/tamil_movies.csv"
}

# === Spotify fetch
def fetch_album_and_tracks(title, lang, year, retries=3):
    global sp
    query = f"{title} {lang} {year}"
    for attempt in range(retries):
        try:
            print(f"🔎 Spotify search: {query}")
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
            print(f"❌ Spotify fetch error: {e}")
            time.sleep(2 ** attempt)
            sp = get_spotify_client(attempt)
    return []

# === Use pyppeteer to get YouTube video URL
async def get_stream_url(title, artist):
    query = f"{title} {artist} official audio"
    search_url = f"https://www.youtube.com/results?search_query={query.replace(' ', '+')}"
    print(f"🌐 Searching on YouTube: {query}")

    browser = await launch(headless=True, args=['--no-sandbox'])
    page = await browser.newPage()
    await page.goto(search_url, timeout=60000)

    video_id = await page.evaluate('''() => {
        const el = document.querySelector('a#video-title');
        return el ? el.href.split('v=')[1].split('&')[0] : null;
    }''')

    await browser.close()

    if not video_id:
        print("❌ No video found.")
        return None

    return f"https://www.youtube.com/watch?v={video_id}"

# === Wrapper for sync call
def get_youtube_url(title, artist):
    try:
        return asyncio.get_event_loop().run_until_complete(get_stream_url(title, artist))
    except Exception as e:
        print(f"❌ Pyppeteer error: {e}")
        return None

# === Use yt-dlp + ffmpeg to stream and save audio
def download_audio(youtube_url, filename):
    out_path = f"audio_files/{filename}.wav"
    try:
        print(f"🎯 Fetching stream URL for: {youtube_url}")
        stream_url = subprocess.check_output([
            "yt-dlp", "-f", "bestaudio", "-g", youtube_url
        ], text=True).strip()

        print(f"⬇️ Downloading audio with FFmpeg to: {out_path}")
        subprocess.run([
            "ffmpeg", "-y", "-i", stream_url,
            "-vn", "-acodec", "pcm_s16le", "-ar", "44100", "-ac", "2",
            out_path
        ], check=True)

        return out_path
    except subprocess.CalledProcessError as e:
        print(f"❌ Audio download failed: {e}")
        return None

# === Extract features
def extract_audio_features(file_path):
    try:
        print(f"🎧 Extracting features from: {file_path}")
        y, sr = librosa.load(file_path, sr=22050)
        return {
            "tempo": librosa.beat.beat_track(y=y, sr=sr)[0],
            "loudness": float(np.mean(librosa.feature.rms(y=y))),
            "key": float(librosa.feature.chroma_stft(y=y, sr=sr).mean()),
            "danceability": float(np.mean(librosa.feature.spectral_contrast(y=y, sr=sr))),
            "energy": float(np.mean(librosa.feature.mfcc(y=y, sr=sr, n_mfcc=1))),
            "speechiness": float(np.mean(librosa.feature.mfcc(y=y, sr=sr, n_mfcc=2))),
            "instrumentalness": float(np.mean(librosa.feature.zero_crossing_rate(y=y)))
        }
    except Exception as e:
        print(f"❌ Feature extraction failed: {e}")
        return None

def process_song(song):
    print(f"\n🎵 Processing: {song['Title']} by {song['Artist']}")
    youtube_url = get_youtube_url(song["Title"], song["Artist"])
    if not youtube_url:
        return None

    audio_path = download_audio(youtube_url, song["Spotify ID"])
    if not audio_path:
        return None

    features = extract_audio_features(audio_path)

    if os.path.exists(audio_path):
        os.remove(audio_path)

    return {**song, **features} if features else None


# === Output columns
csv_columns = [
    "Spotify ID", "Title", "Artist", "Album", "Release Date", "Popularity",
    "tempo", "loudness", "key", "danceability", "energy", "speechiness", "instrumentalness",
    "movie_title", "language", "year"
]

output_csv = "song_features_combined.csv"
if not os.path.exists(output_csv):
    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow(csv_columns)

# === Run through all songs
for lang, file_path in movie_files.items():
    if not os.path.exists(file_path):
        print(f"⚠️ Missing file: {file_path}")
        continue

    df = pd.read_csv(file_path)
    if not {'Title', 'Release Date', 'Language'}.issubset(df.columns):
        print(f"⚠️ Skipping invalid file: {file_path}")
        continue

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

print("\n✅ All songs processed! Output saved to:", output_csv)
