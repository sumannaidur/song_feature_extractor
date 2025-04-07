import os
import csv
import time
import librosa
import asyncio
import shutil
import spotipy
import numpy as np
import pandas as pd
import subprocess
from datetime import datetime
from spotipy.oauth2 import SpotifyClientCredentials
from pyppeteer import launch
from pyppeteer.chromium_downloader import chromium_executable

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

# === Fetch Album and Tracks from Spotify
def fetch_album_and_tracks(title, lang, year, retries=3):
    global sp  # Fix for the UnboundLocalError
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
            sp = get_spotify_client(attempt)  # switch client
    return []

# === Pyppeteer-based YouTube Search
async def fetch_youtube_url(query):
    try:
        print(f"🔍 Searching YouTube for: {query}")

        # ✅ Skip downloading Chromium; use local Chrome
        chrome_path = "C:/Program Files/Google/Chrome/Application/chrome.exe"  # Adjust this if needed
        if not os.path.exists(chrome_path):
            raise FileNotFoundError(f"Chrome not found at: {chrome_path}")

        browser = await launch(
            headless=True,
            executablePath=chrome_path,
            args=["--no-sandbox"]
        )
        page = await browser.newPage()
        await page.goto(f"https://www.youtube.com/results?search_query={query.replace(' ', '+')}")
        await page.waitForSelector("ytd-video-renderer a#thumbnail", timeout=10000)
        url = await page.evaluate('''() => {
            const video = document.querySelector("ytd-video-renderer a#thumbnail");
            return video ? video.href : null;
        }''')
        await browser.close()
        print(f"🎯 YouTube URL: {url}")
        return url
    except Exception as e:
        print(f"❌ YouTube search failed: {e}")
        return None


def get_youtube_url(title, artist):
    query = f"{title} {artist} official audio"
    try:
        return asyncio.run(fetch_youtube_url(query))  # Replaces deprecated get_event_loop
    except Exception as e:
        print(f"❌ Async error: {e}")
        return None



# === Download Audio Using ffmpeg From YouTube URL
def download_audio(youtube_url, filename):
    out_path = f"audio_files/{filename}.wav"
    try:
        print(f"⬇️ Downloading audio via yt-dlp for: {youtube_url}")
        temp_audio = f"audio_files/{filename}.m4a"

        # Use yt-dlp to download the best audio
        subprocess.run([
            "yt-dlp", "-x", "--audio-format", "m4a", "-o", temp_audio, youtube_url
        ], check=True)

        # Convert to WAV using ffmpeg
        subprocess.run([
            "ffmpeg", "-y", "-i", temp_audio,
            "-vn", "-acodec", "pcm_s16le", "-ar", "44100", "-ac", "2",
            out_path
        ], check=True)

        # Cleanup intermediate file
        if os.path.exists(temp_audio):
            os.remove(temp_audio)

        return out_path
    except subprocess.CalledProcessError as e:
        print(f"❌ yt-dlp or ffmpeg download failed: {e}")
        return None


# === Extract Audio Features Using Librosa
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

# === Main Song Processing
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

# === Output Columns
csv_columns = [
    "Spotify ID", "Title", "Artist", "Album", "Release Date", "Popularity",
    "tempo", "loudness", "key", "danceability", "energy", "speechiness", "instrumentalness",
    "movie_title", "language", "year"
]

output_csv = "song_features_combined.csv"
if not os.path.exists(output_csv):
    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow(csv_columns)

# === Iterate Movie Data and Process Songs
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
