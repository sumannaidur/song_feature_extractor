import os
import csv
import time
import librosa
import spotipy
import yt_dlp
import asyncio
import numpy as np
import pandas as pd
import shutil
from datetime import datetime
from spotipy.oauth2 import SpotifyOAuth, SpotifyClientCredentials

# === Pyppeteer imports
from pyppeteer import launch
from pyppeteer.chromium_downloader import chromium_executable

# === YouTube Cookies
YOUTUBE_COOKIE_PATH = "/etc/secrets/youtube_cookies.txt"
TEMP_COOKIE_PATH = "/tmp/youtube_cookies.txt"

# === Copy YouTube cookie file to writable temp dir on Render
if os.path.exists(YOUTUBE_COOKIE_PATH):
    try:
        shutil.copy(YOUTUBE_COOKIE_PATH, TEMP_COOKIE_PATH)
        print("✅ YouTube cookie file copied to /tmp.")
    except Exception as e:
        print(f"❌ Failed to copy cookie file: {e}")
else:
    print("❌ YouTube cookie file missing!")

# === Spotify Credentials Rotation ===
SPOTIFY_CREDENTIALS = [
    {"client_id": "15adf67aec934fe792bee0d467742326", "client_secret": "d03b2411aad24b8e80f3257660f9f10f"},
    {"client_id": "241765db513d43218e1e996b7d13d73f", "client_secret": "0fb1d0f0eed44f2e98d0e022335dd9e1"},
    {"client_id": "56bfb61f27234852826fd13e813174e6", "client_secret": "401f40941cba4f5bb2a0274f9fb34df2"}
]

def get_spotify_client(index=0):
    creds = SPOTIFY_CREDENTIALS[index % len(SPOTIFY_CREDENTIALS)]
    print(f"🔄 Switching to Spotify client #{index % len(SPOTIFY_CREDENTIALS)}")
    auth_manager = SpotifyClientCredentials(client_id=creds["client_id"], client_secret=creds["client_secret"])
    return spotipy.Spotify(auth_manager=auth_manager)

sp = get_spotify_client()

# === Auth for SpotifyOAuth (if needed)
auth_sp = spotipy.Spotify(auth_manager=SpotifyOAuth(
    client_id=SPOTIFY_CREDENTIALS[0]["client_id"],
    client_secret=SPOTIFY_CREDENTIALS[0]["client_secret"],
    redirect_uri="http://127.0.0.1:8888/callback",
    scope="user-read-recently-played user-library-read"
))

# === Setup Folders
os.makedirs("csvs", exist_ok=True)
os.makedirs("audio_files", exist_ok=True)
os.makedirs("songs_by_year", exist_ok=True)

# === Movie input files
movie_files = {
    "telugu": "movies_by_language/telugu_movies.csv",
    "hindi": "movies_by_language/hindi_movies.csv",
    "kannada": "movies_by_language/kannada_movies.csv",
    "tamil": "movies_by_language/tamil_movies.csv"
}

# === Spotify album + track fetcher
def fetch_album_and_tracks(title, lang, year, max_retries=3, base_delay=5):
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
            time.sleep(base_delay * (2 ** attempt))
            sp = get_spotify_client(attempt)
    return []

# === Pyppeteer YouTube Search
async def fetch_youtube_url_with_pyppeteer(query):
    try:
        browser = await launch(
            headless=True,
            executablePath=chromium_executable(),
            args=["--no-sandbox", "--disable-dev-shm-usage"]
        )
        page = await browser.newPage()
        search_url = f"https://www.youtube.com/results?search_query={query.replace(' ', '+')}"
        await page.goto(search_url)
        await page.waitForSelector("ytd-video-renderer", timeout=10000)

        video_url = await page.evaluate('''() => {
            const video = document.querySelector("ytd-video-renderer a#thumbnail");
            return video ? video.href : null;
        }''')

        await browser.close()
        return video_url
    except Exception as e:
        print(f"❌ Pyppeteer search failed: {e}")
        return None

def get_youtube_url(title, artist):
    query = f"{title} {artist} official audio"
    try:
        return asyncio.get_event_loop().run_until_complete(fetch_youtube_url_with_pyppeteer(query))
    except Exception as e:
        print(f"❌ Async error: {e}")
        return None

# === Download YouTube Audio
def download_audio(youtube_url, filename):
    out_path = f"audio_files/{filename}.wav"
    ydl_opts = {
        "format": "bestaudio/best",
        "outtmpl": f"audio_files/{filename}.%(ext)s",
        "postprocessors": [{"key": "FFmpegExtractAudio", "preferredcodec": "wav", "preferredquality": "192"}],
        "quiet": True,
        "cookiefile": TEMP_COOKIE_PATH,
    }
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([youtube_url])
            return out_path
    except Exception as e:
        print(f"❌ Download failed: {e}")
        return None

# === Extract Audio Features
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

# === Song Pipeline
def process_song(song):
    print(f"🎵 Processing: {song['Title']} by {song['Artist']}")

    youtube_url = get_youtube_url(song["Title"], song["Artist"])
    if not youtube_url:
        return None

    audio_path = download_audio(youtube_url, song["Spotify ID"])
    if not audio_path:
        return None

    features = extract_audio_features(audio_path)

    if os.path.exists(audio_path):
        os.remove(audio_path)

    if not features:
        return None

    return {**song, **features}

# === Final Output
csv_columns = ["Spotify ID", "Title", "Artist", "Album", "Release Date", "Popularity",
               "tempo", "loudness", "key", "danceability", "energy", "speechiness", "instrumentalness",
               "movie_title", "language", "year"]

output_csv = "song_features_combined.csv"
if not os.path.exists(output_csv):
    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow(csv_columns)

for lang, file_path in movie_files.items():
    if not os.path.exists(file_path):
        continue
    df = pd.read_csv(file_path)
    if not {'Title', 'Release Date', 'Language'}.issubset(df.columns):
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

print("\n🎉 All songs processed. Output saved to:", output_csv)
