import os
import csv
import time
import librosa
import shutil
import spotipy
import numpy as np
import pandas as pd
import subprocess
import random
from datetime import datetime
from spotipy.oauth2 import SpotifyClientCredentials

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

# === YouTube Search via yt-dlp with anti-bot detection
def get_youtube_url(title, artist):
    query = f"{title} {artist} official audio"
    try:
        print(f"🔍 Searching YouTube for: {query}")
        
        # Configure yt-dlp with additional options to avoid bot detection
        command = [
            "yt-dlp",
            f"ytsearch1:{query}",
            "--get-url",
            "--no-check-certificate",
            "--user-agent", get_random_user_agent(),
            "--sleep-interval", "2",
            "--max-sleep-interval", "5",
            "--force-ipv4"
        ]
        
        result = subprocess.check_output(command, text=True)
        url = result.strip()
        print(f"🎯 YouTube URL: {url}")
        return url
    except subprocess.CalledProcessError as e:
        print(f"❌ YouTube search failed: {e}")
        # Try alternative source if YouTube fails
        return get_alternative_audio_source(title, artist)

# Random user agent to avoid detection
def get_random_user_agent():
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0",
        "Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Mobile/15E148 Safari/604.1"
    ]
    return random.choice(user_agents)

# Try alternative audio sources if YouTube fails
def get_alternative_audio_source(title, artist):
    # Try Soundcloud with yt-dlp
    try:
        query = f"{title} {artist}"
        print(f"🔍 Searching SoundCloud for: {query}")
        result = subprocess.check_output([
            "yt-dlp", 
            f"scsearch1:{query}", 
            "--get-url",
            "--user-agent", get_random_user_agent()
        ], text=True)
        url = result.strip()
        print(f"🎯 SoundCloud URL: {url}")
        return url
    except subprocess.CalledProcessError:
        # Try other fallback methods here
        pass
    
    # If all else fails, check if there's a preview_url from Spotify
    try:
        results = sp.search(q=f"track:{title} artist:{artist}", type='track', limit=1)
        if results['tracks']['items'] and results['tracks']['items'][0]['preview_url']:
            preview_url = results['tracks']['items'][0]['preview_url']
            print(f"🎯 Using Spotify preview URL: {preview_url}")
            return preview_url
    except Exception as e:
        print(f"❌ Spotify preview fetch error: {e}")
    
    return None

# === Download audio
def download_audio(url, filename):
    out_path = f"audio_files/{filename}.wav"
    try:
        print(f"⬇️ Downloading audio for: {url}")
        temp_audio = f"audio_files/{filename}.m4a"

        # Add additional parameters to avoid detection
        subprocess.run([
            "yt-dlp", 
            "-x", 
            "--audio-format", "m4a", 
            "-o", temp_audio,
            "--no-check-certificate",
            "--user-agent", get_random_user_agent(),
            "--sleep-interval", "1",
            "--max-sleep-interval", "3",
            url
        ], check=True)

        subprocess.run([
            "ffmpeg", "-y", "-i", temp_audio,
            "-vn", "-acodec", "pcm_s16le", "-ar", "44100", "-ac", "2",
            out_path
        ], check=True)

        if os.path.exists(temp_audio):
            os.remove(temp_audio)

        return out_path
    except subprocess.CalledProcessError as e:
        print(f"❌ Download failed: {e}")
        # If it's a Spotify preview URL, download directly
        if url and "spotify.com" in url:
            try:
                import requests
                r = requests.get(url)
                with open(temp_audio, 'wb') as f:
                    f.write(r.content)
                
                subprocess.run([
                    "ffmpeg", "-y", "-i", temp_audio,
                    "-vn", "-acodec", "pcm_s16le", "-ar", "44100", "-ac", "2",
                    out_path
                ], check=True)
                
                if os.path.exists(temp_audio):
                    os.remove(temp_audio)
                
                return out_path
            except Exception as e2:
                print(f"❌ Direct download failed: {e2}")
        
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

# === Song processing
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

# === Rate limiting and retries
def process_with_rate_limiting(df):
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
            
            # Introduce random delays between requests to avoid detection
            sleep_time = random.uniform(5, 15)
            print(f"😴 Sleeping for {sleep_time:.2f} seconds to avoid detection...")
            time.sleep(sleep_time)

# === Run through all songs
for lang, file_path in movie_files.items():
    if not os.path.exists(file_path):
        print(f"⚠️ Missing file: {file_path}")
        continue

    df = pd.read_csv(file_path)
    if not {'Title', 'Release Date', 'Language'}.issubset(df.columns):
        print(f"⚠️ Skipping invalid file: {file_path}")
        continue
    
    process_with_rate_limiting(df)

print("\n✅ All songs processed! Output saved to:", output_csv)