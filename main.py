import os
import shutil
import tempfile
import yt_dlp

# --- Constants ---
YOUTUBE_COOKIE_ORIG = "/etc/secrets/youtube_cookies.txt"
VIDEO_URL = "https://youtu.be/hvBL2J6q_RI"  # Public YouTube URL for testing

# --- Copy cookie to temp file ---
if os.path.exists(YOUTUBE_COOKIE_ORIG):
    with tempfile.NamedTemporaryFile(delete=False, suffix=".txt") as tmp_cookie:
        shutil.copy(YOUTUBE_COOKIE_ORIG, tmp_cookie.name)
        cookie_path = tmp_cookie.name
        print(f"✅ Copied cookie file to temp path: {cookie_path}")
else:
    print("❌ Cookie file not found!")
    cookie_path = None

# --- Attempt download ---
if cookie_path:
    ydl_opts = {
        "format": "bestaudio/best",
        "outtmpl": "test_audio.%(ext)s",
        "postprocessors": [{
            "key": "FFmpegExtractAudio",
            "preferredcodec": "mp3",
            "preferredquality": "192"
        }],
        "cookiefile": cookie_path,
        "quiet": False,
    }

    try:
        print(f"🎬 Trying to download: {VIDEO_URL}")
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([VIDEO_URL])
        print("✅ Download complete.")
    except Exception as e:
        print(f"❌ Download failed: {e}")
else:
    print("🚫 Skipping download since no cookie available.")
