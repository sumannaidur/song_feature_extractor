import os
import pandas as pd
from flask import Flask, render_template_string
from music_pipeline import MusicFeatureExtractor

# === Spotify API credentials ===
SPOTIFY_CREDENTIALS = [
    {"client_id": "15adf67aec934fe792bee0d467742326", "client_secret": "d03b2411aad24b8e80f3257660f9f10f"},
    {"client_id": "241765db513d43218e1e996b7d13d73f", "client_secret": "0fb1d0f0eed44f2e98d0e022335dd9e1"},
    {"client_id": "56bfb61f27234852826fd13e813174e6", "client_secret": "401f40941cba4f5bb2a0274f9fb34df2"}
]

# Initialize Flask app
app = Flask(__name__)
extractor = MusicFeatureExtractor(SPOTIFY_CREDENTIALS)

# Movie CSVs by language
movie_files = {
    "telugu": "movies_by_language/telugu_movies.csv",
    "kannada": "movies_by_language/kannada_movies.csv",
    "tamil": "movies_by_language/tamil_movies.csv",
    "hindi": "movies_by_language/hindi_movies.csv"
}

# HTML template for simple dashboard
TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>Music Feature Extractor</title>
</head>
<body style="font-family: Arial; padding: 30px;">
    <h2>🎵 Music Feature Extractor</h2>
    <form method="POST">
        <button type="submit">Run Pipeline</button>
    </form>
    {% if message %}
        <p>{{ message }}</p>
    {% endif %}
</body>
</html>
"""

@app.route('/', methods=['GET', 'POST'])
def index():
    message = None
    if os.environ.get("RENDER") == "true" or os.environ.get("RUN_PIPELINE") == "true":
        message = run_pipeline()
    return render_template_string(TEMPLATE, message=message)

def run_pipeline():
    for lang, path in movie_files.items():
        if not os.path.exists(path):
            print(f"⚠️ File not found: {path}")
            continue

        df = pd.read_csv(path)
        if not {'Title', 'Release Date', 'Language'}.issubset(df.columns):
            print(f"⚠️ Invalid format in: {path}")
            continue

        for _, row in df.iterrows():
            title = row.get("Title")
            release = row.get("Release Date")

            try:
                year = pd.to_datetime(release, errors="coerce", dayfirst=True).year
                if pd.isna(year) or year < 1900:
                    continue
            except Exception as e:
                print(f"⚠️ Date parsing error: {e}")
                continue

            tracks = extractor.fetch_album_tracks(title, lang, year)
            for track in tracks:
                extractor.process_song(track)

    print("🎉 All movie songs processed successfully!")
    return "✅ Pipeline completed successfully!"

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)), debug=True)
