import os
import csv
import time
import spotipy
import pandas as pd
import io
import zipfile
from datetime import datetime
from spotipy.oauth2 import SpotifyClientCredentials
from flask import Flask, render_template, send_file, jsonify, request
import plotly.express as px
import plotly.io as pio
import json

app = Flask(__name__)

# === Spotify Credentials Rotation
SPOTIFY_CREDENTIALS = [
    {"client_id": "15adf67aec934fe792bee0d467742326", "client_secret": "d03b2411aad24b8e80f3257660f9f10f"},
    {"client_id": "241765db513d43218e1e996b7d13d73f", "client_secret": "0fb1d0f0eed44f2e98d0e022335dd9e1"},
    {"client_id": "56bfb61f27234852826fd13e813174e6", "client_secret": "401f40941cba4f5bb2a0274f9fb34df2"}
]

# Create folder structure
os.makedirs("csvs", exist_ok=True)
os.makedirs("static", exist_ok=True)
os.makedirs("templates", exist_ok=True)

# === Helper functions
def get_spotify_client(index=0):
    creds = SPOTIFY_CREDENTIALS[index % len(SPOTIFY_CREDENTIALS)]
    print(f"🔄 Using Spotify client #{index + 1}")
    auth = SpotifyClientCredentials(client_id=creds["client_id"], client_secret=creds["client_secret"])
    return spotipy.Spotify(auth_manager=auth)

def fetch_album_tracks(title, lang, year, retries=3):
    sp = get_spotify_client()
    query = f"{title} {lang} {year}"
    for attempt in range(retries):
        try:
            print(f"🔍 Searching Spotify for: {query}")
            result = sp.search(q=query, type="album", limit=1)
            albums = result['albums']['items']
            if not albums:
                return []
            album = albums[0]
            tracks = sp.album_tracks(album['id'])['items']
            return [{
                "Spotify ID": track['id'],
                "Track Name": track['name'],
                "Artist(s)": ", ".join([a['name'] for a in track['artists']]),
                "Album": album['name'],
                "Release Date": album['release_date'],
                "Spotify URL": track['external_urls']['spotify'],
                "movie_title": title,
                "language": lang,
                "year": year
            } for track in tracks]
        except Exception as e:
            print(f"❌ Error: {e}")
            time.sleep(2 ** attempt)
            sp = get_spotify_client(attempt + 1)  # rotate credentials
    return []

def get_all_languages():
    return [d for d in os.listdir("csvs") if os.path.isdir(os.path.join("csvs", d))]

def get_language_years(language):
    language_dir = os.path.join("csvs", language)
    if not os.path.exists(language_dir):
        return []
    return [f.split('.')[0] for f in os.listdir(language_dir) if f.endswith('.csv')]

def get_data_summary():
    languages = get_all_languages()
    summary = {}
    total_tracks = 0
    
    for lang in languages:
        lang_dir = os.path.join("csvs", lang)
        if not os.path.exists(lang_dir):
            continue
            
        lang_files = [f for f in os.listdir(lang_dir) if f.endswith('.csv')]
        lang_tracks = 0
        years_data = {}
        
        for csv_file in lang_files:
            year = csv_file.split('.')[0]
            file_path = os.path.join(lang_dir, csv_file)
            try:
                df = pd.read_csv(file_path)
                track_count = len(df)
                years_data[year] = track_count
                lang_tracks += track_count
            except Exception as e:
                print(f"Error reading {file_path}: {e}")
                
        summary[lang] = {
            "total_tracks": lang_tracks,
            "years": years_data
        }
        total_tracks += lang_tracks
    
    return {
        "languages": languages,
        "summary": summary,
        "total_tracks": total_tracks
    }

def generate_plots():
    data_summary = get_data_summary()
    plots = {}
    
    # 1. Tracks by language
    if data_summary["summary"]:
        lang_counts = {lang: data["total_tracks"] for lang, data in data_summary["summary"].items()}
        fig = px.bar(
            x=list(lang_counts.keys()),
            y=list(lang_counts.values()),
            labels={"x": "Language", "y": "Number of Tracks"},
            title="Tracks by Language"
        )
        plots["lang_distribution"] = json.loads(pio.to_json(fig))
        
        # 2. Tracks by year (across all languages)
        year_data = []
        for lang, data in data_summary["summary"].items():
            for year, count in data["years"].items():
                year_data.append({"language": lang, "year": year, "tracks": count})
                
        if year_data:
            df = pd.DataFrame(year_data)
            fig = px.line(
                df, 
                x="year", 
                y="tracks", 
                color="language",
                title="Tracks by Year and Language"
            )
            plots["yearly_trend"] = json.loads(pio.to_json(fig))
    
    return plots

# === Routes
@app.route('/')
def index():
    data_summary = get_data_summary()
    plots = generate_plots()
    return render_template('index.html', data=data_summary, plots=plots)

@app.route('/run-collection', methods=['POST'])
def run_collection():
    movie_files = {
        "telugu": "movies_by_language/telugu_movies.csv",
        "hindi": "movies_by_language/hindi_movies.csv",
        "kannada": "movies_by_language/kannada_movies.csv",
        "tamil": "movies_by_language/tamil_movies.csv"
    }

    # Create output folder structure
    for lang in movie_files:
        os.makedirs(f"csvs/{lang}", exist_ok=True)

    results = {}
    for lang, file_path in movie_files.items():
        if not os.path.exists(file_path):
            results[lang] = {"status": "error", "message": f"Missing file: {file_path}"}
            continue
            
        try:
            df = pd.read_csv(file_path)
            if not {'Title', 'Release Date', 'Language'}.issubset(df.columns):
                results[lang] = {"status": "error", "message": f"Invalid file format: {file_path}"}
                continue
                
            track_count = 0
            for _, row in df.iterrows():
                title = row["Title"]
                release = row["Release Date"]
                try:
                    year = pd.to_datetime(release, errors='coerce').year
                    if pd.isna(year):
                        continue
                except:
                    continue
                    
                songs = fetch_album_tracks(title, lang, year)
                if not songs:
                    continue
                    
                track_count += len(songs)
                output_path = f"csvs/{lang}/{year}.csv"
                file_exists = os.path.exists(output_path)
                
                with open(output_path, "a", newline="", encoding="utf-8") as f:
                    writer = csv.DictWriter(f, fieldnames=songs[0].keys())
                    if not file_exists:
                        writer.writeheader()
                    for song in songs:
                        writer.writerow(song)
                        
            results[lang] = {"status": "success", "tracks_added": track_count}
        except Exception as e:
            results[lang] = {"status": "error", "message": str(e)}
    
    return jsonify(results)

@app.route('/download/<language>/<year>')
def download_year_csv(language, year):
    file_path = f"csvs/{language}/{year}.csv"
    if os.path.exists(file_path):
        return send_file(file_path, as_attachment=True)
    return "File not found", 404

@app.route('/download/<language>')
def download_language_csvs(language):
    lang_dir = f"csvs/{language}"
    if not os.path.exists(lang_dir):
        return "Language not found", 404
        
    memory_file = io.BytesIO()
    with zipfile.ZipFile(memory_file, 'w') as zf:
        for filename in os.listdir(lang_dir):
            if filename.endswith('.csv'):
                file_path = os.path.join(lang_dir, filename)
                zf.write(file_path, arcname=filename)
    
    memory_file.seek(0)
    return send_file(
        memory_file,
        download_name=f"{language}_tracks.zip",
        as_attachment=True
    )

@app.route('/download-all')
def download_all_csvs():
    memory_file = io.BytesIO()
    with zipfile.ZipFile(memory_file, 'w') as zf:
        for language in os.listdir('csvs'):
            lang_dir = os.path.join('csvs', language)
            if os.path.isdir(lang_dir):
                for filename in os.listdir(lang_dir):
                    if filename.endswith('.csv'):
                        file_path = os.path.join(lang_dir, filename)
                        zf.write(file_path, arcname=f"{language}/{filename}")
    
    memory_file.seek(0)
    return send_file(
        memory_file,
        download_name="all_movie_tracks.zip",
        as_attachment=True
    )

@app.route('/api/stats')
def api_stats():
    return jsonify(get_data_summary())

# Create HTML template
with open("templates/index.html", "w") as f:
    f.write("""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Indian Movie Tracks Dashboard</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0-alpha1/dist/css/bootstrap.min.css" rel="stylesheet">
    <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
    <style>
        .card {
            margin-bottom: 20px;
            transition: transform 0.2s;
        }
        .card:hover {
            transform: translateY(-5px);
            box-shadow: 0 10px 20px rgba(0,0,0,0.1);
        }
        .language-card {
            border-left: 5px solid #0d6efd;
        }
        .stats-card {
            border-left: 5px solid #198754;
        }
        .year-card {
            border-left: 5px solid #6f42c1;
        }
    </style>
</head>
<body>
    <nav class="navbar navbar-expand-lg navbar-dark bg-primary">
        <div class="container">
            <a class="navbar-brand" href="/">Indian Movie Tracks</a>
            <button class="navbar-toggler" type="button" data-bs-toggle="collapse" data-bs-target="#navbarNav">
                <span class="navbar-toggler-icon"></span>
            </button>
            <div class="collapse navbar-collapse" id="navbarNav">
                <ul class="navbar-nav">
                    <li class="nav-item">
                        <a class="nav-link" href="/">Dashboard</a>
                    </li>
                    <li class="nav-item dropdown">
                        <a class="nav-link dropdown-toggle" href="#" id="downloadDropdown" role="button" data-bs-toggle="dropdown">
                            Downloads
                        </a>
                        <ul class="dropdown-menu" id="downloadMenu">
                            <li><a class="dropdown-item" href="/download-all">Download All CSVs</a></li>
                            <li><hr class="dropdown-divider"></li>
                            {% for language in data.languages %}
                            <li><a class="dropdown-item" href="/download/{{ language }}">Download {{ language|capitalize }} CSVs</a></li>
                            {% endfor %}
                        </ul>
                    </li>
                </ul>
                <ul class="navbar-nav ms-auto">
                    <li class="nav-item">
                        <button id="runCollectionBtn" class="btn btn-outline-light">Run Collection</button>
                    </li>
                </ul>
            </div>
        </div>
    </nav>

    <div class="container mt-4">
        <div class="row mb-4">
            <div class="col-md-12">
                <div class="alert alert-info" id="statusAlert" style="display: none;"></div>
            </div>
        </div>

        <div class="row mb-4">
            <div class="col-md-4">
                <div class="card stats-card h-100">
                    <div class="card-header">
                        <h5>Overall Statistics</h5>
                    </div>
                    <div class="card-body">
                        <h2 class="display-4">{{ data.total_tracks }}</h2>
                        <p class="lead">Total Tracks</p>
                        <hr>
                        <p>Languages: {{ data.languages|length }}</p>
                    </div>
                </div>
            </div>
            <div class="col-md-8">
                <div class="card">
                    <div class="card-header">
                        <h5>Tracks by Language</h5>
                    </div>
                    <div class="card-body">
                        <div id="langDistribution" style="height: 300px;"></div>
                    </div>
                </div>
            </div>
        </div>

        <div class="row mb-4">
            <div class="col-md-12">
                <div class="card">
                    <div class="card-header">
                        <h5>Yearly Trends</h5>
                    </div>
                    <div class="card-body">
                        <div id="yearlyTrend" style="height: 400px;"></div>
                    </div>
                </div>
            </div>
        </div>

        <h3 class="mb-3">Language Breakdown</h3>
        <div class="row">
            {% for language, lang_data in data.summary.items() %}
            <div class="col-md-6 mb-4">
                <div class="card language-card h-100">
                    <div class="card-header d-flex justify-content-between align-items-center">
                        <h5>{{ language|capitalize }}</h5>
                        <a href="/download/{{ language }}" class="btn btn-sm btn-outline-primary">Download All</a>
                    </div>
                    <div class="card-body">
                        <p class="lead">{{ lang_data.total_tracks }} tracks</p>
                        <hr>
                        <h6>Available Years:</h6>
                        <div class="row g-2">
                            {% for year, count in lang_data.years.items()|sort %}
                            <div class="col-auto">
                                <a href="/download/{{ language }}/{{ year }}.csv" class="btn btn-sm btn-outline-secondary">
                                    {{ year }} ({{ count }})
                                </a>
                            </div>
                            {% endfor %}
                        </div>
                    </div>
                </div>
            </div>
            {% endfor %}
        </div>
    </div>

    <footer class="bg-light py-3 mt-5">
        <div class="container text-center">
            <p class="text-muted">Indian Movie Tracks Dashboard © 2025</p>
        </div>
    </footer>

    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0-alpha1/dist/js/bootstrap.bundle.min.js"></script>
    <script>
        // Plot charts
        document.addEventListener('DOMContentLoaded', function() {
            {% if plots.lang_distribution %}
                Plotly.newPlot('langDistribution', {{ plots.lang_distribution | safe }}.data, {{ plots.lang_distribution | safe }}.layout);
            {% endif %}
            
            {% if plots.yearly_trend %}
                Plotly.newPlot('yearlyTrend', {{ plots.yearly_trend | safe }}.data, {{ plots.yearly_trend | safe }}.layout);
            {% endif %}
            
            // Run collection button
            document.getElementById('runCollectionBtn').addEventListener('click', function() {
                const statusAlert = document.getElementById('statusAlert');
                statusAlert.innerHTML = 'Collection is running... This may take several minutes.';
                statusAlert.className = 'alert alert-warning';
                statusAlert.style.display = 'block';
                
                fetch('/run-collection', {
                    method: 'POST',
                })
                .then(response => response.json())
                .then(data => {
                    let successCount = 0;
                    let trackCount = 0;
                    let messages = [];
                    
                    for (const lang in data) {
                        if (data[lang].status === 'success') {
                            successCount++;
                            trackCount += data[lang].tracks_added;
                            messages.push(`${lang}: Added ${data[lang].tracks_added} tracks`);
                        } else {
                            messages.push(`${lang}: Error - ${data[lang].message}`);
                        }
                    }
                    
                    if (successCount > 0) {
                        statusAlert.className = 'alert alert-success';
                        statusAlert.innerHTML = `Collection completed with ${successCount} languages successful. Added ${trackCount} tracks.<br>` + 
                                               messages.join('<br>') + 
                                               '<br><br>Refresh the page to see updated data.';
                    } else {
                        statusAlert.className = 'alert alert-danger';
                        statusAlert.innerHTML = 'Collection failed for all languages:<br>' + messages.join('<br>');
                    }
                })
                .catch(error => {
                    statusAlert.className = 'alert alert-danger';
                    statusAlert.innerHTML = 'An error occurred during collection: ' + error;
                });
            });
        });
    </script>
</body>
</html>""")

if __name__ == '__main__':
    # Create required directories
    for lang in ["telugu", "hindi", "kannada", "tamil"]:
        os.makedirs(f"csvs/{lang}", exist_ok=True)
    
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)