# Use slim Python image
FROM python:3.10-slim

# Avoid prompts
ENV DEBIAN_FRONTEND=noninteractive

# Install ffmpeg and system deps
RUN apt-get update && apt-get install -y \
    ffmpeg \
    curl \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Install yt-dlp
RUN curl -L https://github.com/yt-dlp/yt-dlp/releases/latest/download/yt-dlp -o /usr/local/bin/yt-dlp && \
    chmod a+rx /usr/local/bin/yt-dlp

# App dir
WORKDIR /app

# Copy source
COPY . .

# Install Python deps
RUN pip install --no-cache-dir -r requirements.txt

# Expose port (optional)
EXPOSE 8000

# Start script
CMD ["python", "puppy.py"]
