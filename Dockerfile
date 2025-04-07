# Use slim Python image
FROM python:3.10-slim

# Avoid prompts during install
ENV DEBIAN_FRONTEND=noninteractive

# Install dependencies
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    ca-certificates \
    fonts-liberation \
    libasound2 \
    libatk-bridge2.0-0 \
    libatk1.0-0 \
    libcups2 \
    libdbus-1-3 \
    libgdk-pixbuf2.0-0 \
    libnspr4 \
    libnss3 \
    libx11-xcb1 \
    libxcomposite1 \
    libxdamage1 \
    libxrandr2 \
    xdg-utils \
    libu2f-udev \
    libvulkan1 \
    libappindicator3-1 \
    libxss1 \
    libgbm1 \
    libgtk-3-0 \
    curl \
    unzip \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Create app directory
WORKDIR /app

# Copy app code
COPY . .

# Install Python deps
RUN pip install --upgrade pip && \
    pip install -r requirements.txt

# Download Chromium via pyppeteer
RUN python -c "import pyppeteer; pyppeteer.chromium_downloader.download_chromium()"

# Expose port (if needed)
EXPOSE 8000

# Run your script
CMD ["python", "puppy.py"]
