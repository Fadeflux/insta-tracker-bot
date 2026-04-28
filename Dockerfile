# Dockerfile for Railway deployment.
# We use a Node.js base image and install Chromium + required system libraries
# so puppeteer-core can launch a headless browser for the Threads scraper.
#
# This replaces the nixpacks.toml approach, which was unreliable on Railway.

FROM node:20-slim

# Install Chromium and the libraries it needs at runtime.
# debian-slim is missing a lot of fonts/libs by default — Chromium will refuse
# to launch without these.
RUN apt-get update && apt-get install -y --no-install-recommends \
    chromium \
    fonts-liberation \
    fonts-noto-color-emoji \
    libasound2 \
    libatk-bridge2.0-0 \
    libatk1.0-0 \
    libc6 \
    libcairo2 \
    libcups2 \
    libdbus-1-3 \
    libexpat1 \
    libfontconfig1 \
    libgbm1 \
    libgcc1 \
    libglib2.0-0 \
    libgtk-3-0 \
    libnspr4 \
    libnss3 \
    libpango-1.0-0 \
    libpangocairo-1.0-0 \
    libstdc++6 \
    libx11-6 \
    libx11-xcb1 \
    libxcb1 \
    libxcomposite1 \
    libxcursor1 \
    libxdamage1 \
    libxext6 \
    libxfixes3 \
    libxi6 \
    libxrandr2 \
    libxrender1 \
    libxss1 \
    libxtst6 \
    lsb-release \
    wget \
    xdg-utils \
    ca-certificates \
  && rm -rf /var/lib/apt/lists/*

# Tell puppeteer where to find Chromium and to skip its own download
ENV PUPPETEER_SKIP_CHROMIUM_DOWNLOAD=true
ENV PUPPETEER_EXECUTABLE_PATH=/usr/bin/chromium

WORKDIR /app

# Install dependencies first (cached layer if package.json doesn't change)
COPY package*.json ./
RUN npm install --omit=dev --no-audit --no-fund

# Copy the rest of the source
COPY . .

# Default command: same as Railway's "npm start"
CMD ["node", "src/index.js"]
