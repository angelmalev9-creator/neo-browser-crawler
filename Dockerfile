FROM node:20-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    ca-certificates \
    wget \
    fonts-liberation \
    libatk-bridge2.0-0 \
    libatk1.0-0 \
    libcups2 \
    libdrm2 \
    libxkbcommon0 \
    libxcomposite1 \
    libxdamage1 \
    libxrandr2 \
    libgbm1 \
    libasound2 \
    libnss3 \
    libx11-xcb1 \
    libxfixes3 \
    libxext6 \
    libx11-6 \
    libxcb1 \
    libxrender1 \
    libglib2.0-0 \
    libpango-1.0-0 \
    libpangocairo-1.0-0 \
    libgtk-3-0 \
    --no-install-recommends && rm -rf /var/lib/apt/lists/*

COPY package.json ./
RUN npm install

# 🔥 ВАЖНОТО
RUN npx playwright install chromium

COPY . .

CMD ["node", "server.js"]
