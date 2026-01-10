FROM node:20-slim

# Install deps needed for Chromium
RUN apt-get update && apt-get install -y \
  chromium \
  ca-certificates \
  fonts-liberation \
  libnss3 \
  libatk-bridge2.0-0 \
  libgtk-3-0 \
  libxss1 \
  libasound2 \
  libxshmfence1 \
  libgbm1 \
  libu2f-udev \
  wget \
  --no-install-recommends && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY package*.json ./
RUN npm install

COPY . .

ENV PORT=10000
EXPOSE 10000

CMD ["node", "server.js"]
