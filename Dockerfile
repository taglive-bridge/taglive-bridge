FROM node:20
WORKDIR /src
COPY package.json ./
RUN npm install
RUN apt-get update && apt-get install -y ffmpeg && rm -rf /var/lib/apt/lists/*
COPY . .
CMD ["node", "server.js"]
