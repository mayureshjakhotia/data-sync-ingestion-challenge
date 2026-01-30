FROM node:20-alpine

WORKDIR /app

COPY package.json package-lock.json* ./
RUN npm ci 2>/dev/null || npm install

COPY tsconfig.json ./
COPY src/ ./src/

RUN npm run build

CMD ["node", "--max-old-space-size=2048", "dist/index.js"]
