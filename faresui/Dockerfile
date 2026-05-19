FROM node:18

WORKDIR /app

# Copy package files and install dependencies
COPY package.json ./
COPY package-lock.json ./
RUN npm install --legacy-peer-deps --silent

# Copy source and build
COPY . ./
RUN npm run build --silent

# Serve the built files with a lightweight static server
RUN npm install -g http-server --silent
EXPOSE 4200
CMD ["http-server", "dist/faresui", "-p", "4200", "-c-1"]
