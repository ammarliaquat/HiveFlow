# ==============================================================================
# HIVEFLOW FRONTEND MULTI-STAGE CONTAINER
# Builds React Vite app and serves via ultra-lightweight Nginx
# ==============================================================================
FROM node:20-alpine AS build

WORKDIR /app
COPY package*.json ./
RUN npm ci

COPY . .
RUN npm run build

# Stage 2: Serve with Nginx
FROM nginx:alpine

COPY --from=build /app/dist /usr/share/nginx/html

# SPA Routing Nginx Configuration
RUN echo 'server { \
    listen 80; \
    location / { \
        root /usr/share/nginx/html; \
        index index.html index.htm; \
        try_files $$uri $$uri/ /index.html; \
    } \
    location /api { \
        proxy_pass http://api:8000; \
        proxy_set_header Host $$host; \
        proxy_set_header X-Real-IP $$remote_addr; \
    } \
}' > /etc/nginx/conf.d/default.conf

EXPOSE 80

CMD ["nginx", "-g", "daemon off;"]
