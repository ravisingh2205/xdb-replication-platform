# ---- build ----
FROM node:22-alpine AS build
WORKDIR /app
COPY package.json package-lock.json* ./
RUN npm install --no-audit --no-fund
COPY . .
RUN npm run build

# ---- serve ----
FROM nginxinc/nginx-unprivileged:1.27-alpine
ENV API_GATEWAY_HOST=api-gateway
# nginx's official image renders /etc/nginx/templates/*.template with envsubst at start-up.
COPY nginx.conf /etc/nginx/templates/default.conf.template
COPY --from=build /app/dist /usr/share/nginx/html
EXPOSE 8080
