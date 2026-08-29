FROM oven/bun:1 AS base
WORKDIR /app
COPY package.json bun.lock ./
RUN bun install --frozen-lockfile

FROM base AS dev
COPY . .
EXPOSE 5175
CMD ["bun", "run", "dev"]

FROM base AS build
COPY . .
RUN bun run build

FROM nginx:1.27-alpine AS production
COPY --from=build /app/dist /usr/share/nginx/html
COPY nginx.conf /etc/nginx/conf.d/default.conf
RUN mkdir -p /etc/nginx/extra-conf.d
COPY nginx-backend-not-found.conf /etc/nginx/extra-conf.d/backend-not-found.conf
EXPOSE 80
