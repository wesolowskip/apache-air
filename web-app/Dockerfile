FROM node:14.20.0-alpine3.15 as builder
COPY . /app
WORKDIR /app
RUN npm install
RUN npm run build

FROM nginx:1.17.10-alpine
EXPOSE 80
COPY --from=builder /app/dist/apache-air-app /usr/share/nginx/html