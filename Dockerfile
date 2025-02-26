FROM golang:1.24-alpine AS builder

WORKDIR /app
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o app

FROM jrottenberg/ffmpeg:7.1-scratch
COPY --from=builder /app/app .
EXPOSE 8080
ENTRYPOINT ["./app"]
