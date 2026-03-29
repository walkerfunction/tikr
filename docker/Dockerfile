FROM golang:1.23-bookworm AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -o /app/bin/tikr ./cmd/tikr

# --- Runtime ---
FROM gcr.io/distroless/static-debian12

COPY --from=builder /app/bin/tikr /usr/local/bin/tikr

EXPOSE 9876 9877 9878

ENTRYPOINT ["tikr"]
CMD ["--config", "/etc/tikr/default.yaml"]
