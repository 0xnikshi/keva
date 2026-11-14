# Build all Keva binaries as static Linux executables.
FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/kevad    ./cmd/kevad  && \
    CGO_ENABLED=0 GOOS=linux go build -o /out/kevaload ./cmd/kevaload && \
    CGO_ENABLED=0 GOOS=linux go build -o /out/keva     ./cmd/keva

# Minimal runtime image; the compose file selects which binary to run.
FROM alpine:3.20
RUN apk add --no-cache ca-certificates wget
COPY --from=build /out/kevad /out/kevaload /out/keva /usr/local/bin/
EXPOSE 7070 9100
