FROM golang:1.19-alpine3.16 AS stage

WORKDIR /source

COPY . .

RUN go mod download

RUN go build .

FROM alpine:latest

WORKDIR /

COPY --from=stage /source /app/

RUN chmod +x /app/notion-inbox-publisher

RUN addgroup proxy && adduser -S -G proxy proxy

USER proxy

WORKDIR /app

ENTRYPOINT ["./notion-inbox-publisher"]