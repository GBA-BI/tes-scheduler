FROM golang:1.20.5 AS builder
WORKDIR /go/src/github.com/GBA-BI/tes-scheduler
COPY . .
RUN make build-linux

FROM debian:11
RUN apt update \
     && apt install -y --no-install-recommends ca-certificates curl locales \
     && apt clean \
     && rm -rf /var/lib/apt/lists/*
ENV LANG=C.UTF-8
RUN locale-gen
USER nobody
WORKDIR /app
COPY --from=builder /go/src/github.com/GBA-BI/tes-scheduler/bin/vetes-scheduler .
ENTRYPOINT ["/app/vetes-scheduler"]
