FROM golang:1-alpine3.23 AS builder

RUN apk add --no-cache git ca-certificates build-base su-exec olm-dev

COPY . /build
WORKDIR /build
RUN ./build.sh

FROM alpine:3.23

ENV UID=1337 \
    GID=1337

RUN apk add --no-cache ca-certificates su-exec olm

COPY --from=builder /build/tumblr-dms /usr/bin/tumblr-dms
COPY docker-run.sh /docker-run.sh
RUN chmod +x /docker-run.sh

VOLUME /data

CMD ["/docker-run.sh"]
