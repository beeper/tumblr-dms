# syntax=docker/dockerfile:1.7@sha256:a57df69d0ea827fb7266491f2813635de6f17269be881f696fbfdf2d83dda33e

ARG GO_IMAGE=golang:1.26.5-alpine3.23@sha256:622e56dbc11a8cfe87cafa2331e9a201877271cbff918af53d3be315f3da88cc
ARG RUNTIME_IMAGE=alpine:3.23.5@sha256:fd791d74b68913cbb027c6546007b3f0d3bc45125f797758156952bc2d6daf40

FROM ${GO_IMAGE} AS builder

ARG CI_COMMIT_SHA=unknown
ARG CI_COMMIT_TAG

ENV CGO_ENABLED=1 \
    GOTOOLCHAIN=local

RUN apk add --no-cache \
    build-base=0.5-r3 \
    git=2.52.0-r0

COPY . /build
WORKDIR /build
RUN CI=true \
    CI_COMMIT_SHA="$CI_COMMIT_SHA" \
    CI_COMMIT_TAG="$CI_COMMIT_TAG" \
    MAU_STATIC_BUILD=true \
    ./build.sh -o /out/tumblr-dms \
    && /out/tumblr-dms --version

FROM scratch AS artifact

COPY --from=builder /out/tumblr-dms /tumblr-dms

FROM ${RUNTIME_IMAGE} AS runtime

ARG CI_COMMIT_SHA=unknown
ARG CI_COMMIT_TAG

LABEL org.opencontainers.image.title="Tumblr DMs bridge" \
      org.opencontainers.image.source="https://github.com/beeper/tumblr-dms" \
      org.opencontainers.image.revision="$CI_COMMIT_SHA" \
      org.opencontainers.image.version="$CI_COMMIT_TAG"

RUN apk add --no-cache \
    ca-certificates=20260611-r0 \
    su-exec=0.3-r0

COPY --from=builder --chmod=0755 /out/tumblr-dms /usr/bin/tumblr-dms
COPY --chmod=0755 docker-run.sh /docker-run.sh

VOLUME /data
WORKDIR /data

CMD ["/docker-run.sh"]
