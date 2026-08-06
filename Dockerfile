# syntax=docker/dockerfile:1.7@sha256:a57df69d0ea827fb7266491f2813635de6f17269be881f696fbfdf2d83dda33e

ARG GO_IMAGE=golang:1.26.5-alpine3.23@sha256:622e56dbc11a8cfe87cafa2331e9a201877271cbff918af53d3be315f3da88cc
ARG RUNTIME_IMAGE=alpine:3.23.5@sha256:fd791d74b68913cbb027c6546007b3f0d3bc45125f797758156952bc2d6daf40

FROM ${GO_IMAGE} AS builder

ARG CI_COMMIT_SHA=unknown
ARG CI_COMMIT_TAG

ENV CGO_ENABLED=1 \
    GOTOOLCHAIN=local

RUN apk add --no-cache \
    git \
    ca-certificates \
    build-base \
    su-exec \
    olm-dev

COPY . /build
WORKDIR /build
RUN CI=true \
    CI_COMMIT_SHA="$CI_COMMIT_SHA" \
    CI_COMMIT_TAG="$CI_COMMIT_TAG" \
    ./build.sh \
    && ./tumblr-dms --version

FROM ${RUNTIME_IMAGE} AS runtime

ENV UID=1337 \
    GID=1337

# Keep the standard mautrix bridge runtime toolset. Beeper's hosted launcher
# directly uses bash, curl, jq, and yq, while the remaining tools keep the
# image consistent with supported self-hosting and live debugging workflows.
RUN apk add --no-cache ffmpeg su-exec ca-certificates olm bash jq yq-go curl

COPY --from=builder --chmod=0755 /build/tumblr-dms /usr/bin/tumblr-dms
COPY --chmod=0755 docker-run.sh /docker-run.sh

VOLUME /data
WORKDIR /data

CMD ["/docker-run.sh"]
