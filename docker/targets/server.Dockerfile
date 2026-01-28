# IMPORTANT: When updating ALPINE_TAG, also update the default value in:
# - docker/docker-bake.hcl (variable "ALPINE_TAG")
# - docker/targets/admin-tools.Dockerfile (ARG ALPINE_TAG)
# NOTE: We use just the tag without a digest pin because digest-pinned manifest lists
# cause platform resolution issues in multi-arch buildx builds (InvalidBaseImagePlatform warnings).
ARG ALPINE_TAG=3.23

# Get grpc_health_probe for health checks
FROM --platform=$TARGETPLATFORM grpc/grpc-health-probe:v0.4.37 AS grpc-health-probe

FROM --platform=$TARGETPLATFORM alpine:${ALPINE_TAG}

ARG TARGETARCH

RUN apk add --no-cache \
    ca-certificates \
    tzdata && addgroup -g 1000 temporal && \
    adduser -u 1000 -G temporal -D temporal

COPY --chmod=755 ./build/${TARGETARCH}/temporal-server /usr/local/bin/
COPY --chmod=755 ./scripts/sh/entrypoint.sh /etc/temporal/entrypoint.sh
COPY --from=grpc-health-probe --chmod=755 /ko-app/grpc-health-probe /usr/local/bin/

WORKDIR /etc/temporal
USER temporal

# Health check using gRPC health protocol on frontend port (7233)
# The server registers with grpc.health.v1 and sets SERVING status when ready
HEALTHCHECK --interval=5s --timeout=3s --start-period=30s --retries=3 \
    CMD ["grpc-health-probe", "-addr=:7233"]

CMD [ "/etc/temporal/entrypoint.sh" ]
