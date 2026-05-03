# syntax=docker/dockerfile:1.7

# ---- builder ---------------------------------------------------------------
# Builds a self-contained mix release (with bundled ERTS) on Alpine. We force
# exqlite to compile its NIF from source against musl rather than try to use
# the precompiled glibc binary.

FROM elixir:1.18-otp-27-alpine AS builder

ENV MIX_ENV=prod \
    LANG=C.UTF-8 \
    ELIXIR_MAKE_FORCE_BUILD=true

RUN apk add --no-cache build-base git

WORKDIR /app

RUN mix local.hex --force && mix local.rebar --force

# Dep manifest first, so `deps.get` only re-runs when the manifest changes.
COPY mix.exs mix.lock ./
COPY config config
RUN mix deps.get --only prod
RUN mix deps.compile

# App source.
COPY lib lib
COPY priv priv

RUN mix compile
RUN mix release --overwrite

# ---- runtime ---------------------------------------------------------------
# Tiny Alpine image — release brings ERTS, we just need the C runtime libs
# the BEAM links against and a CA bundle for outbound HTTPS (the migrator).
#
# Runtime alpine version MUST match the builder's. The builder image
# `elixir:1.18-otp-27-alpine` currently tracks alpine 3.22, so we pin
# 3.22 here. If the builder drifts to a newer alpine (check with
# `docker run --rm elixir:1.18-otp-27-alpine cat /etc/alpine-release`),
# bump this tag to match — otherwise crypto's NIF will fail to load
# against an older OpenSSL ABI.

FROM alpine:3.22 AS runtime

RUN apk add --no-cache \
    libstdc++ \
    ncurses-libs \
    libgcc \
    openssl \
    ca-certificates \
    tzdata

WORKDIR /app

COPY --from=builder /app/_build/prod/rel/kafun ./

ENV LANG=C.UTF-8

EXPOSE 8333 8334

ENTRYPOINT ["/app/bin/kafun"]
CMD ["start"]
