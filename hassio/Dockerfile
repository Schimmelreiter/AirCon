# Baut das Add-on lokal aus dem Zweig "schimmelreiter" dieses Forks (kein fertiges Image).
# BUILD_VERSION (vom Supervisor = Add-on-Version) erzwingt bei jeder neuen Version einen
# frischen Download des Codes statt eines alten Docker-Zwischenstands.
FROM python:3.10-slim

ARG BUILD_VERSION=latest
ARG AIRCON_REF=schimmelreiter
LABEL io.hass.version="$BUILD_VERSION" io.hass.type="addon" io.hass.arch="armhf|armv7|aarch64|amd64|i386"

RUN apt-get update && apt-get install -y --no-install-recommends jq git && rm -rf /var/lib/apt/lists/*
RUN echo "Version $BUILD_VERSION, Quelle $AIRCON_REF" && \
    pip install --no-cache-dir "git+https://github.com/Schimmelreiter/AirCon@${AIRCON_REF}"

ENV PLATFORM=docker
ENV CONFIG_DIR=/opt/hisense
ENV OPTIONS_FILE=/data/options.json

COPY run.sh /
RUN chmod a+x /run.sh

CMD [ "/run.sh" ]
