#!/bin/sh

OUT_DIR="build/libs"
CERTIFICATE="${OUT_DIR}/dummy.crt"
PRIVATE_KEY="${OUT_DIR}/dummy.key"

if [ ! -d "${OUT_DIR}" ]; then
  echo "${OUT_DIR} does not exist" >&2
  exit 1
fi

if [ -f "${CERTIFICATE}" ]; then unlink "${CERTIFICATE}"; fi
if [ -f "${PRIVATE_KEY}" ]; then unlink "${PRIVATE_KEY}"; fi

openssl req -x509 -newkey rsa:4096 \
  -keyout "${PRIVATE_KEY}" \
  -out "${CERTIFICATE}" \
  -days 30 -nodes \
  -subj "/CN=*"
