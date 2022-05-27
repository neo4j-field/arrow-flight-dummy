#!/bin/sh

# hardcode for now
OUT_DIR="build/libs"
CERTIFICATE="${OUT_DIR}/dummy.crt"
PRIVATE_KEY="${OUT_DIR}/dummy.key"

# We assume `gradle shadowJar` or the like has been run and created ${OUT_DIR}
if [ ! -d "${OUT_DIR}" ]; then
  echo "${OUT_DIR} does not exist" >&2
  exit 1
fi

# Wipe any existing keys
if [ -f "${CERTIFICATE}" ]; then unlink "${CERTIFICATE}"; fi
if [ -f "${PRIVATE_KEY}" ]; then unlink "${PRIVATE_KEY}"; fi

# Refresh our cert and key
openssl req -x509 -newkey rsa:4096 \
  -keyout "${PRIVATE_KEY}" \
  -out "${CERTIFICATE}" \
  -days 30 -nodes \
  -subj "/CN=*"
