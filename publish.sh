#!/usr/bin/env bash
set -e

echo "$PGP_SECRET" | base64 -d | gpg --batch --passphrase "$PGP_PASSPHRASE" --import

sbt publishSigned
