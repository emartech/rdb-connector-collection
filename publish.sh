#!/usr/bin/env bash
set -e

echo "$PGP_SECRET" | base64 --decode | gpg --batch --passphrase "$PGP_PASSPHRASE" --import

sbt publishSigned
