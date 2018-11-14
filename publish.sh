#!/usr/bin/env bash

openssl enc -d -aes-256-cbc -K $ENCRYPTION_KEY -iv $ENCRYPTION_IV -in ci/secrets.tar.enc -out ci/secrets.tar
tar xv -C ci -f ci/secrets.tar
sbt $1/releaseEarly
