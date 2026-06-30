#!/bin/sh

# Seed writable runtime config tree from baked package configs.
# Some packages (e.g. ccsdspy) require their *_CONFIGDIR env var to point at a
# writable directory, but the Lambda image filesystem is read-only at runtime
# (only /tmp is writable). We mirror the baked /lambda_function/config/ tree
# into /tmp/config/ here, before the Python runtime starts. AWS Lambda mounts
# a fresh /tmp per execution environment, so this runs on every cold start;
# warm starts re-copy harmlessly (idempotent). See src/config/README.md.
if [ -d "/lambda_function/config" ]; then
    mkdir -p /tmp/config
    cp -R /lambda_function/config/. /tmp/config/
fi

if [ -z "${AWS_LAMBDA_RUNTIME_API}" ]; then
    exec /usr/local/bin/aws-lambda-rie python3 -m awslambdaric $@
else
    exec python3 -m awslambdaric $@
fi