#!/bin/bash

echo "Hello"
python --version
python scripts/affiliate-server "$AFFILIATE_CONFIG" --bind :31337
