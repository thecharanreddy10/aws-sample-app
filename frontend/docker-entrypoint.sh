#!/bin/sh
set -eu

API_BASE_URL="${API_BASE_URL:-/api}"
printf 'window.__API_BASE_URL__ = "%s";\n' "$API_BASE_URL" > /usr/share/nginx/html/env.js

exec nginx -g 'daemon off;'
