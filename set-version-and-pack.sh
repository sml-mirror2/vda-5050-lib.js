#!/bin/sh

set -e

npm version v1.6.2-fixed
npm run build
cd ./dist
npm pack
