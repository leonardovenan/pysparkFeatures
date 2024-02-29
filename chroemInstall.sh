#!/bin/bash

LAST_VERSION="https://www.googleapis.com/download/storage/v1/b/chromium-browser-snapshots/o/Linux_x64%2FLAST_CHANGE?alt=media"
VERSION=$(curl -s -S "$LAST_VERSION")
if [ -z "$VERSION" ] ; then
  echo "Failed to retrieve version"
  exit 1
fi

if [ -d "/tmp/chrome/$VERSION" ] ; then
  echo "Version already installed"
  exit
fi

rm -rf "/tmp/chrome/$VERSION"
mkdir -p "/tmp/chrome/$VERSION"

URL="https://www.googleapis.com/download/storage/v1/b/chromium-browser-snapshots/o/Linux_x64%2F$VERSION%2Fchrome-linux.zip?alt=media"
ZIP="${VERSION}-chrome-linux.zip"

if ! curl -# "$URL" > "/tmp/chrome/$ZIP"; then
  echo "Failed to download Chrome"
  exit 1
fi

if ! unzip "/tmp/chrome/$ZIP" -d "/tmp/chrome/$VERSION"; then
  echo "Failed to unzip Chrome"
  exit 1
fi

URL="https://www.googleapis.com/download/storage/v1/b/chromium-browser-snapshots/o/Linux_x64%2F$VERSION%2Fchromedriver_linux64.zip?alt=media"
ZIP="${VERSION}-chromedriver_linux64.zip"

if ! curl -# "$URL" > "/tmp/chrome/$ZIP"; then
  echo "Failed to download Chromedriver"
  exit 1
fi

if ! unzip "/tmp/chrome/$ZIP" -d "/tmp/chrome/$VERSION"; then
  echo "Failed to unzip Chromedriver"
  exit 1
fi

mkdir -p /tmp/chrome/chrome-user-data-dir

rm -f /tmp/chrome/latest
ln -s "/tmp/chrome/$VERSION" /tmp/chrome/latest

# Garantindo que as dependências estejam instaladas antes de continuar
if ! sudo apt-get update && sudo apt-get install -y libgbm-dev; then
  echo "Failed to install dependencies"
  exit 1
fi

echo "Installation completed successfully"
