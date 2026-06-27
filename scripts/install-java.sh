#!/usr/bin/env bash
set -euo pipefail

JDK_DIR="${JDK_DIR:-$HOME/.local/jdk-21}"
ARCH="$(uname -m)"
case "$ARCH" in
  arm64) PLATFORM="mac/aarch64" ;;
  x86_64) PLATFORM="mac/x64" ;;
  *) echo "Unsupported architecture: $ARCH"; exit 1 ;;
esac

if [ -x "$JDK_DIR/Contents/Home/bin/java" ]; then
  echo "JDK 21 already installed at $JDK_DIR"
  "$JDK_DIR/Contents/Home/bin/java" -version
  exit 0
fi

TMP_JDK="/tmp/jdk21/jdk-21.0.11+10"
if [ -x "$TMP_JDK/Contents/Home/bin/java" ]; then
  echo "Copying JDK from $TMP_JDK to $JDK_DIR..."
  mkdir -p "$(dirname "$JDK_DIR")"
  cp -R "$TMP_JDK" "$JDK_DIR"
  echo "JDK 21 installed at $JDK_DIR"
  "$JDK_DIR/Contents/Home/bin/java" -version
  exit 0
fi

echo "Downloading Eclipse Temurin JDK 21..."
TMP_ARCHIVE="$(mktemp -t jdk21.XXXXXX.tar.gz)"
curl -fsSL -o "$TMP_ARCHIVE" \
  "https://api.adoptium.net/v3/binary/latest/21/ga/${PLATFORM}/jdk/hotspot/normal/eclipse"
mkdir -p "$(dirname "$JDK_DIR")"
tar -xzf "$TMP_ARCHIVE" -C "$(dirname "$JDK_DIR")"
rm -f "$TMP_ARCHIVE"

EXTRACTED="$(find "$(dirname "$JDK_DIR")" -maxdepth 1 -type d -name 'jdk-21*' | head -1)"
if [ -z "$EXTRACTED" ]; then
  echo "Failed to locate extracted JDK directory"
  exit 1
fi
mv "$EXTRACTED" "$JDK_DIR"

echo "JDK 21 installed at $JDK_DIR"
"$JDK_DIR/Contents/Home/bin/java" -version
