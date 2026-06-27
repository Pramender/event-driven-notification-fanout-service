#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

resolve_java_home() {
  if [ -n "${JAVA_HOME:-}" ] && [ -x "$JAVA_HOME/bin/java" ]; then
    echo "$JAVA_HOME"
    return 0
  fi

  for candidate in \
    "$HOME/.local/jdk-21/Contents/Home" \
    "/tmp/jdk21/jdk-21.0.11+10/Contents/Home" \
    "$(/usr/libexec/java_home -v 21 2>/dev/null || true)"
  do
    if [ -n "$candidate" ] && [ -x "$candidate/bin/java" ]; then
      echo "$candidate"
      return 0
    fi
  done

  return 1
}

if ! JAVA_HOME="$(resolve_java_home)"; then
  echo "JDK 21 not found. Install it with:"
  echo "  ./scripts/install-java.sh"
  echo
  echo "Or set JAVA_HOME manually, then re-run this script."
  exit 1
fi

export JAVA_HOME
export PATH="$JAVA_HOME/bin:$PATH"
cd "$ROOT"
exec ./mvnw "$@"
