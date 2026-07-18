#!/usr/bin/env bash
# Assemble a macOS .app bundle so Automation and Full Disk Access grants attach
# to a stable application identity during local development and releases.
#
# Usage: scripts/bundle.sh [debug|release]
#   SIGN_ID=<identity>  codesign identity (default: ad-hoc)
set -euo pipefail
cd "$(dirname "$0")/.."

PROFILE="${1:-debug}"
BIN_NAME="blueski"
APP_DISPLAY="Blueski"
IDENTIFIER="com.razteam.blueski"
VERSION="$(sed -n 's/^version = "\([^"]*\)"/\1/p' Cargo.toml | head -1)"
SIGN_ID="${SIGN_ID:--}"

APP="dist/${APP_DISPLAY}.app"
CONTENTS="$APP/Contents"

if [ "$PROFILE" = "release" ]; then
  cargo build --release --locked
  BIN="target/release/$BIN_NAME"
else
  cargo build --locked
  BIN="target/debug/$BIN_NAME"
fi

rm -rf "$APP"
mkdir -p "$CONTENTS/MacOS"
cp "$BIN" "$CONTENTS/MacOS/$BIN_NAME"

cat > "$CONTENTS/Info.plist" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>CFBundleName</key>                 <string>${APP_DISPLAY}</string>
    <key>CFBundleDisplayName</key>          <string>${APP_DISPLAY}</string>
    <key>CFBundleIdentifier</key>           <string>${IDENTIFIER}</string>
    <key>CFBundleExecutable</key>           <string>${BIN_NAME}</string>
    <key>CFBundlePackageType</key>          <string>APPL</string>
    <key>CFBundleShortVersionString</key>   <string>${VERSION}</string>
    <key>CFBundleVersion</key>              <string>${VERSION}</string>
    <key>LSMinimumSystemVersion</key>       <string>12.0</string>
    <key>LSUIElement</key>                  <true/>
</dict>
</plist>
PLIST

codesign --force --options runtime --sign "$SIGN_ID" "$APP"

echo "built $APP (profile: $PROFILE, signed: $SIGN_ID)"
echo "inner binary: $(pwd)/$CONTENTS/MacOS/$BIN_NAME"
codesign -dv "$APP" 2>&1 | sed 's/^/  /' || true
