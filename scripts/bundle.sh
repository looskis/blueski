#!/usr/bin/env bash
# Assemble a macOS .app bundle so Automation and Full Disk Access grants attach
# to a stable application identity during local development and releases.
#
# Usage: scripts/bundle.sh [debug|release]
#   SIGN_ID=<identity>  codesign identity: a certificate SHA-1 or name, or "-"
#                       for ad-hoc. Default: the longest-lived "Developer ID
#                       Application" identity in the keychain, else ad-hoc.
#
# macOS pins Full Disk Access and Automation grants to the app's designated
# requirement. A Developer ID signature pins them to the bundle identifier and
# team, so every later build inherits the grants. An ad-hoc signature pins them
# to the binary hash, so every rebuild must be granted again.
set -euo pipefail
cd "$(dirname "$0")/.."

PROFILE="${1:-debug}"
BIN_NAME="blueski"
APP_DISPLAY="Blueski"
IDENTIFIER="com.looskis.blueski"
VERSION="$(sed -n 's/^version = "\([^"]*\)"/\1/p' Cargo.toml | head -1)"
LOGO_DEFAULT=assets/blueski-icon.png

# Pick the Developer ID Application certificate that expires last. Two copies
# of the same name make a bare name ambiguous to codesign, so sign by hash.
developer_id() {
  local best="" best_exp=0 hash exp
  while read -r hash; do
    exp="$(security find-certificate -Z -p -c "Developer ID Application" -a 2>/dev/null \
      | awk -v want="$hash" '/^SHA-1 hash:/{cur=$3} /BEGIN CERT/{c=1;buf=""} c{buf=buf $0 "\n"} /END CERT/{c=0; if(cur==want) printf "%s", buf}' \
      | openssl x509 -noout -enddate 2>/dev/null | sed 's/notAfter=//')"
    [ -n "$exp" ] || continue
    exp="$(date -j -f "%b %e %T %Y %Z" "$exp" +%s 2>/dev/null || echo 0)"
    if [ "$exp" -gt "$best_exp" ]; then best="$hash"; best_exp="$exp"; fi
  done < <(security find-identity -v -p codesigning 2>/dev/null \
    | awk '/Developer ID Application/{print $2}')
  printf '%s' "$best"
}

if [ -z "${SIGN_ID:-}" ]; then
  SIGN_ID="$(developer_id)"
  if [ -z "$SIGN_ID" ]; then
    SIGN_ID="-"
    echo "warning: no Developer ID Application identity found; signing ad-hoc." >&2
    echo "warning: Full Disk Access and Automation grants will not survive the next build." >&2
  fi
fi
LOGO="${LOGO:-$LOGO_DEFAULT}"
ENTITLEMENTS="${ENTITLEMENTS:-assets/Blueski.entitlements}"

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
mkdir -p "$CONTENTS/MacOS" "$CONTENTS/Resources"
cp "$BIN" "$CONTENTS/MacOS/$BIN_NAME"

ICONSET="dist/AppIcon.iconset"
rm -rf "$ICONSET"
mkdir -p "$ICONSET"
for size in 16 32 128 256 512; do
  sips -z "$size" "$size" "$LOGO" --out "$ICONSET/icon_${size}x${size}.png" >/dev/null
  retina_size=$((size * 2))
  sips -z "$retina_size" "$retina_size" "$LOGO" --out "$ICONSET/icon_${size}x${size}@2x.png" >/dev/null
done
iconutil -c icns "$ICONSET" -o "$CONTENTS/Resources/AppIcon.icns"

cat > "$CONTENTS/Info.plist" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>CFBundleName</key>                 <string>${APP_DISPLAY}</string>
    <key>CFBundleDisplayName</key>          <string>${APP_DISPLAY}</string>
    <key>CFBundleIdentifier</key>           <string>${IDENTIFIER}</string>
    <key>CFBundleExecutable</key>           <string>${BIN_NAME}</string>
    <key>CFBundleIconFile</key>             <string>AppIcon</string>
    <key>CFBundlePackageType</key>          <string>APPL</string>
    <key>CFBundleShortVersionString</key>   <string>${VERSION}</string>
    <key>CFBundleVersion</key>              <string>${VERSION}</string>
    <key>LSMinimumSystemVersion</key>       <string>12.0</string>
    <key>LSUIElement</key>                  <true/>
    <key>NSAppleEventsUsageDescription</key>
    <string>Blueski uses Automation to send messages through Messages.</string>
</dict>
</plist>
PLIST

codesign --force --options runtime --entitlements "$ENTITLEMENTS" --sign "$SIGN_ID" "$APP"

echo "built $APP (profile: $PROFILE, signed: $SIGN_ID)"
echo "inner binary: $(pwd)/$CONTENTS/MacOS/$BIN_NAME"
codesign -dv "$APP" 2>&1 | sed 's/^/  /' || true
if [ "$SIGN_ID" = "-" ]; then
  echo "  designated requirement is the binary hash: TCC grants are per build"
else
  echo "  designated requirement: $(codesign -d -r- "$APP" 2>&1 | sed -n 's/^designated => //p')"
fi
