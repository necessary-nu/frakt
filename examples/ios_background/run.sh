#!/bin/sh
# Build the background-download harness for the iOS simulator and install
# it. Usage: examples/ios_background/run.sh <simulator udid>
# Then drive it with:
#   xcrun simctl launch <udid> se.brendan.frakt.bgharness enqueue <job> <url>
#   xcrun simctl launch <udid> se.brendan.frakt.bgharness attach
#   xcrun simctl launch <udid> se.brendan.frakt.bgharness jobs
#   xcrun simctl launch <udid> se.brendan.frakt.bgharness cancel <job>
# and read the events it logs:
#   cat "$(xcrun simctl get_app_container <udid> se.brendan.frakt.bgharness data)/Documents/events.log"
set -eu
udid="$1"
here="$(cd "$(dirname "$0")" && pwd)"
root="$(cd "$here/../.." && pwd)"
cd "$root"
# nsurlsessiond takes a client's bundle ID from its application-identifier
# entitlement and rejects background sessions from processes without one.
# Simulator apps carry entitlements in a __TEXT,__entitlements section
# rather than in their (ad-hoc) signature, as Xcode builds them.
build() {
    cargo rustc --example ios_background --target aarch64-apple-ios-sim \
        --no-default-features --features backend-foundation "$@" -- \
        -C "link-arg=-Wl,-sectcreate,__TEXT,__entitlements,$here/Entitlements.plist"
}
build
exe="$(build --message-format=json 2>/dev/null \
    | sed -n 's/.*"executable":"\([^"]*ios_background[^"]*\)".*/\1/p' | tail -1)"
app="$root/target/aarch64-apple-ios-sim/frakt-bgharness.app"
rm -rf "$app"
mkdir -p "$app"
cp "$exe" "$app/ios_background"
cp "$here/Info.plist" "$app/Info.plist"
codesign --force --sign - "$app"
xcrun simctl install "$udid" "$app"
echo "installed $app"
