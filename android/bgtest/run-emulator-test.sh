#!/bin/sh
# Exercise frakt::background on a running emulator or device:
#
# 1. A download whose process is killed mid-transfer (as the system kills a
#    background process) is resumed by WorkManager with Range/If-Range in a
#    new process, and collected with the right BLAKE3 when the app is next
#    started.
# 2. After `am force-stop`, which cancels the app's jobs, the download does
#    not continue until the app is started again; then it resumes.
# 3. A 404 is reported as a failure that is not resumable, and a cancelled
#    job is listed while pending, then disappears with its files.
#
# Needs ANDROID_HOME and ANDROID_NDK_HOME, cargo-ndk and Gradle. The test
# server runs on this machine; the emulator reaches it as 10.0.2.2.
set -eu

cd "$(dirname "$0")/../.."
ADB="$ANDROID_HOME/platform-tools/adb"
PKG=se.brendan.frakt.bgtest
LEN=33554432
RATE=2097152

cargo ndk -t arm64-v8a build --release --example android_bg_harness \
    --no-default-features --features backend-android
mkdir -p android/bgtest/src/main/jniLibs/arm64-v8a
cp target/aarch64-linux-android/release/examples/libandroid_bg_harness.so \
    android/bgtest/src/main/jniLibs/arm64-v8a/
(cd android && gradle --no-daemon -q :bgtest:assembleDebug)
cargo build --release --example test_server

"$ADB" install -r android/bgtest/build/outputs/apk/debug/bgtest-debug.apk
"$ADB" shell pm clear "$PKG"

LOG=$(mktemp)
target/release/examples/test_server serve kill1 force1 >"$LOG" &
SERVER=$!
trap 'kill $SERVER' EXIT
sleep 1
BASE=$(head -1 "$LOG" | sed 's/127.0.0.1/10.0.2.2/')

app() {
    "$ADB" shell am start -W -n "$PKG/.MainActivity" "$@" >/dev/null
}

status() {
    "$ADB" shell "run-as $PKG sh -c 'cat files/frakt-background/*/$1.json 2>/dev/null'"
    echo
}

hexid() {
    printf '%s' "$1" | od -An -tx1 | tr -d ' \n'
}

events() {
    "$ADB" shell "run-as $PKG cat files/events.jsonl 2>/dev/null" | grep -v progress || true
}

wait_for() {
    job=$1
    state=$2
    for _ in $(seq 1 120); do
        if status "$(hexid "$job")" | grep -q "\"state\":\"$state\""; then
            return 0
        fi
        sleep 1
    done
    echo "timed out waiting for $job to be $state" >&2
    return 1
}

echo "== 1. process killed mid-download"
app --es action enqueue --es job kill1 \
    --es url "'$BASE/bytes?len=$LEN&seed=3&rate=$RATE&id=kill1'"
sleep 6
echo "before the kill: $(status "$(hexid kill1)")"
echo "process before the kill: $("$ADB" shell pidof "$PKG")"
"$ADB" shell am kill "$PKG"
sleep 1
if "$ADB" shell pidof "$PKG" >/dev/null; then
    echo "am kill left the process running (it holds a foreground service); killing it"
    "$ADB" shell "run-as $PKG kill -9 \$(pidof $PKG)" || true
fi
sleep 3
echo "process after the kill: $("$ADB" shell pidof "$PKG" || echo none)"
wait_for kill1 succeeded
echo "after the restart: $(status "$(hexid kill1)")"
echo "requests the server saw:"
grep "id=kill1" "$LOG"
app --es action collect
sleep 2
echo "events after relaunch:"
events
echo "expected BLAKE3: $(target/release/examples/test_server hash 3 $LEN)"

echo "== 2. force-stop mid-download"
app --es action enqueue --es job force1 \
    --es url "'$BASE/bytes?len=$LEN&seed=4&rate=$RATE&id=force1'"
sleep 6
"$ADB" shell am force-stop "$PKG"
echo "after force-stop: $(status "$(hexid force1)")"
sleep 20
echo "20 s later, requests so far:"
grep "id=force1" "$LOG"
# This launch also runs the poller, which takes the result as soon as the
# worker writes it, so wait for the event rather than the status.
app --es action collect
for _ in $(seq 1 120); do
    events | grep -q '"job":"force1"' && break
    sleep 1
done
echo "requests after the app started again:"
grep "id=force1" "$LOG"
echo "events:"
events
echo "expected BLAKE3: $(target/release/examples/test_server hash 4 $LEN)"

echo "== 3. failure, listing and cancelling"
app --es action enqueue --es job missing --es url "'$BASE/status?code=404&id=missing'"
for _ in $(seq 1 60); do
    events | grep -q '"job":"missing"' && break
    sleep 1
done
app --es action enqueue --es job doomed \
    --es url "'$BASE/bytes?len=$LEN&seed=5&rate=$RATE&id=doomed'"
sleep 4
app --es action jobs
app --es action cancel --es job doomed
sleep 3
app --es action jobs
"$ADB" logcat -d -s FraktHarness:I | tail -6
echo "files left for doomed: $("$ADB" shell "run-as $PKG sh -c 'ls files/frakt-background/*/ | grep $(hexid doomed) || echo none'")"
echo "events:"
events
