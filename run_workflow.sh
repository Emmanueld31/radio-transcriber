#!/usr/bin/env bash
set -euo pipefail

# Kill background jobs on exit, even if cancelled
trap 'echo "[trap] Stopping background recorders..."; kill $(jobs -p) >/dev/null 2>&1 || true' EXIT

# --- Config from env (with sensible defaults) ---
DURATION_SECONDS="${DURATION_SECONDS:-1800}"
GRACE_SECONDS="${GRACE_SECONDS:-120}"
OUTPUT_DIR="${OUTPUT_DIR:-live_output}"
LOG_DIR="${LOG_DIR:-logs}"
STATIONS_FILE="${STATIONS_FILE:-stations.csv}"

# Recording robustness
MAX_RETRIES="${MAX_RETRIES:-2}"               # total attempts per station (1 initial + retries-1)
SLEEP_BETWEEN_RETRIES="${SLEEP_BETWEEN_RETRIES:-5}"
MIN_BYTES="${MIN_BYTES:-65536}"               # <64KB => treat as failed
# ------------------------------------------------

mkdir -p "$OUTPUT_DIR" "$LOG_DIR"
exec > >(tee -a "$LOG_DIR/main.log") 2>&1

echo "=== Boot ==="
date -Is
uname -a
echo "Python: $(python -V)"
echo "PWD: $PWD"
echo "Stations file: $STATIONS_FILE"

# Sanity checks
command -v ffmpeg >/dev/null || { echo "ERROR: ffmpeg missing"; exit 1; }
command -v ffprobe >/dev/null || { echo "WARN: ffprobe not found"; }

# Normalize stations.csv line endings
sed -i 's/\r$//' "$STATIONS_FILE" || true

# --- Load stations: name,url,lang,cc[,ua,referer] OR name,url,lang[,ua,referer] ---
mapfile -t NAMES < <(awk -F',' 'BEGIN{OFS=","} /^[[:space:]]*#/ {next} NF>=3 {gsub(/^[[:space:]]+|[[:space:]]+$/,"",$1); if($1!="") print $1}' "$STATIONS_FILE")
mapfile -t URLS  < <(awk -F',' 'BEGIN{OFS=","} /^[[:space:]]*#/ {next} NF>=2 {gsub(/^[[:space:]]+|[[:space:]]+$/,"",$2); if($2!="") print $2}' "$STATIONS_FILE")
mapfile -t UAS   < <(awk -F',' 'BEGIN{OFS=","} /^[[:space:]]*#/ {next}      {gsub(/^[[:space:]]+|[[:space:]]+$/,"",$5); print $5}' "$STATIONS_FILE" 2>/dev/null || true)
mapfile -t REFS  < <(awk -F',' 'BEGIN{OFS=","} /^[[:space:]]*#/ {next}      {gsub(/^[[:space:]]+|[[:space:]]+$/,"",$6); print $6}' "$STATIONS_FILE" 2>/dev/null || true)

if [ "${#NAMES[@]}" -eq 0 ] || [ "${#URLS[@]}" -eq 0 ]; then
  echo "ERROR: No stations found in $STATIONS_FILE"; exit 1
fi
if [ "${#NAMES[@]}" -ne "${#URLS[@]}" ]; then
  echo "WARN: NAMES and URLS count differ; using the min length"
fi
COUNT=$(( ${#NAMES[@]} < ${#URLS[@]} ? ${#NAMES[@]} : ${#URLS[@]} ))

# helper: GNU stat size (we installed coreutils on GitHub runner)
file_size() { stat -c%s -- "$1" 2>/dev/null || echo 0; }

record_one() {
  local name="$1" url="$2" ua="$3" ref="$4"
  local wav="$OUTPUT_DIR/${name}.wav"
  local slog="$LOG_DIR/${name}.ffmpeg.log"
  local tmp="${wav}.part"

  # Build header flags if provided
  local extra_flags=()
  [ -n "$ua" ]  && extra_flags+=("-user_agent" "$ua")
  [ -n "$ref" ] && extra_flags+=("-headers" "Referer: $ref")

  local attempt=1
  : > "$slog"
  while [ "$attempt" -le "$MAX_RETRIES" ]; do
    echo "[$(date '+%F %T')] $name attempt $attempt/$MAX_RETRIES"

    # Run ffmpeg
    if ffmpeg -hide_banner -loglevel error -nostdin \
        -reconnect 1 -reconnect_streamed 1 -reconnect_on_network_error 1 \
        -reconnect_on_http_error 4xx,5xx -rw_timeout 15000000 \
        "${extra_flags[@]}" \
        -i "$url" -t "$DURATION_SECONDS" \
        -ac 1 -ar 16000 -c:a pcm_s16le "$tmp" 2>> "$slog"; then
      # Success exit code; now verify size
      local sz
      sz=$(file_size "$tmp")
      if [ "$sz" -ge "$MIN_BYTES" ]; then
        mv -f "$tmp" "$wav"
        echo "[$(date '+%F %T')] $name OK (bytes=$sz)"
        return 0
      else
        echo "[$(date '+%F %T')] $name too small (bytes=$sz < $MIN_BYTES), will retry"
        rm -f "$tmp"
      fi
    else
      echo "[$(date '+%F %T')] $name ffmpeg error, will retry"
      rm -f "$tmp"
    fi

    attempt=$((attempt+1))
    [ "$attempt" -le "$MAX_RETRIES" ] && sleep "$SLEEP_BETWEEN_RETRIES"
  done

  echo "[$(date '+%F %T')] $name FAILED after $MAX_RETRIES attempts"
  return 1
}

echo "=== PHASE 1: RECORDING (${COUNT} stations, ${DURATION_SECONDS}s) ==="
pids=()
names_for_pid=()
for ((i=0; i<COUNT; i++)); do
  name="${NAMES[$i]}"
  url="${URLS[$i]}"
  ua="${UAS[$i]:-}"
  ref="${REFS[$i]:-}"

  echo "launching recorder: $name"
  ( record_one "$name" "$url" "$ua" "$ref" ) &
  pids+=($!)
  names_for_pid+=("$name")
done

echo "PIDs: ${pids[*]}"
fail_count=0
for idx in "${!pids[@]}"; do
  pid="${pids[$idx]}"
  if ! wait "$pid"; then
    fail_count=$((fail_count+1))
  fi
done
echo "[$(date '+%F %T')] recording phase complete (failures: $fail_count)"

# Optional grace period
if [ "${GRACE_SECONDS:-0}" -gt 0 ]; then
  echo "Sleeping ${GRACE_SECONDS}s before transcription..."
  sleep "$GRACE_SECONDS"
fi

# --- PHASE 2: TRANSCRIBING ---
echo "=== PHASE 2: TRANSCRIBING ==="
tslog="$LOG_DIR/transcribe.log"

# Redacted tails for any failures
if [ "$fail_count" -gt 0 ]; then
  echo "Some recorders failed; last lines (redacted) from their logs:"
  for name in "${names_for_pid[@]}"; do
    slog="$LOG_DIR/${name}.ffmpeg.log"
    if [ -s "$slog" ]; then
      echo "----- $name (tail) -----"
      sed -E 's#https?://[^ ]+#<redacted>#g' "$slog" | tail -n 40 || true
    fi
  done
fi

python transcribe_only.py --dir "$OUTPUT_DIR" --stations "$STATIONS_FILE" 2>&1 | tee -a "$tslog"

echo "=== All done ==="
date -Is
