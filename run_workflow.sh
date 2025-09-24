#!/bin/bash
set -euo pipefail
trap 'echo "Exit signal received, killing background jobs..."; kill $(jobs -p) >/dev/null 2>&1 || true' EXIT

# ---- Config (overridable via env) ----
DURATION_SECONDS=${DURATION_SECONDS:-1800}       # target capture
GRACE_SECONDS=${GRACE_SECONDS:-120}             # extra time for reconnect/cleanup
OUTPUT_DIR=${OUTPUT_DIR:-live_output}
LOG_DIR=${LOG_DIR:-logs}
STATIONS_FILE=${STATIONS_FILE:-stations.csv}
RETRIES=${RETRIES:-2}
SLEEP_BETWEEN_RETRIES=${SLEEP_BETWEEN_RETRIES:-5}
MIN_BYTES=${MIN_BYTES:-65536}                   # <64KB => treat as failed
# --------------------------------------

mkdir -p "$OUTPUT_DIR" "$LOG_DIR"
exec > >(tee -a "$LOG_DIR/main.log") 2>&1
echo "[$(date '+%F %T')] Workflow started."

PYTHON_EXEC="$(command -v python3 || command -v python)"
echo "Using Python: $($PYTHON_EXEC -V)"

# Normalize CRLF
tmp_csv="$LOG_DIR/stations.normalized.csv"
tr -d '\r' < "$STATIONS_FILE" > "$tmp_csv"

fail_count=0
pids=()
names=()

# Preflight with ffprobe + HEAD content-type check to avoid HTML pages
preflight() {
  local url="$1" ua="$2" ref="$3"
  local curl_args=( -sIL --max-time 10 )
  [[ -n "$ua"  ]] && curl_args+=( -H "User-Agent: $ua" )
  [[ -n "$ref" ]] && curl_args+=( -H "Referer: $ref" )
  # Accept typical audio/HLS types
  if ! CT=$(curl "${curl_args[@]}" "$url" | awk -F': *' 'tolower($1)=="content-type"{print tolower($2)}' | tail -1); then
    CT=""
  fi
  if [[ -n "$CT" ]] && ! [[ "$CT" =~ ^audio/|^application/vnd.apple.mpegurl|^application/x-mpegurl|^video/mp2t ]]; then
    echo "Preflight: content-type looks non-audio ($CT) → likely HTML; skipping."
    return 1
  fi
  # ffprobe sanity
  local ff_args=()
  [[ -n "$ua"  ]] && ff_args+=( -user_agent "$ua" )
  [[ -n "$ref" ]] && ff_args+=( -headers "Referer: $ref" )
  timeout 15s ffprobe -v error "${ff_args[@]}" -hide_banner -i "$url" -show_streams >/dev/null 2>&1
}

run_station() {
  local name="$1" url="$2" lang="$3" ua="$4" ref="$5"
  local outdir="$OUTPUT_DIR"
  local wav="$OUTPUT_DIR/${name}.wav"
  local slog="$LOG_DIR/${name}.ffmpeg.log"
  mkdir -p "$outdir"

  echo "[$(date '+%F %T')] PreFlight $name ..."
  if ! preflight "$url" "$ua" "$ref"; then
    echo "[$(date '+%F %T')] $name: preflight failed; skipping."
    echo "preflight failed" > "$slog"
    return 251
  fi

  : > "$slog"
  local attempt rc
  attempt=1
  while (( attempt <= RETRIES )); do
    echo "[$(date '+%F %T')] $name: attempt $attempt/$RETRIES"
    # Build ffmpeg command as an array (so timeout can exec it)
    cmd=( ffmpeg -hide_banner -loglevel info -nostdin
          -reconnect 1 -reconnect_streamed 1 -reconnect_on_network_error 1
          -reconnect_on_http_error 4xx,5xx -rw_timeout 15000000 )
    [[ -n "$ua " ]] && cmd+=( -user_agent "$ua" )
    [[ -n "$ref" ]] && cmd+=( -headers "Referer: $ref" )
    cmd+=( -i "$url" -t "$DURATION_SECONDS" -ac 1 -ar 16000 -c:a pcm_s16le "$wav" )

    if timeout "$(( DURATION_SECONDS + GRACE_SECONDS ))" "${cmd[@]}" 2>>"$slog"; then
      rc=0
    else
      rc=$?
      echo "[$(date '+%F %T')] $name: ffmpeg failed (rc=$rc)"
    fi

    # Post-check size
    if [[ $rc -eq 0 ]]; then
      if [[ ! -s "$wav" || $(stat -c%s "$wav") -lt $MIN_BYTES ]]; then
        echo "[$(date '+%F %T')] $name: wav too small; treating as failure"
        rc=252
      fi
    fi

    (( rc==0 )) && break
    (( attempt++ ))
    sleep "$SLEEP_BETWEEN_RETRIES"
  done

  if [[ $rc -ne 0 ]]; then
    echo "------ LAST 40 LINES ($name ffmpeg) ------"
    tail -n 40 "$slog" || true
    echo "------------------------------------------"
  fi
  return "$rc"
}

echo "--- PHASE 1: RECORDING ---"
while IFS=, read -r name url lang ua ref; do
  [[ -z "${name// }" ]] && continue
  [[ "$name" =~ ^# ]] && continue
  ( run_station "$name" "$url" "$lang" "${ua:-}" "${ref:-}" ) &
  pids+=($!)
  names+=("$name")
done < "$tmp_csv"

echo "Spawned ${#pids[@]} recorder(s). Waiting..."
set +e
for idx in "${!pids[@]}"; do
  pid="${pids[$idx]}"
  nm="${names[$idx]}"
  if wait "$pid"; then
    echo "[$(date '+%F %T')] $nm: finished (OK)"
  else
    rc=$?
    echo "[$(date '+%F %T')] $nm: FAILED (exit $rc)"
    fail_count=$((fail_count+1))
  fi
done
set -e

echo "[$(date '+%F %T')] Recording phase complete. Failures: $fail_count"

echo "--- PHASE 2: TRANSCRIBING ---"
"$PYTHON_EXEC" transcribe_only.py --dir "$OUTPUT_DIR" --stations "$STATIONS_FILE"
echo "[$(date '+%F %T')] Workflow complete."
