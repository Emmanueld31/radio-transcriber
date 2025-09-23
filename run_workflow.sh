#!/bin/bash
set -euo pipefail
trap 'echo "Exit signal received, killing background jobs..."; kill $(jobs -p) >/dev/null 2>&1 || true' EXIT

# ---- Config (overridable via env) ----
DURATION_SECONDS=${DURATION_SECONDS:-1800}
GRACE_SECONDS=${GRACE_SECONDS:-120}
OUTPUT_DIR=${OUTPUT_DIR:-live_output}
LOG_DIR=${LOG_DIR:-logs}
STATIONS_FILE=${STATIONS_FILE:-stations.csv}
RETRIES=${RETRIES:-2}
SLEEP_BETWEEN_RETRIES=${SLEEP_BETWEEN_RETRIES:-5}
MIN_BYTES=${MIN_BYTES:-65536}
# --------------------------------------

mkdir -p "$OUTPUT_DIR" "$LOG_DIR"
exec > >(tee -a "$LOG_DIR/main.log") 2>&1
echo "[$(date '+%F %T')] Workflow started."

PYTHON_EXEC="$(command -v python3 || command -v python)"
echo "Using Python: $($PYTHON_EXEC -V)"

tmp_csv="$LOG_DIR/stations.normalized.csv"
tr -d '\r' < "$STATIONS_FILE" > "$tmp_csv"

fail_count=0
pids=()
names=()

ffmpeg_cmd() {
  local url="$1" out="$2" ua="$3" ref="$4"
  local ua_arg=() ref_arg=()
  [[ -n "${ua:-}" ]]  && ua_arg=( -user_agent "$ua" )
  [[ -n "${ref:-}" ]] && ref_arg=( -headers "Referer: $ref" )
  ffmpeg -hide_banner -loglevel info -nostdin \
         -reconnect 1 -reconnect_streamed 1 -reconnect_on_network_error 1 \
         -reconnect_on_http_error 4xx,5xx -rw_timeout 15000000 \
         "${ua_arg[@]}" "${ref_arg[@]}" \
         -i "$url" -t "$DURATION_SECONDS" \
         -ac 1 -ar 16000 -c:a pcm_s16le "$out"
}

preflight() {
  local url="$1" ua="$2" ref="$3"
  local ua_arg=() ref_arg=()
  [[ -n "${ua:-}" ]]  && ua_arg=( -user_agent "$ua" )
  [[ -n "${ref:-}" ]] && ref_arg=( -headers "Referer: $ref" )
  timeout 15s ffprobe -v error "${ua_arg[@]}" "${ref_arg[@]}" -hide_banner -i "$url" -show_streams >/dev/null 2>&1
}

run_station() {
  local name="$1" url="$2" lang="$3" ua="$4" ref="$5"
  local outdir="$OUTPUT_DIR/$name"
  local wav="$outdir/$name.wav"
  local slog="$LOG_DIR/${name}.ffmpeg.log"
  mkdir -p "$outdir"

  echo "[$(date '+%F %T')] Preflight $name ..."
  if ! preflight "$url" "$ua" "$ref"; then
    echo "[$(date '+%F %T')] $name: preflight failed; skipping."
    echo "preflight failed" > "$slog"
    return 251
  fi

  local attempt=1 rc=0
  : > "$slog"
  while (( attempt <= RETRIES )); do
    echo "[$(date '+%F %T')] $name: attempt $attempt/$RETRIES"
    if timeout "$(( DURATION_SECONDS + GRACE_SECONDS ))" \
        ffmpeg_cmd "$url" "$wav" "$ua" "$ref" 2>>"$slog"; then
      rc=0
      break
    else
      rc=$?
      echo "[$(date '+%F %T')] $name: attempt $attempt failed (rc=$rc)"
      sleep "$SLEEP_BETWEEN_RETRIES"
    fi
    attempt=$((attempt+1))
  done

  if [[ $rc -eq 0 ]]; then
    if [[ ! -s "$wav" || $(stat -c%s "$wav") -lt $MIN_BYTES ]]; then
      echo "[$(date '+%F %T')] $name: wav too small, marking failed"
      rc=252
    fi
  fi

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