#!/bin/bash
set -euo pipefail

# Always kill any leftover background jobs when exiting
trap 'echo "Exit signal received, killing background jobs..."; kill $(jobs -p) >/dev/null 2>&1 || true' EXIT

# ---- Config (overridable via env) ----
DURATION_SECONDS=${DURATION_SECONDS:-1800}     # 30 minutes target capture
GRACE_SECONDS=${GRACE_SECONDS:-120}           # extra time for reconnect/cleanup
OUTPUT_DIR=${OUTPUT_DIR:-live_output}
LOG_DIR=${LOG_DIR:-logs}
STATIONS_FILE=${STATIONS_FILE:-stations.csv}
# --------------------------------------

mkdir -p "$OUTPUT_DIR" "$LOG_DIR"
exec > >(tee -a "$LOG_DIR/main.log") 2>&1
echo "[$(date '+%F %T')] Workflow started."

PYTHON_EXEC="$(command -v python3 || command -v python)"
echo "Using Python: $($PYTHON_EXEC -V)"

# Normalize CRLF just in case
tmp_csv="$LOG_DIR/stations.normalized.csv"
tr -d '\r' < "$STATIONS_FILE" > "$tmp_csv"

echo "--- PHASE 1: RECORDING ---"
pids=()
names=()

# Start each station under a hard timeout so a hung ffmpeg can't stall the job.
# We also enable reconnect flags to handle transient network blips.
while IFS=, read -r name url lang; do
  [[ -z "${name// }" ]] && continue
  [[ "$name" =~ ^# ]] && continue

  outdir="$OUTPUT_DIR/$name"
  wav="$outdir/$name.wav"
  slog="$LOG_DIR/${name}.ffmpeg.log"
  mkdir -p "$outdir"

  echo "[$(date '+%F %T')] Launching recorder for $name"
  timeout "$(( DURATION_SECONDS + GRACE_SECONDS ))" \
    ffmpeg -hide_banner -loglevel info -nostdin \
           -reconnect 1 -reconnect_streamed 1 -reconnect_on_network_error 1 \
           -reconnect_on_http_error 4xx,5xx -rw_timeout 15000000 \
           -i "$url" -t "$DURATION_SECONDS" \
           -ac 1 -ar 16000 -c:a pcm_s16le "$wav" \
           2> "$slog" &

  pids+=($!)
  names+=("$name")
done < "$tmp_csv"

echo "Spawned ${#pids[@]} recorder(s). PIDs: ${pids[*]}"

# Wait for each recorder individually and log status; DO NOT exit on failure.
echo "Waiting for recorders..."
set +e  # don't exit on non-zero wait statuses
failed=0
for idx in "${!pids[@]}"; do
  pid="${pids[$idx]}"
  nm="${names[$idx]}"
  if wait "$pid"; then
    echo "[$(date '+%F %T')] $nm: recording finished (OK)"
  else
    rc=$?
    echo "[$(date '+%F %T')] $nm: recording FAILED (exit $rc)"
    failed=$((failed+1))
  fi
done
set -e

if (( failed > 0 )); then
  echo "[$(date '+%F %T')] Recording phase completed with $failed failure(s). Continuing to transcription."
else
  echo "[$(date '+%F %T')] Recording phase completed successfully."
fi

echo "--- PHASE 2: TRANSCRIBING ---"
# Transcribe whatever WAVs exist; Python will skip missing stations safely
"$PYTHON_EXEC" transcribe_only.py --dir "$OUTPUT_DIR" --stations "$STATIONS_FILE"

echo "[$(date '+%F %T')] Workflow complete."