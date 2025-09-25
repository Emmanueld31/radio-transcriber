#!/usr/bin/env python3
import argparse, csv, os, statistics, sys
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo
from faster_whisper import WhisperModel

# ===== Defaults (override via flags or env) =====
MODEL_SIZE        = os.environ.get("WHISPER_MODEL", "small")
CPU_THREADS_DEF   = int(os.environ.get("CT2_CPU_THREADS", "2"))     # good for GitHub 2 vCPU
VAD_ON_DEF        = os.environ.get("WHISPER_VAD", "1") == "1"       # VAD enabled by default
PROBE_SECONDS_DEF = int(os.environ.get("WHISPER_PROBE_SECONDS", "90"))
THRESH_GOOD_DEF   = float(os.environ.get("WHISPER_THRESH_GOOD", "-0.80"))
THRESH_BAD_DEF    = float(os.environ.get("WHISPER_THRESH_BAD", "-0.90"))
GREEDY_BEAM_DEF   = int(os.environ.get("WHISPER_GREEDY_BEAM", "1"))
FALLBACK_BEAM_DEF = int(os.environ.get("WHISPER_FALLBACK_BEAM", "5"))
# =================================================

def load_station_meta(stations_csv: Path):
    """
    Return dict[name] -> (lang, cc) from CSV rows like:
      name,url,lang,cc[,ua,referer]
    or:
      name,url,lang[,ua,referer]
    Header is optional; lines starting with '#' are ignored.
    """
    meta = {}
    with stations_csv.open("r", encoding="utf-8") as f:
        for raw in f:
            s = raw.strip()
            if not s or s.startswith("#"):
                continue
            row = next(csv.reader([s]))
            row = [c.strip() for c in row]
            if len(row) < 3:
                continue
            name, url, lang = row[0], row[1], row[2]
            cc = ""
            # If a 4th col exists and looks like ISO-2
            if len(row) >= 4 and len(row[3]) == 2:
                cc = row[3].upper()
            if name and lang:
                meta[name] = (lang, cc)
    return meta

def mean_logprob(segments) -> float:
    vals = []
    for s in segments:
        lp = getattr(s, "avg_logprob", None)
        if lp is not None:
            vals.append(lp)
    return statistics.mean(vals) if vals else -1.5

def probe_confidence(model: WhisperModel, wav: Path, lang: str, vad: bool, probe_seconds: int) -> float:
    """
    Decode only the first ~probe_seconds by consuming the segment generator and breaking early.
    Returns mean avg_logprob (higher is better).
    """
    seg_gen, _info = model.transcribe(str(wav), language=lang, vad_filter=vad, beam_size=GREEDY_BEAM_DEF)
    taken = []
    cutoff = float(probe_seconds)
    elapsed = 0.0
    for seg in seg_gen:
        taken.append(seg)
        elapsed = getattr(seg, "end", elapsed)
        if elapsed and elapsed >= cutoff:
            break
    return mean_logprob(taken)

def full_transcribe(model: WhisperModel, wav: Path, lang: str, vad: bool, beam_size: int) -> str:
    seg_gen, _info = model.transcribe(str(wav), language=lang, vad_filter=vad, beam_size=beam_size)
    return " ".join(s.text.strip() for s in seg_gen)

def main():
    ap = argparse.ArgumentParser(description="Transcribe WAVs (flat dir) with probe-then-decide beam strategy.")
    ap.add_argument("--dir", type=Path, required=True, help="Directory containing .wav files (flat).")
    ap.add_argument("--stations", type=Path, required=True, help="Stations CSV (needs at least name,url,lang; optional cc).")
    ap.add_argument("--cpu-threads", type=int, default=CPU_THREADS_DEF, help=f"Threads per model (default {CPU_THREADS_DEF}).")
    ap.add_argument("--probe-seconds", type=int, default=PROBE_SECONDS_DEF, help=f"Probe duration in seconds (default {PROBE_SECONDS_DEF}).")
    ap.add_argument("--good-threshold", type=float, default=THRESH_GOOD_DEF, help=f"Mean logprob ≥ this → greedy (default {THRESH_GOOD_DEF}).")
    ap.add_argument("--bad-threshold", type=float, default=THRESH_BAD_DEF, help=f"Mean logprob < this → fallback beam (default {THRESH_BAD_DEF}).")
    ap.add_argument("--greedy-beam", type=int, default=GREEDY_BEAM_DEF, help=f"Beam size for greedy path (default {GREEDY_BEAM_DEF}).")
    ap.add_argument("--fallback-beam", type=int, default=FALLBACK_BEAM_DEF, help=f"Beam size for fallback path (default {FALLBACK_BEAM_DEF}).")
    ap.add_argument("--no-vad", action="store_true", help="Disable VAD (default is ON).")
    args = ap.parse_args()

    out_dir: Path = args.dir
    meta = load_station_meta(args.stations)
    wavs = sorted(out_dir.glob("*.wav"))
    if not wavs:
        print("No .wav files found.")
        return

    vad = not args.no_vad
    date_str = datetime.now(ZoneInfo("Europe/Amsterdam")).strftime("%y%m%d")

    print(f"Model={MODEL_SIZE} | cpu_threads={args.cpu_threads} | vad={vad} | "
          f"probe={args.probe_seconds}s | good≥{args.good_threshold:.2f} | bad<{args.bad_threshold:.2f} | "
          f"greedy={args.greedy_beam} | fallback={args.fallback_beam}")

    # One model instance (best for 2-core runner)
    model = WhisperModel(MODEL_SIZE, device="cpu", compute_type="int8", cpu_threads=args.cpu_threads)

    for i, wav in enumerate(wavs, 1):
        station = wav.stem
        lang, cc = meta.get(station, (None, ""))
        if not lang:
            print(f"[{i}/{len(wavs)}] Skip '{station}': not in stations CSV.")
            continue

        # 1) Probe a short window with greedy to estimate confidence
        conf = probe_confidence(model, wav, lang, vad, args.probe_seconds)
        # Decide path
        if conf < args.bad_threshold:
            chosen_beam = max(2, args.fallback_beam)
            path = f"fallback beam={chosen_beam}"
        else:
            chosen_beam = max(1, args.greedy_beam)
            # Optional: if conf between bad and good, you could set chosen_beam=3; we keep greedy for speed
            path = f"greedy beam={chosen_beam}"

        print(f"[{i}/{len(wavs)}] {station} | lang={lang} | mean_logprob_probe={conf:.3f} → {path}")

        # 2) Single full transcription with the chosen beam size
        text = full_transcribe(model, wav, lang, vad, chosen_beam)

        # Save
        cc_part = (cc or "XX")
        out_name = f"{date_str} - {cc_part} - {station} - Radio.txt"
        out_path = out_dir / out_name
        out_path.write_text(text, encoding="utf-8")
        print(f"-> saved {out_name}")

        # Clean up WAV
        wav.unlink(missing_ok=True)
        print(f"-> deleted {wav.name}")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
