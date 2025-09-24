#!/usr/bin/env python3
import argparse, csv
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo
from faster_whisper import WhisperModel

MODEL_SIZE = "small"

def load_station_meta(stations_csv: Path):
    """Return dict[name] -> (lang, cc)."""
    meta = {}
    with stations_csv.open("r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            row = next(csv.reader([s]))
            # Expect: name,url,lang,cc[,ua,referer]
            if len(row) < 4:
                continue
            name, url, lang, cc, *rest = [c.strip() for c in row]
            if name and lang:
                meta[name] = (lang, (cc or "").upper())
    return meta

def transcribe_files(directory: Path, stations_csv: Path):
    meta = load_station_meta(stations_csv)

    print(f"Loading '{MODEL_SIZE}' model (this may take a moment)...")
    model = WhisperModel(MODEL_SIZE, device="cpu", compute_type="int8")
    print("Model loaded. Starting transcription.")

    wav_files = sorted(directory.glob("*.wav"))  # flat
    if not wav_files:
        print("No .wav files found to transcribe.")
        return

    print(f"Found {len(wav_files)} audio file(s) to process.")
    # Date in Amsterdam
    date_str = datetime.now(ZoneInfo("Europe/Amsterdam")).strftime("%y%m%d")

    for i, wav_path in enumerate(wav_files, start=1):
        station_name = wav_path.stem  # e.g., "FranceInfo"
        lang, cc = meta.get(station_name, (None, ""))

        if not lang:
            print(f"-> Skipping '{wav_path.name}': station '{station_name}' not in {stations_csv}.")
            continue

        print(f"\n({i}/{len(wav_files)}) Transcribing '{wav_path.name}' (lang='{lang}')...")
        segments, _ = model.transcribe(str(wav_path), language=lang, vad_filter=False)
        full_transcript = " ".join(s.text.strip() for s in segments)

        # YYMMDD - CC - Name_of_Radio - Radio.txt
        cc_part = cc if cc else "XX"
        txt_name = f"{date_str} - {cc_part} - {station_name} - Radio.txt"
        txt_path = directory / txt_name
        txt_path.write_text(full_transcript, encoding="utf-8")
        print(f"-> Transcript saved to '{txt_path.name}'.")

        wav_path.unlink(missing_ok=True)
        print(f"-> Deleted '{wav_path.name}'.")

if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Transcribe audio files in a directory.")
    p.add_argument("--dir", type=Path, required=True, help="Directory containing the .wav files to process.")
    p.add_argument("--stations", type=Path, required=True, help="CSV with columns: name,url,lang,cc[,ua,referer]")
    args = p.parse_args()
    transcribe_files(args.dir, args.stations)
