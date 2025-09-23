#!/usr/bin/env python3
import argparse, csv
from pathlib import Path
from faster_whisper import WhisperModel

MODEL_SIZE = "small"

def load_language_map(stations_csv: Path) -> dict:
    lang_map = {}
    with stations_csv.open("r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            row = next(csv.reader([s]))
            if len(row) != 3:
                continue
            name, url, lang = [c.strip() for c in row]
            if name:
                lang_map[name] = lang
    return lang_map

def transcribe_files(directory: Path, stations_csv: Path):
    language_map = load_language_map(stations_csv)

    print(f"Loading '{MODEL_SIZE}' model (this may take a moment)...")
    model = WhisperModel(MODEL_SIZE, device="cpu", compute_type="int8")
    print("Model loaded. Starting transcription.")

    wav_files = sorted(directory.glob("**/*.wav"))
    if not wav_files:
        print("No .wav files found to transcribe.")
        return

    print(f"Found {len(wav_files)} audio file(s) to process.")
    for i, wav_path in enumerate(wav_files, start=1):
        station_name = wav_path.stem
        language_code = language_map.get(station_name)
        if not language_code:
            print(f"-> Skipping '{wav_path.name}': '{station_name}' not found in {stations_csv}.")
            continue

        print(f"\n({i}/{len(wav_files)}) Transcribing '{wav_path.name}' (lang='{language_code}')...")
        segments, _ = model.transcribe(str(wav_path), language=language_code, vad_filter=False)
        full_transcript = " ".join(s.text.strip() for s in segments)
        txt_path = wav_path.with_suffix(".txt")
        txt_path.write_text(full_transcript, encoding="utf-8")
        print(f"-> Transcript saved to '{txt_path}'.")
        wav_path.unlink(missing_ok=True)
        print(f"-> Deleted '{wav_path.name}'.")

if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Transcribe audio files in a directory.")
    p.add_argument("--dir", type=Path, required=True, help="Directory containing the .wav files to process.")
    p.add_argument("--stations", type=Path, required=True, help="CSV with columns: name,url,lang")
    args = p.parse_args()
    transcribe_files(args.dir, args.stations)