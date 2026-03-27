#!/usr/bin/env python3
"""
extract_carolina.py
-------------------
Extracts plain Portuguese text from the Corpus Carolina dataset and writes
multiple .txt files, each capped at a given size limit (but never cutting
mid-sentence — the last sentence always completes before the file closes).

Supported dataset layouts (auto-detected):
  1. Git-cloned repo  → corpus/**/*.xml  (TEI P5 XML files)
  2. HuggingFace cache → Arrow/Parquet files loaded via the datasets library

Usage:
    python extract_carolina.py [--path /path/to/corpus-carolina] [--out ./output]

If --path is omitted the script tries:
  a) ./corpus-carolina
  b) ~/corpus-carolina
  c) The HuggingFace datasets cache (carolina-c4ai/corpus-carolina)
"""

import argparse
import os
import sys
import glob
import re
from pathlib import Path

# ---------------------------------------------------------------------------
# Size limits (bytes). Files may slightly exceed these to finish the sentence.
# ---------------------------------------------------------------------------
LIMITS = {
    "1MB":   1 * 1024 ** 2,
    "10MB":  10 * 1024 ** 2,
    "100MB": 100 * 1024 ** 2,
    "1GB":   1 * 1024 ** 3,
    "2GB":   2 * 1024 ** 3,
}

# Sentence boundary: end of string that looks like end-of-sentence punctuation
# followed by whitespace or end-of-text.
SENTENCE_END = re.compile(r'(?<=[.!?…»"\')\]])\s+')


def split_sentences(text: str) -> list[str]:
    """
    Very lightweight sentence splitter. Splits on sentence-ending punctuation
    followed by whitespace. Returns a list of sentence strings (with trailing
    space stripped). Falls back to splitting on newlines if no sentence
    boundaries are found.
    """
    parts = SENTENCE_END.split(text.strip())
    if len(parts) == 1:
        # Try newlines as a fallback boundary
        parts = [line.strip() for line in text.splitlines() if line.strip()]
    return [p for p in parts if p]


# ---------------------------------------------------------------------------
# TEI XML extraction
# ---------------------------------------------------------------------------

def extract_text_from_tei(xml_path: str) -> list[str]:
    """
    Parse a TEI P5 XML file and return a list of text strings, one per <text>
    or <body> element found. Falls back to stripping all XML tags if the
    expected tags are not present.
    """
    try:
        from lxml import etree as ET
        parser = ET.XMLParser(recover=True, encoding="utf-8")
        tree = ET.parse(xml_path, parser)
        root = tree.getroot()

        # TEI namespace (optional — Carolina files may or may not declare it)
        ns = {"tei": "http://www.tei-c.org/ns/1.0"}
        texts = []

        # Try <text> elements first
        for node in root.iter("{http://www.tei-c.org/ns/1.0}text"):
            raw = "".join(node.itertext())
            if raw.strip():
                texts.append(raw.strip())

        # If nothing found, try without namespace
        if not texts:
            for node in root.iter("text"):
                raw = "".join(node.itertext())
                if raw.strip():
                    texts.append(raw.strip())

        # Last resort: whole document text
        if not texts:
            raw = "".join(root.itertext())
            if raw.strip():
                texts.append(raw.strip())

        return texts

    except Exception as e:
        print(f"  [WARN] Could not parse {xml_path}: {e}", file=sys.stderr)
        return []


def iter_xml_texts(corpus_root: str):
    """
    Generator: yields (source_file, text_string) for every text found in
    every XML file under corpus_root.
    """
    pattern = os.path.join(corpus_root, "**", "*.xml")
    files = sorted(glob.glob(pattern, recursive=True))
    if not files:
        print(f"[ERROR] No XML files found under '{corpus_root}'.", file=sys.stderr)
        sys.exit(1)

    print(f"[INFO] Found {len(files)} XML files under '{corpus_root}'.")
    for fpath in files:
        for text in extract_text_from_tei(fpath):
            yield fpath, text


# ---------------------------------------------------------------------------
# HuggingFace datasets extraction
# ---------------------------------------------------------------------------

def iter_hf_texts():
    """
    Generator: yields (source_id, text_string) by streaming from the
    HuggingFace hub (carolina-c4ai/corpus-carolina).
    Requires: pip install datasets
    """
    try:
        from datasets import load_dataset
    except ImportError:
        print("[ERROR] 'datasets' package not installed. Run: pip install datasets",
              file=sys.stderr)
        sys.exit(1)

    print("[INFO] Loading Corpus Carolina from HuggingFace (streaming)...")
    ds = load_dataset(
        "carolina-c4ai/corpus-carolina",
        split="corpus",
        streaming=True,
        trust_remote_code=True,
    )
    for i, item in enumerate(ds):
        text = item.get("text", "")
        if text and text.strip():
            yield f"hf:{i}", text.strip()


# ---------------------------------------------------------------------------
# Writer
# ---------------------------------------------------------------------------

def write_limited_files(text_iter, out_dir: str, limits: dict[str, int]):
    """
    Consume text_iter (yields (source, text) tuples) once, writing to all
    limit-files simultaneously. Each file stops accepting new sentences once
    its limit is reached, but the current sentence always finishes writing.

    Files are named  carolina_<LABEL>.txt  (e.g. carolina_1MB.txt).
    """
    os.makedirs(out_dir, exist_ok=True)

    # Sort limits ascending so we can close files as we go
    sorted_limits = sorted(limits.items(), key=lambda x: x[1])
    active = []  # list of (label, limit_bytes, file_handle, bytes_written, done)

    for label, limit in sorted_limits:
        out_path = os.path.join(out_dir, f"carolina_{label}.txt")
        fh = open(out_path, "w", encoding="utf-8")
        active.append({"label": label, "limit": limit, "fh": fh,
                        "written": 0, "done": False})
        print(f"[INFO] Writing  → {out_path}  (limit: {label})")

    total_texts = 0
    total_sentences = 0

    for source, text in text_iter:
        sentences = split_sentences(text)
        for sentence in sentences:
            line = sentence + "\n"
            line_bytes = line.encode("utf-8")
            line_len = len(line_bytes)

            for slot in active:
                if slot["done"]:
                    continue
                slot["fh"].write(line)
                slot["written"] += line_len
                # Close file once it has exceeded its limit AND this sentence ended
                if slot["written"] >= slot["limit"]:
                    slot["fh"].close()
                    size_mb = slot["written"] / 1024 ** 2
                    print(f"[DONE] {slot['label']} file closed "
                          f"({size_mb:.2f} MB written).")
                    slot["done"] = True

            total_sentences += 1

        total_texts += 1
        if total_texts % 1000 == 0:
            still_open = sum(1 for s in active if not s["done"])
            print(f"[INFO] Processed {total_texts} texts, "
                  f"{total_sentences} sentences. "
                  f"{still_open} file(s) still open.")

        # Stop iterating only when ALL files are done
        if all(s["done"] for s in active):
            print("[INFO] All size limits reached. Stopping early.")
            break

    # Close any files that were not yet closed (corpus exhausted before limit)
    for slot in active:
        if not slot["done"]:
            slot["fh"].close()
            size_mb = slot["written"] / 1024 ** 2
            print(f"[DONE] {slot['label']} file closed at end of corpus "
                  f"({size_mb:.2f} MB written — corpus exhausted before limit).")

    print(f"\n[SUMMARY] {total_texts} texts, {total_sentences} sentences processed.")
    print(f"[SUMMARY] Output files written to: {os.path.abspath(out_dir)}")


# ---------------------------------------------------------------------------
# Auto-detect corpus location
# ---------------------------------------------------------------------------

def find_corpus_root(user_path: str | None) -> tuple[str, str]:
    """
    Returns (corpus_root, mode) where mode is 'xml' or 'hf'.
    """
    candidates = []

    if user_path:
        candidates.append(user_path)

    candidates += [
        "./corpus-carolina",
        os.path.expanduser("~/corpus-carolina"),
        "./corpus",          # repo root directly named corpus
    ]

    for path in candidates:
        if os.path.isdir(path):
            # Check if there are XML files inside
            has_xml = bool(glob.glob(os.path.join(path, "**", "*.xml"),
                                     recursive=True))
            if has_xml:
                print(f"[INFO] Found corpus XML files at: {path}")
                return path, "xml"

    print("[INFO] No local XML corpus found. Falling back to HuggingFace datasets.")
    return "", "hf"


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Extract Portuguese text from Corpus Carolina into "
                    "size-limited .txt files.")
    parser.add_argument(
        "--path", default=None,
        help="Path to the corpus-carolina directory (containing XML files). "
             "Auto-detected if omitted.")
    parser.add_argument(
        "--out", default="./carolina_output",
        help="Output directory for .txt files (default: ./carolina_output).")
    args = parser.parse_args()

    corpus_root, mode = find_corpus_root(args.path)

    if mode == "xml":
        text_iter = iter_xml_texts(corpus_root)
    else:
        text_iter = iter_hf_texts()

    write_limited_files(text_iter, args.out, LIMITS)


if __name__ == "__main__":
    main()
