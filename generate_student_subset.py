import argparse
import hashlib
import json
import os
import platform
import random
import re
import socket
import uuid
from pathlib import Path
from typing import Any, Dict, List


def load_jsonl(path: str) -> List[Dict[str, Any]]:
    records = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


def save_jsonl(records: List[Dict[str, Any]], path: str) -> None:
    with open(path, "w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def machine_meta() -> Dict[str, str]:
    username = os.getenv("USERNAME") or os.getenv("USER") or "unknown_user"
    meta = {
        "hostname": socket.gethostname(),
        "username": username,
        "platform": platform.platform(),
        "python_version": platform.python_version(),
        "mac_addr": str(uuid.getnode()),
    }
    meta_json = json.dumps(meta, sort_keys=True, ensure_ascii=False)
    meta_hash = hashlib.sha256(meta_json.encode("utf-8")).hexdigest()
    meta["machine_hash"] = meta_hash
    return meta


def stable_seed(student_id: str, machine_hash: str) -> int:
    raw = f"{student_id}|{machine_hash}"
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()
    return int(digest[:16], 16)


def choose_config(seed: int) -> Dict[str, Any]:
    rng = random.Random(seed)
    threshold = rng.choice([0.18, 0.20, 0.22, 0.25, 0.28])
    min_token_len = rng.choice([2, 3, 4])
    top_k = rng.choice([10, 15, 20])
    remove_stopwords = rng.choice([True, True, True, False])
    num_workers = rng.choice([2, 4])
    return {
        "threshold": threshold,
        "min_token_len": min_token_len,
        "top_k": top_k,
        "remove_stopwords": remove_stopwords,
        "num_workers": num_workers,
    }


def stratified_sample(records: List[Dict[str, Any]], sample_size: int, rng: random.Random) -> List[Dict[str, Any]]:
    by_primary = {}
    for r in records:
        key = r.get("primary_category", "unknown")
        by_primary.setdefault(key, []).append(r)

    categories = sorted(by_primary.keys())
    if not categories:
        return []

    selected = []
    per_bucket = max(1, sample_size // len(categories))

    for cat in categories:
        bucket = by_primary[cat][:]
        rng.shuffle(bucket)
        selected.extend(bucket[: min(per_bucket, len(bucket))])

    if len(selected) < sample_size:
        remaining_ids = {r["id"] for r in selected}
        rest = [r for r in records if r["id"] not in remaining_ids]
        rng.shuffle(rest)
        selected.extend(rest[: sample_size - len(selected)])

    if len(selected) > sample_size:
        rng.shuffle(selected)
        selected = selected[:sample_size]

    selected.sort(key=lambda x: x["id"])
    return selected


def dataset_hash(records: List[Dict[str, Any]]) -> str:
    joined = "\n".join(r["id"] for r in records)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


def sanitize_student_id(student_id: str) -> str:
    return re.sub(r"[^A-Za-z0-9_-]", "_", student_id)


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate per-student dataset subset from master dataset.")
    parser.add_argument("--student-id", required=True, help="Student ID")
    parser.add_argument("--input", default="master_arxiv_cs_ai_2026_to_2026_03_12.jsonl")
    parser.add_argument("--sample-size", type=int, default=400)
    parser.add_argument("--output-dir", default="student_release")
    args = parser.parse_args()

    records = load_jsonl(args.input)
    meta = machine_meta()
    seed = stable_seed(args.student_id, meta["machine_hash"])
    rng = random.Random(seed)
    subset = stratified_sample(records, args.sample_size, rng)
    config = choose_config(seed)

    student_key = sanitize_student_id(args.student_id)
    out_dir = Path(args.output_dir) / student_key
    out_dir.mkdir(parents=True, exist_ok=True)

    dataset_path = out_dir / "papers.jsonl"
    config_path = out_dir / "config.json"
    runmeta_path = out_dir / "assigned_meta.json"

    save_jsonl(subset, str(dataset_path))
    with open(config_path, "w", encoding="utf-8") as f:
        json.dump(config, f, ensure_ascii=False, indent=2)

    assigned_meta = {
        "student_id": args.student_id,
        "machine_hash": meta["machine_hash"],
        "machine_meta_used": {
            "hostname": meta["hostname"],
            "username": meta["username"],
            "platform": meta["platform"],
            "python_version": meta["python_version"],
            "mac_addr": meta["mac_addr"],
        },
        "seed": str(seed),
        "dataset_hash": dataset_hash(subset),
        "sample_size": len(subset),
        "config": config,
        "note": "Please disclose environment fingerprint collection in the assignment notice.",
    }
    with open(runmeta_path, "w", encoding="utf-8") as f:
        json.dump(assigned_meta, f, ensure_ascii=False, indent=2)

    print(f"Generated files in: {out_dir}")
    print(f"  - {dataset_path}")
    print(f"  - {config_path}")
    print(f"  - {runmeta_path}")


if __name__ == "__main__":
    main()
