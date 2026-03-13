from __future__ import annotations

import argparse
import json
from functools import partial
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

from jobs import (
    _doc_token_sets,
    _ii_mapper,
    _ii_reducer,
    _pair_mapper,
    _pair_reducer,
    _pf_mapper,
    _pf_reducer,
    _wc_mapper,
    _wc_reducer,
)
from mr_core import KV, map_stage, reduce_stage, shuffle_stage


def _load_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _load_jsonl(path: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def _preview_pairs(pairs: Iterable[KV], limit: int) -> List[KV]:
    out: List[KV] = []
    for pair in pairs:
        out.append(pair)
        if len(out) >= limit:
            break
    return out


def _preview_grouped(grouped: Dict[str, List[Any]], limit: int) -> List[Tuple[str, int, List[Any]]]:
    # Returns (key, value_count, sample_values)
    items = sorted(grouped.items(), key=lambda kv: kv[0])
    out: List[Tuple[str, int, List[Any]]] = []
    for key, values in items[:limit]:
        out.append((key, len(values), values[: min(8, len(values))]))
    return out


def _show_task_pipeline(
    records: List[Dict[str, Any]],
    mapper,
    reducer,
    num_workers: int,
    limit: int,
    title: str,
) -> None:
    print(f"\n=== {title} ===")
    mapped = map_stage(records, mapper=mapper, num_workers=num_workers)
    print(f"map_count = {len(mapped)}")
    print("map_preview =", json.dumps(_preview_pairs(mapped, limit), ensure_ascii=False, indent=2))

    grouped = shuffle_stage(mapped)
    print(f"shuffle_key_count = {len(grouped)}")
    print("shuffle_preview =", json.dumps(_preview_grouped(grouped, limit), ensure_ascii=False, indent=2))

    reduced = reduce_stage(grouped, reducer=reducer, num_workers=num_workers)
    reduced.sort(key=lambda kv: kv[0])
    print(f"reduce_count = {len(reduced)}")
    print("reduce_preview =", json.dumps(_preview_pairs(reduced, limit), ensure_ascii=False, indent=2))


def _similarity_pipeline(
    records: List[Dict[str, Any]],
    config: Dict[str, Any],
    num_workers: int,
    limit: int,
) -> None:
    print("\n=== similarity (pair intersection stage) ===")
    doc_tokens = _doc_token_sets(records, config)
    print(f"doc_count = {len(doc_tokens)}")

    token_to_docs: Dict[str, List[str]] = {}
    for doc_id, tokens in doc_tokens.items():
        for token in tokens:
            token_to_docs.setdefault(token, []).append(doc_id)

    posting_records = [
        {"token": token, "docs": sorted(docs)}
        for token, docs in token_to_docs.items()
        if len(docs) >= 2
    ]
    print(f"posting_record_count = {len(posting_records)}")
    print("posting_preview =", json.dumps(posting_records[:limit], ensure_ascii=False, indent=2))

    mapped = map_stage(posting_records, mapper=_pair_mapper, num_workers=num_workers)
    print(f"pair_map_count = {len(mapped)}")
    print("pair_map_preview =", json.dumps(_preview_pairs(mapped, limit), ensure_ascii=False, indent=2))

    grouped = shuffle_stage(mapped)
    print(f"pair_shuffle_key_count = {len(grouped)}")
    print("pair_shuffle_preview =", json.dumps(_preview_grouped(grouped, limit), ensure_ascii=False, indent=2))

    reduced = reduce_stage(grouped, reducer=_pair_reducer, num_workers=num_workers)
    reduced.sort(key=lambda kv: (-int(kv[1]), kv[0]))
    print(f"pair_reduce_count = {len(reduced)}")
    print("pair_reduce_preview =", json.dumps(_preview_pairs(reduced, limit), ensure_ascii=False, indent=2))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Inspect intermediate MapReduce outputs for debugging.")
    parser.add_argument("--student-dir", required=True, help="Path like student_release/2025012345")
    parser.add_argument(
        "--task",
        default="all",
        choices=["all", "word_count", "inverted_index", "prefix_filter", "similarity"],
        help="Which task pipeline to inspect",
    )
    parser.add_argument("--prefix", default="trans", help="Prefix for prefix_filter")
    parser.add_argument("--limit", type=int, default=5, help="How many preview items to print")
    parser.add_argument("--num-workers", type=int, default=None, help="Override worker count")
    parser.add_argument("--record-limit", type=int, default=30, help="Use first N records for quick inspection")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    student_dir = Path(args.student_dir).resolve()
    papers_path = student_dir / "papers.jsonl"
    config_path = student_dir / "config.json"
    records = _load_jsonl(papers_path)
    config = _load_json(config_path)

    if args.record_limit > 0:
        records = records[: args.record_limit]

    num_workers = int(args.num_workers) if args.num_workers is not None else int(config["num_workers"])
    base_mapper_kwargs = {
        "min_token_len": int(config["min_token_len"]),
        "remove_stopwords": bool(config["remove_stopwords"]),
    }

    print("student_dir =", str(student_dir))
    print("record_count_used =", len(records))
    print("num_workers =", num_workers)

    if args.task in ("all", "word_count"):
        mapper = partial(_wc_mapper, **base_mapper_kwargs)
        _show_task_pipeline(records, mapper, _wc_reducer, num_workers, args.limit, "word_count")

    if args.task in ("all", "inverted_index"):
        mapper = partial(_ii_mapper, **base_mapper_kwargs)
        _show_task_pipeline(records, mapper, _ii_reducer, num_workers, args.limit, "inverted_index")

    if args.task in ("all", "prefix_filter"):
        mapper = partial(_pf_mapper, prefix=args.prefix.lower(), **base_mapper_kwargs)
        _show_task_pipeline(records, mapper, _pf_reducer, num_workers, args.limit, "prefix_filter")

    if args.task in ("all", "similarity"):
        _similarity_pipeline(records, config, num_workers, args.limit)


if __name__ == "__main__":
    main()
