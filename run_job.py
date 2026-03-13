from __future__ import annotations

import argparse
import ast
import hashlib
import itertools
import json
import platform
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

from jobs import inverted_index_job, prefix_filter_job, similarity_job, word_count_job


EXPECTED_RUNNER_SHA256 = ""
LOCK_FILENAME = "run_job.sha256"
STOPWORDS = {
    "a",
    "an",
    "and",
    "are",
    "as",
    "at",
    "be",
    "by",
    "for",
    "from",
    "in",
    "is",
    "it",
    "of",
    "on",
    "or",
    "that",
    "the",
    "to",
    "with",
}


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _sha256_file(path: Path) -> str:
    return _sha256_bytes(path.read_bytes())


def _load_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _load_student_machine_hash(student_dir: Path) -> str | None:
    meta_path = student_dir / "assigned_meta.json"
    if not meta_path.exists():
        return None
    try:
        meta = _load_json(meta_path)
    except Exception:
        return None
    return meta.get("machine_hash")


def _load_jsonl(path: Path) -> List[Dict[str, Any]]:
    records: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


def _stable_hash_obj(obj: Any) -> str:
    payload = json.dumps(obj, ensure_ascii=False, sort_keys=True).encode("utf-8")
    return _sha256_bytes(payload)


def _extract_function_snippet(file_path: Path, function_name: str, max_lines: int = 80) -> Dict[str, Any]:
    source = file_path.read_text(encoding="utf-8")
    tree = ast.parse(source)
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == function_name:
            segment = ast.get_source_segment(source, node) or ""
            lines = segment.splitlines()
            snippet = "\n".join(lines[:max_lines])
            return {
                "file": str(file_path.name),
                "function": function_name,
                "source_sha256": _sha256_bytes(segment.encode("utf-8")),
                "source_excerpt": snippet,
                "line_count": len(lines),
            }
    return {
        "file": str(file_path.name),
        "function": function_name,
        "error": "function_not_found",
    }


def _resolve_expected_runner_hash(run_job_path: Path) -> str | None:
    if EXPECTED_RUNNER_SHA256:
        return EXPECTED_RUNNER_SHA256
    lock_file = run_job_path.with_name(LOCK_FILENAME)
    if lock_file.exists():
        raw = lock_file.read_text(encoding="utf-8").strip()
        if raw:
            return raw
    return None


def _integrity_info(run_job_path: Path, papers_path: Path, config_path: Path) -> Dict[str, Any]:
    actual_runner_hash = _sha256_file(run_job_path)
    expected_runner_hash = _resolve_expected_runner_hash(run_job_path)
    runner_match = expected_runner_hash == actual_runner_hash if expected_runner_hash else None
    return {
        "run_job_expected_sha256": expected_runner_hash,
        "run_job_actual_sha256": actual_runner_hash,
        "run_job_hash_match": runner_match,
        "dataset_sha256": _sha256_file(papers_path),
        "config_sha256": _sha256_file(config_path),
    }


def _run_one_task(task: str, records: List[Dict[str, Any]], config: Dict[str, Any], prefix: str) -> Dict[str, Any]:
    start = time.perf_counter()
    result = _run_task_raw(task, records, config, prefix)
    elapsed = round(time.perf_counter() - start, 6)
    return {
        "runtime_sec": elapsed,
        "num_output_keys": len(result),
        "output_sha256": _stable_hash_obj(result),
        "preview": result[:20],
    }


def _run_task_raw(task: str, records: List[Dict[str, Any]], config: Dict[str, Any], prefix: str) -> List[Any]:
    if task == "word_count":
        result = word_count_job(records, config)
    elif task == "inverted_index":
        result = inverted_index_job(records, config)
    elif task == "prefix_filter":
        result = prefix_filter_job(records, config, prefix=prefix)
    elif task == "similarity":
        result = similarity_job(records, config)
    else:
        raise ValueError(f"Unsupported task: {task}")
    return result


def _first_difference(left: List[Any], right: List[Any]) -> Dict[str, Any] | None:
    if len(left) != len(right):
        return {
            "reason": "length_mismatch",
            "left_len": len(left),
            "right_len": len(right),
        }
    for idx, (l_item, r_item) in enumerate(zip(left, right)):
        if l_item != r_item:
            return {
                "reason": "value_mismatch",
                "index": idx,
                "left_item": l_item,
                "right_item": r_item,
            }
    return None


def _normalize_tokens_ref(text: str, min_token_len: int, remove_stopwords: bool) -> List[str]:
    letters = []
    current = []
    for ch in text.lower():
        if "a" <= ch <= "z":
            current.append(ch)
        elif current:
            letters.append("".join(current))
            current = []
    if current:
        letters.append("".join(current))
    out = [tok for tok in letters if len(tok) >= min_token_len]
    if remove_stopwords:
        out = [tok for tok in out if tok not in STOPWORDS]
    return out


def _run_task_reference(task: str, records: List[Dict[str, Any]], config: Dict[str, Any], prefix: str) -> List[Any]:
    min_token_len = int(config["min_token_len"])
    remove_stopwords = bool(config["remove_stopwords"])
    top_k = int(config["top_k"])
    prefix = prefix.lower()

    if task == "word_count":
        counter: Dict[str, int] = {}
        for r in records:
            for token in _normalize_tokens_ref(r.get("text", ""), min_token_len, remove_stopwords):
                counter[token] = counter.get(token, 0) + 1
        items = sorted(counter.items(), key=lambda kv: (-int(kv[1]), kv[0]))
        return items[:top_k]

    if task == "inverted_index":
        token_docs: Dict[str, set[str]] = {}
        for r in records:
            doc_id = str(r["id"])
            tokens = set(_normalize_tokens_ref(r.get("text", ""), min_token_len, remove_stopwords))
            for token in tokens:
                if token not in token_docs:
                    token_docs[token] = set()
                token_docs[token].add(doc_id)
        items = [(token, sorted(docs)) for token, docs in token_docs.items()]
        items.sort(key=lambda kv: kv[0])
        return items

    if task == "prefix_filter":
        counter: Dict[str, int] = {}
        for r in records:
            for token in _normalize_tokens_ref(r.get("text", ""), min_token_len, remove_stopwords):
                if token.startswith(prefix):
                    counter[token] = counter.get(token, 0) + 1
        items = sorted(counter.items(), key=lambda kv: (-int(kv[1]), kv[0]))
        return items

    if task == "similarity":
        threshold = float(config["threshold"])
        doc_tokens: Dict[str, set[str]] = {}
        for r in records:
            doc_id = str(r["id"])
            doc_tokens[doc_id] = set(_normalize_tokens_ref(r.get("text", ""), min_token_len, remove_stopwords))

        pairs: List[Any] = []
        doc_ids = sorted(doc_tokens.keys())
        for left, right in itertools.combinations(doc_ids, 2):
            left_set = doc_tokens[left]
            right_set = doc_tokens[right]
            inter = len(left_set.intersection(right_set))
            if inter == 0:
                continue
            union = len(left_set) + len(right_set) - inter
            if union <= 0:
                continue
            score = inter / union
            if score >= threshold:
                pairs.append((f"{left}||{right}", round(score, 6)))
        pairs.sort(key=lambda kv: (-float(kv[1]), kv[0]))
        return pairs[:top_k]

    raise ValueError(f"Unsupported task: {task}")


def _correctness_check(task: str, records: List[Dict[str, Any]], config: Dict[str, Any], prefix: str) -> Dict[str, Any]:
    actual = _run_task_raw(task, records, config, prefix)
    expected = _run_task_reference(task, records, config, prefix)
    mismatch_detail = _first_difference(actual, expected)
    actual_hash = _stable_hash_obj(actual)
    expected_hash = _stable_hash_obj(expected)
    return {
        "match_reference": actual_hash == expected_hash,
        "actual_hash": actual_hash,
        "expected_hash": expected_hash,
        "actual_len": len(actual),
        "expected_len": len(expected),
        "mismatch_detail": mismatch_detail,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fixed entrypoint for fake MapReduce assignment.")
    parser.add_argument("--student-dir", required=True, help="Path like: student_release/2025012345")
    parser.add_argument(
        "--task",
        default="all",
        choices=["all", "word_count", "inverted_index", "prefix_filter", "similarity"],
        help="Task selector",
    )
    parser.add_argument("--prefix", default="trans", help="Prefix used by prefix_filter task")
    parser.add_argument("--output", default="output/report.json", help="Output report path")
    parser.set_defaults(strict_runner_hash=True)
    parser.add_argument(
        "--strict-runner-hash",
        action="store_true",
        dest="strict_runner_hash",
        help="Enable runner-hash verification (default: enabled).",
    )
    parser.add_argument(
        "--no-strict-runner-hash",
        action="store_false",
        dest="strict_runner_hash",
        help="Disable runner-hash verification (debug only).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    root = Path(__file__).resolve().parent
    student_dir = Path(args.student_dir).resolve()

    papers_path = student_dir / "papers.jsonl"
    config_path = student_dir / "config.json"
    if not papers_path.exists() or not config_path.exists():
        raise FileNotFoundError("student-dir must contain papers.jsonl and config.json")

    records = _load_jsonl(papers_path)
    config = _load_json(config_path)
    student_machine_hash = _load_student_machine_hash(student_dir)

    run_job_path = Path(__file__).resolve()
    integrity = _integrity_info(run_job_path, papers_path, config_path)
    if args.strict_runner_hash and integrity["run_job_hash_match"] is False:
        raise RuntimeError("run_job.py hash mismatch under strict mode")

    selected_tasks = (
        ["word_count", "inverted_index", "prefix_filter", "similarity"]
        if args.task == "all"
        else [args.task]
    )

    results: Dict[str, Any] = {}
    correctness: Dict[str, Any] = {}
    for task in selected_tasks:
        results[task] = _run_one_task(task, records, config, prefix=args.prefix)
        correctness[task] = _correctness_check(task, records, config, prefix=args.prefix)

    code_evidence = {
        "mr_core.map_stage": _extract_function_snippet(root / "mr_core.py", "map_stage"),
        "mr_core.shuffle_stage": _extract_function_snippet(root / "mr_core.py", "shuffle_stage"),
        "mr_core.reduce_stage": _extract_function_snippet(root / "mr_core.py", "reduce_stage"),
        "jobs._wc_mapper": _extract_function_snippet(root / "jobs.py", "_wc_mapper"),
        "jobs._wc_reducer": _extract_function_snippet(root / "jobs.py", "_wc_reducer"),
        "jobs._ii_mapper": _extract_function_snippet(root / "jobs.py", "_ii_mapper"),
        "jobs._ii_reducer": _extract_function_snippet(root / "jobs.py", "_ii_reducer"),
        "jobs._pf_mapper": _extract_function_snippet(root / "jobs.py", "_pf_mapper"),
        "jobs._pf_reducer": _extract_function_snippet(root / "jobs.py", "_pf_reducer"),
        "jobs._pair_mapper": _extract_function_snippet(root / "jobs.py", "_pair_mapper"),
        "jobs._pair_reducer": _extract_function_snippet(root / "jobs.py", "_pair_reducer"),
    }

    report = {
        "meta": {
            "student_id": student_dir.name,
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "python_version": platform.python_version(),
            "platform": platform.platform(),
            "task": args.task,
            "prefix": args.prefix,
            "num_records": len(records),
            "config": config,
        },
        "integrity": integrity,
        "code_evidence": code_evidence,
        "results": results,
        "task_debug": {
            "method": "compare task outputs against built-in reference implementation",
            "per_task": correctness,
            "all_tasks_correct": all(v["match_reference"] for v in correctness.values()),
        },
        "_student_id_hash": student_machine_hash,
    }

    output_path = Path(args.output).resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Report generated: {output_path}")


if __name__ == "__main__":
    main()
