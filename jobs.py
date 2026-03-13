from __future__ import annotations

import itertools
import multiprocessing as mp
import re
import warnings
from functools import partial
from typing import Any, Dict, Iterable, List, Set, Tuple

from mr_core import KV, run_map_reduce


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

TOKEN_RE = re.compile(r"[A-Za-z]+")
_WARNED_TODOS: Set[str] = set()


def _todo_warning(name: str) -> None:
    # Suppress duplicate warnings from multiprocessing workers.
    if mp.current_process().name != "MainProcess":
        return
    if name in _WARNED_TODOS:
        return
    _WARNED_TODOS.add(name)
    warnings.warn(f"{name} is not implemented yet; using fallback output.", RuntimeWarning, stacklevel=2)


def _normalize_tokens(text: str, min_token_len: int, remove_stopwords: bool) -> List[str]:
    # ################ STUDENT TODO (Layer 2) BEGIN ################
    # Implement a deterministic tokenizer.
    # Required behavior:
    # - Lowercase all words
    # - Keep alphabetic tokens only (already supported by TOKEN_RE)
    # - Drop tokens shorter than min_token_len
    # - If remove_stopwords=True, remove words in STOPWORDS
    # Return value:
    # - A list of normalized tokens (order can be kept as original appearance)

    #_todo_warning("_normalize_tokens")

    text = text.lower()
    alphabetic_tokens = TOKEN_RE.findall(text)  #原代码中设置好的名为TOKEN_RE的正则（找字母表token）
    filtered_tokens = []
    for token in alphabetic_tokens:
        if len(token) < min_token_len:
            continue  # continue跳过这次循环剩余部分，直接进入下一个token的匹配处理
        if remove_stopwords and token in STOPWORDS:
            continue
        filtered_tokens.append(token)
    return filtered_tokens


    #return []   #换成真实实现的token列表
    # ################ STUDENT TODO (Layer 2) END ################


# -------------------- Word Count --------------------
# ################ STUDENT TODO (Layer 2) BEGIN ################
def _wc_mapper(record: Dict[str, Any], min_token_len: int, remove_stopwords: bool) -> List[KV]:
    # Input: one paper record
    # Output: list of (token, 1)
    # Meaning: each occurrence contributes 1 count
    _todo_warning("_wc_mapper")
    return []


def _wc_reducer(key: str, values: List[Any]) -> KV:
    # Input: key=token, values=[1,1,1,...]
    # Output: (token, total_count)
    _todo_warning("_wc_reducer")
    return key, 0
# ################ STUDENT TODO (Layer 2) END ################


def word_count_job(records: List[Dict[str, Any]], config: Dict[str, Any]) -> List[KV]:
    mapper = partial(
        _wc_mapper,
        min_token_len=int(config["min_token_len"]),
        remove_stopwords=bool(config["remove_stopwords"]),
    )
    reduced = run_map_reduce(
        records,
        mapper=mapper,
        reducer=_wc_reducer,
        num_workers=int(config["num_workers"]),
    )
    reduced.sort(key=lambda kv: (-int(kv[1]), kv[0]))
    top_k = int(config["top_k"])
    return reduced[:top_k]


# -------------------- Inverted Index --------------------
# ################ STUDENT TODO (Layer 2) BEGIN ################
def _ii_mapper(record: Dict[str, Any], min_token_len: int, remove_stopwords: bool) -> List[KV]:
    # Input: one paper record
    # Output: list of (token, doc_id)
    # Important: emit each token at most once per document (use set)
    _todo_warning("_ii_mapper")
    return []


def _ii_reducer(key: str, values: List[Any]) -> KV:
    # Input: key=token, values=[doc_id, doc_id, ...]
    # Output: (token, sorted unique doc_id list)
    _todo_warning("_ii_reducer")
    return key, []
# ################ STUDENT TODO (Layer 2) END ################


def inverted_index_job(records: List[Dict[str, Any]], config: Dict[str, Any]) -> List[KV]:
    mapper = partial(
        _ii_mapper,
        min_token_len=int(config["min_token_len"]),
        remove_stopwords=bool(config["remove_stopwords"]),
    )
    reduced = run_map_reduce(
        records,
        mapper=mapper,
        reducer=_ii_reducer,
        num_workers=int(config["num_workers"]),
    )
    reduced.sort(key=lambda kv: kv[0])
    return reduced


# -------------------- Prefix Filter --------------------
# ################ STUDENT TODO (Layer 2) BEGIN ################
def _pf_mapper(record: Dict[str, Any], min_token_len: int, remove_stopwords: bool, prefix: str) -> List[KV]:
    # Input: one paper record + prefix (already lowercased by caller)
    # Output: (token, 1) only for tokens starting with prefix
    _todo_warning("_pf_mapper")
    return []


def _pf_reducer(key: str, values: List[Any]) -> KV:
    # Input: key=matched token, values=[1,1,...]
    # Output: (token, frequency among matched-prefix words)
    _todo_warning("_pf_reducer")
    return key, 0
# ################ STUDENT TODO (Layer 2) END ################


def prefix_filter_job(records: List[Dict[str, Any]], config: Dict[str, Any], prefix: str) -> List[KV]:
    mapper = partial(
        _pf_mapper,
        min_token_len=int(config["min_token_len"]),
        remove_stopwords=bool(config["remove_stopwords"]),
        prefix=prefix.lower(),
    )
    reduced = run_map_reduce(
        records,
        mapper=mapper,
        reducer=_pf_reducer,
        num_workers=int(config["num_workers"]),
    )
    reduced.sort(key=lambda kv: (-int(kv[1]), kv[0]))
    return reduced


# -------------------- Similarity (Jaccard) --------------------
def _doc_token_sets(records: Iterable[Dict[str, Any]], config: Dict[str, Any]) -> Dict[str, Set[str]]:
    out: Dict[str, Set[str]] = {}
    for r in records:
        doc_id = str(r["id"])
        tokens = set(
            _normalize_tokens(
                r.get("text", ""),
                min_token_len=int(config["min_token_len"]),
                remove_stopwords=bool(config["remove_stopwords"]),
            )
        )
        out[doc_id] = tokens
    return out


# ################ STUDENT TODO (Layer 2) BEGIN ################
def _pair_mapper(token_posting: Dict[str, Any]) -> List[KV]:
    # Input example:
    # {"token": "model", "docs": ["d1", "d2", "d3"]}
    # Output:
    # [("d1||d2", 1), ("d1||d3", 1), ("d2||d3", 1)]
    # Meaning:
    # - This token contributes 1 to intersection size of every doc pair sharing it.
    _todo_warning("_pair_mapper")
    return []


def _pair_reducer(key: str, values: List[Any]) -> KV:
    # Input: key=doc_i||doc_j, values=[1,1,...] from different shared tokens
    # Output: (doc_i||doc_j, intersection_count)
    _todo_warning("_pair_reducer")
    return key, 0
# ################ STUDENT TODO (Layer 2) END ################


def similarity_job(records: List[Dict[str, Any]], config: Dict[str, Any]) -> List[KV]:
    doc_tokens = _doc_token_sets(records, config)
    token_to_docs: Dict[str, List[str]] = {}
    for doc_id, tokens in doc_tokens.items():
        for token in tokens:
            token_to_docs.setdefault(token, []).append(doc_id)

    posting_records = [{"token": token, "docs": sorted(docs)} for token, docs in token_to_docs.items() if len(docs) >= 2]

    pair_intersections = run_map_reduce(
        posting_records,
        mapper=_pair_mapper,
        reducer=_pair_reducer,
        num_workers=int(config["num_workers"]),
    )

    threshold = float(config["threshold"])
    results: List[KV] = []
    for pair_key, inter_count in pair_intersections:
        left, right = pair_key.split("||", 1)
        left_size = len(doc_tokens[left])
        right_size = len(doc_tokens[right])
        union = left_size + right_size - int(inter_count)
        if union <= 0:
            continue
        jaccard = float(inter_count) / float(union)
        if jaccard >= threshold:
            results.append((pair_key, round(jaccard, 6)))

    results.sort(key=lambda kv: (-float(kv[1]), kv[0]))
    top_k = int(config["top_k"])
    return results[:top_k]
