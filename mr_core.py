from __future__ import annotations

from collections import defaultdict
from multiprocessing import Pool
import warnings
from typing import Any, Callable, Dict, Iterable, List, Sequence, Tuple


KV = Tuple[str, Any]
Mapper = Callable[[Dict[str, Any]], List[KV]]
Reducer = Callable[[str, List[Any]], KV | None]
_WARNED_TODOS = set()


def _todo_warning(name: str, message: str) -> None:
    if name in _WARNED_TODOS:
        return
    _WARNED_TODOS.add(name)
    warnings.warn(message, RuntimeWarning, stacklevel=2)


def _chunk_records(records: Sequence[Dict[str, Any]], num_chunks: int) -> List[List[Dict[str, Any]]]:
    # Split input records into approximately equal chunks.
    # Why this helper exists:
    # - multiprocessing.Pool.map works best when each worker gets a batch of records.
    # - If we send one record per task, scheduling overhead is higher.
    #
    # Example:
    # - len(records)=10, num_chunks=3 => chunk_size=4
    # - produced chunks have sizes [4, 4, 2]
    #
    # Edge cases:
    # - If num_chunks <= 1, return one full chunk (sequential-like behavior).
    # - If input is very small (<=1), also return one chunk.
    if num_chunks <= 1 or len(records) <= 1:
        return [list(records)]
    chunk_size = max(1, (len(records) + num_chunks - 1) // num_chunks)
    return [list(records[i : i + chunk_size]) for i in range(0, len(records), chunk_size)]


def _map_chunk(args: Tuple[List[Dict[str, Any]], Mapper]) -> List[KV]:
    # Worker-side helper for the map stage.
    # Input:
    # - chunk: a list of records assigned to one worker
    # - mapper: user-defined function(record) -> List[(key, value)]
    #
    # Output:
    # - One flat list containing all (key, value) pairs emitted from this chunk.
    #
    # Note:
    # - A record may emit zero pairs, so we check `if mapped` before extending.
    chunk, mapper = args
    outputs: List[KV] = []
    for record in chunk:
        mapped = mapper(record)
        if mapped:
            outputs.extend(mapped)
    return outputs


def _reduce_item(args: Tuple[str, List[Any], Reducer]) -> KV | None:
    # Worker-side helper for reduce stage.
    # Input:
    # - key: grouped key produced by shuffle
    # - values: all values that share this key
    # - reducer: user-defined function(key, values) -> (key, reduced_value) or None
    #
    # Output:
    # - reducer output for one key
    key, values, reducer = args
    return reducer(key, values)


def _filter_none_results(items: List[KV | None]) -> List[KV]:
    # Reduce post-processing helper:
    # - Some reducers may return None to indicate "drop this key".
    # - This helper keeps only valid (key, value) pairs.
    return [item for item in items if item is not None]


def map_stage(records: List[Dict[str, Any]], mapper: Mapper, num_workers: int) -> List[KV]:
    """
    Map stage (intermediate data generation).

    Steps:
    - For each input record, call mapper(record)
    - Collect all emitted pairs into one flat list

    Return:
    - List[(key, value)] for the next shuffle stage
    """
    # Framework code is provided here so students can focus on core MapReduce logic.
    # You do NOT need to modify parallel details in this function.
    if num_workers <= 1:
        mapped: List[KV] = []
        for record in records:
            mapped.extend(mapper(record))
        return mapped

    chunks = _chunk_records(records, num_workers)
    with Pool(processes=num_workers) as pool:
        chunk_outputs = pool.map(_map_chunk, [(chunk, mapper) for chunk in chunks])
    flattened: List[KV] = []
    for out in chunk_outputs:
        flattened.extend(out)
    return flattened


def shuffle_stage(mapped: Iterable[KV]) -> Dict[str, List[Any]]:
    """
    Shuffle stage (group by key).

    Example:
    input  -> [("cat", 1), ("dog", 1), ("cat", 1)]
    output -> {"cat": [1, 1], "dog": [1]}
    """
    # ################ STUDENT TODO (Layer 1 core) BEGIN ################
    # Student goal:
    # - Implement "group by key" for intermediate map outputs.
    # Input:
    # - mapped: iterable of (key, value), e.g. [("cat", 1), ("dog", 1), ("cat", 1)]
    # Output:
    # - grouped dictionary, e.g. {"cat": [1, 1], "dog": [1]}
    # Hints:
    # - Use dict/list append pattern.
    # - Keep all values for each key; do not aggregate in shuffle stage.

    #_todo_warning("shuffle_stage", "shuffle_stage is not implemented yet; using empty grouped output.")

    shuffled_dict: Dict[str, List[Any]] = defaultdict(list)
    for key, value in mapped:
        shuffled_dict[key].append(value)
    return dict(shuffled_dict)  #用了defaultdict，转成普通dict返回，好像就结束了

    # return {}
    # ################ STUDENT TODO (Layer 1 core) END ################


def reduce_stage(grouped: Dict[str, List[Any]], reducer: Reducer, num_workers: int) -> List[KV]:
    """
    Reduce stage (aggregate grouped values).

    Steps:
    - For each key in grouped dict, call reducer(key, values)
    - Collect non-None reducer outputs

    Return:
    - Final List[(key, reduced_value)]
    """
    items = list(grouped.items())
    if num_workers <= 1:
        reduced: List[KV] = []
        for key, values in items:
            out = reducer(key, values)
            if out is not None:
                reduced.append(out)
        return reduced

    with Pool(processes=num_workers) as pool:
        reduced_or_none = pool.map(_reduce_item, [(key, values, reducer) for key, values in items])
    return _filter_none_results(reduced_or_none)


def run_map_reduce(
    records: List[Dict[str, Any]],
    mapper: Mapper,
    reducer: Reducer,
    num_workers: int = 2,
) -> List[KV]:
    """
    End-to-end MapReduce pipeline: map -> shuffle -> reduce.
    """
    mapped = map_stage(records, mapper, num_workers=num_workers)
    grouped = shuffle_stage(mapped)
    reduced = reduce_stage(grouped, reducer, num_workers=num_workers)
    # Stable ordering for deterministic grading
    reduced.sort(key=lambda kv: kv[0])
    return reduced
