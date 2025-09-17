"""Standalone duplicate analysis tool consolidated into a single file.

This module provides a lightweight in-memory simulation of the original
multi-module duplicate analysis workflow. The goal is to preserve the
high-level logical flow of preprocessing, deduplication, post-processing,
stack generation and statistics computation while avoiding external
runtime dependencies such as databases or third-party libraries.
"""
from __future__ import annotations

import json
import logging
from collections import defaultdict
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Optional

# ---------------------------------------------------------------------------
# Global configuration (replacement for the original ``global_cfg`` module)
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("duplicates")

OUTPUT_FOLDER = Path(__file__).resolve().parent / "output"
OUTPUT_FOLDER.mkdir(parents=True, exist_ok=True)

E2E_LOG = OUTPUT_FOLDER / "e2e_temp.log"
UUID = "standalone-duplicate-tool"

tools = {
    4: "Analyze Duplicates",
}

# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------


@dataclass
class Record:
    """Representation of a single business record used in the demo workflow."""

    rowid: int
    supplier: str
    name: str
    phone: str
    address: str
    city: str
    reject: bool = False
    reject_group: str = ""
    sample_type_code: str = ""
    stacking_group_id: int = 0
    stack_type: str = ""
    stack_count: int = 0
    address_score_percentage: float = 0.0

    def as_dict(self) -> Dict[str, object]:
        """Serialize the record to a JSON serialisable dictionary."""

        return {
            "rowid": self.rowid,
            "supplier": self.supplier,
            "name": self.name,
            "phone": self.phone,
            "address": self.address,
            "city": self.city,
            "reject": self.reject,
            "reject_group": self.reject_group,
            "sample_type_code": self.sample_type_code,
            "stacking_group_id": self.stacking_group_id,
            "stack_type": self.stack_type,
            "stack_count": self.stack_count,
            "address_score_percentage": self.address_score_percentage,
        }


# Sample dataset used for the in-memory simulation. The dataset contains
# deliberate duplicates to exercise the deduplication pipeline.
SAMPLE_RECORDS: List[Record] = [
    Record(1, "S1", "Alpha Pizza", "111-1111", "10 Main St", "Springfield"),
    Record(2, "S1", "Alpha Pizza", "111-1111", "10 Main St", "Springfield"),
    Record(3, "S2", "Beta Bakery", "222-2222", "20 Oak St", "Shelbyville"),
    Record(4, "S2", "Beta Bakery", "222-2222", "20 Oak St", "Shelbyville"),
    Record(5, "S3", "Gamma Grocer", "333-3333", "5 Pine Rd", "Springfield"),
    Record(6, "S4", "Delta Diner", "444-4444", "100 Elm St", "Ogdenville"),
    Record(7, "S4", "Delta Diner", "444-4444", "100 Elm St", "Ogdenville"),
    Record(8, "S5", "Echo Eatery", "555-5555", "200 Maple Ave", "Springfield"),
]


# ---------------------------------------------------------------------------
# Query abstraction and execution helpers
# ---------------------------------------------------------------------------


@dataclass
class Query:
    """Simple representation of an executable query action."""

    description: str
    callback: Optional[Callable[[], int]] = None

    def execute(self) -> int:
        if self.callback is None:
            return 0
        return int(self.callback())


class InMemoryQueryToolBox:
    """Small helper replicating a subset of the original ``QueryToolBox``.

    Instead of sending SQL to a database, callbacks operate directly on the
    in-memory record collection and return an integer result to mimic the
    number of affected rows.
    """

    def __init__(self, records: List[Record]):
        self.records = records
        self.history: List[List[object]] = []
        self._counter = 1

    def execute_query_list(self, queries: Iterable[Query]) -> List[List[object]]:
        log_entries: List[List[object]] = []
        for query in queries:
            if not isinstance(query, Query):
                query = Query(str(query))
            rows = query.execute()
            log_entries.append([self._counter, query.description, rows])
            self._counter += 1
        self.history.extend(log_entries)
        return log_entries

    def save_queries(self, queries: Iterable[Query], filename: str) -> str:
        lines: List[str] = []
        for query in queries:
            if isinstance(query, Query):
                lines.append(query.description)
            else:
                lines.append(str(query))
        path = OUTPUT_FOLDER / f"{filename}.sql"
        with open(path, "w", encoding="utf-8") as handle:
            handle.write("\n--\n".join(lines))
        logger.info("Saved %d queries to %s", len(lines), path)
        return str(path)

    def clean_console(self) -> None:
        logger.info("Executed %d queries in total", len(self.history))

    def save_output_table(self, tool_id: int) -> str:
        path = OUTPUT_FOLDER / f"{tools[tool_id].replace(' ', '_').lower()}_result.json"
        with open(path, "w", encoding="utf-8") as handle:
            json.dump([record.as_dict() for record in self.records], handle, indent=2)
        logger.info("Saved output data to %s", path)
        return f"Output table saved to {path}"


# ---------------------------------------------------------------------------
# SQL style logging utilities
# ---------------------------------------------------------------------------


class SQLLogger:
    """Minimal stand-in for the original SQL logger."""

    def __init__(self, logName: str = "ExecutionLog") -> None:
        self.time_started = datetime.now()
        self.logName = logName
        self.temp_table_info: Dict[int, Dict[str, object]] = {}

    def writeLog(self, log: List[List[object]], plog: List[str]) -> None:
        path = OUTPUT_FOLDER / f"{self.logName}.log"
        time_finished = datetime.now()
        with open(path, "w", encoding="utf-8") as handle:
            handle.write(f"Execution started: {self.time_started:%Y-%m-%d %H:%M:%S}\n")
            handle.write(f"Execution finished: {time_finished:%Y-%m-%d %H:%M:%S}\n")
            handle.write(f"Duration: {time_finished - self.time_started}\n\n")
            if self.temp_table_info:
                handle.write("Temporary tables created:\n")
                for key, info in sorted(self.temp_table_info.items()):
                    table = info.get("table", "<unknown>")
                    size = info.get("total", "<unknown>")
                    split_by = info.get("split_by")
                    items = info.get("items")
                    handle.write(f"  Part {key}: table={table}, size={size}")
                    if split_by:
                        handle.write(f", split_by={split_by}, items={items}")
                    handle.write("\n")
                handle.write("\n")
            if plog:
                handle.write("Post-processing log:\n")
                for line in plog:
                    handle.write(f"  - {line}\n")
                handle.write("\n")
            if log:
                handle.write("Executed actions:\n")
                for counter, description, rows in log:
                    handle.write(f"  [{counter}] {description} -> {rows}\n")
        logger.info("Wrote execution log to %s", path)


# ---------------------------------------------------------------------------
# Pre-processing: splitting the dataset into manageable parts
# ---------------------------------------------------------------------------


class Preprocessor:
    """Prepare the dataset for deduplication by creating temporary parts."""

    def __init__(self, records: List[Record]):
        self.records = records
        self.split_by_field = "city"
        self.split_if_greater = 6
        self.part_size = 4
        self.create_table_queries: List[List[Query]] = []
        self.temp_tables: Dict[str, List[int]] = {}
        self.parts: Dict[int, Dict[str, object]] = {}

    def _group_indices_by_city(self) -> Dict[str, List[int]]:
        grouped: Dict[str, List[int]] = defaultdict(list)
        for index, record in enumerate(self.records):
            grouped[record.city].append(index)
        return grouped

    def check_if_split_needed(self) -> (bool, int):
        total = len(self.records)
        unique_cities = {record.city for record in self.records}
        if total >= self.split_if_greater and len(unique_cities) > 1:
            return True, total
        return False, total

    def generate_split_groups(self) -> Dict[int, Dict[str, object]]:
        grouped_by_city = self._group_indices_by_city()
        parts: Dict[int, Dict[str, object]] = {}
        part_id = 1
        current_items: List[str] = []
        current_indices: List[int] = []
        for city, indices in grouped_by_city.items():
            if current_indices and len(current_indices) + len(indices) > self.part_size:
                parts[part_id] = {
                    "items": list(current_items),
                    "indices": list(current_indices),
                    "total": len(current_indices),
                }
                part_id += 1
                current_items = []
                current_indices = []
            current_items.append(city)
            current_indices.extend(indices)
        if current_indices:
            parts[part_id] = {
                "items": list(current_items),
                "indices": list(current_indices),
                "total": len(current_indices),
            }
        return parts

    def _register_temp_table(self, part_id: int, temp_name: str, indices: List[int]) -> None:
        self.temp_tables[temp_name] = indices
        self.parts[part_id]["table"] = temp_name
        self.parts[part_id]["split_by"] = self.split_by_field

    def create_temp_tables(self, groups: Dict[int, Dict[str, object]]) -> Dict[int, Dict[str, object]]:
        self.create_table_queries = []
        self.parts = {}
        for part_id, info in groups.items():
            temp_name = f"temp_part_{part_id}"
            indices = info["indices"]
            self.parts[part_id] = {
                "items": info["items"],
                "indices": indices,
                "total": info["total"],
            }
            self._register_temp_table(part_id, temp_name, indices)
            query = Query(
                description=f"Create temporary table {temp_name} with {len(indices)} records",
                callback=lambda count=len(indices): count,
            )
            self.create_table_queries.append([query])
        return self.parts

    def process(self) -> Dict[int, Dict[str, object]]:
        split_needed, total = self.check_if_split_needed()
        if split_needed:
            groups = self.generate_split_groups()
            parts = self.create_temp_tables(groups)
        else:
            temp_name = "temp_full_table"
            indices = list(range(len(self.records)))
            self.parts = {
                1: {
                    "items": ["ALL"],
                    "indices": indices,
                    "total": total,
                    "split_by": None,
                    "table": temp_name,
                }
            }
            self.temp_tables[temp_name] = indices
            query = Query(
                description=f"Create temporary table {temp_name} with {len(indices)} records",
                callback=lambda count=len(indices): count,
            )
            self.create_table_queries = [[query]]
            parts = self.parts
        return parts


# ---------------------------------------------------------------------------
# Deduplication logic
# ---------------------------------------------------------------------------


class DedupQueryGenerator:
    """Identify duplicates within each temporary table."""

    def __init__(self, records: List[Record], preprocessor: Preprocessor):
        self.records = records
        self.preprocessor = preprocessor
        self.processed_tables: List[str] = []

    def _mark_duplicates(self, indices: Iterable[int]) -> int:
        groups: Dict[tuple, List[Record]] = defaultdict(list)
        for index in indices:
            record = self.records[index]
            key = (record.name.lower(), record.phone)
            groups[key].append(record)
        duplicates = 0
        for members in groups.values():
            if len(members) > 1:
                members[0].sample_type_code = "MASTER"
                members[0].reject = False
                members[0].reject_group = ""
                for record in members[1:]:
                    record.reject = True
                    record.reject_group = "duplicate"
                    record.sample_type_code = ""
                    record.address_score_percentage = 100.0
                    duplicates += 1
            else:
                members[0].reject = False
                members[0].reject_group = ""
                members[0].sample_type_code = "UNIQUE"
                members[0].address_score_percentage = 100.0
        return duplicates

    def generate_queries(self, temp_table_name: str, part: int, analyze: bool = False) -> (List[Query], List[Query]):
        indices = self.preprocessor.temp_tables[temp_table_name]
        info_queries: List[Query] = []
        if analyze:
            info_queries.append(
                Query(description=f"Analyze statistics for {temp_table_name}", callback=lambda: len(indices))
            )
        queries = [
            Query(
                description=f"Deduplicate records in {temp_table_name}",
                callback=lambda idx=tuple(indices): self._mark_duplicates(idx),
            )
        ]
        self.processed_tables.append(temp_table_name)
        return queries, info_queries

    def delete_temps(self, temp_table_name: str) -> None:
        # In the in-memory variant there is nothing to delete, but we keep the
        # method for parity with the original API.
        if temp_table_name in self.preprocessor.temp_tables:
            return


# ---------------------------------------------------------------------------
# Post-processing and stacking helpers
# ---------------------------------------------------------------------------


class Postprocessor:
    """Post-deduplication operations."""

    def __init__(self, records: List[Record], preprocessor: Preprocessor):
        self.records = records
        self.preprocessor = preprocessor

    def run_additional_scripts(self, temp_table_name: str) -> List[Query]:
        indices = self.preprocessor.temp_tables[temp_table_name]

        def _score_addresses() -> int:
            updated = 0
            for index in indices:
                record = self.records[index]
                if record.reject or record.sample_type_code:
                    record.address_score_percentage = max(record.address_score_percentage, 90.0)
                    updated += 1
            return updated

        return [
            Query(
                description=f"Compute address score for {temp_table_name}",
                callback=_score_addresses,
            )
        ]

    def merge_temp_tables(self, tables: Iterable[str], cleanup: bool = True) -> List[Query]:
        description = "Merge results from " + ", ".join(tables)
        return [Query(description=description, callback=lambda: len(self.records))]


class Stacking:
    """Assign stacking identifiers to records."""

    def __init__(self, records: List[Record], preprocessor: Preprocessor):
        self.records = records
        self.preprocessor = preprocessor
        self.next_stack_id = 1

    def run_stacks(self, temp_table_name: str) -> None:
        indices = self.preprocessor.temp_tables[temp_table_name]
        groups: Dict[tuple, List[Record]] = defaultdict(list)
        for index in indices:
            record = self.records[index]
            key = (record.name.lower(), record.phone)
            groups[key].append(record)
        for members in groups.values():
            stack_id = self.next_stack_id
            self.next_stack_id += 1
            for record in members:
                record.stacking_group_id = stack_id
                record.stack_count = len(members)
                record.stack_type = "Valid Stack" if len(members) > 1 else "Inconclusive"


# ---------------------------------------------------------------------------
# Statistics collector
# ---------------------------------------------------------------------------


class DuplicateStatistics:
    """Calculate aggregated statistics for the processed dataset."""

    def __init__(self, records: List[Record]):
        self.records = records

    def run(self) -> List[str]:
        total = len(self.records)
        rejected = sum(1 for record in self.records if record.reject)
        sampled = sum(1 for record in self.records if record.sample_type_code)
        stack_valid = sum(1 for record in self.records if record.stack_type == "Valid Stack")
        stack_inconclusive = sum(1 for record in self.records if record.stack_type == "Inconclusive")

        scenario_breakdown: Dict[str, int] = defaultdict(int)
        for record in self.records:
            if record.reject:
                scenario_breakdown[f"Rejected ({record.reject_group or 'duplicate'})"] += 1
            elif record.sample_type_code:
                scenario_breakdown[f"Sampled ({record.sample_type_code})"] += 1
            else:
                scenario_breakdown["Unclassified"] += 1

        lines = [
            f"Total records: {total}",
            f"Rejected records: {rejected}",
            f"Sampled records: {sampled}",
            f"Valid stacks: {stack_valid}",
            f"Inconclusive stacks: {stack_inconclusive}",
        ]
        for scenario, count in sorted(scenario_breakdown.items()):
            lines.append(f"{scenario}: {count}")
        return lines


# ---------------------------------------------------------------------------
# Main orchestration function
# ---------------------------------------------------------------------------


tool_id = 4

def run(records: Optional[List[Record]] = None) -> List[Record]:
    """Execute the full standalone workflow.

    Parameters
    ----------
    records:
        Optional list of records to process. When omitted, a copy of the
        built-in :data:`SAMPLE_RECORDS` is used.

    Returns
    -------
    List[Record]
        The processed record collection, allowing callers to inspect the
        deduplication results programmatically.
    """

    working_records = deepcopy(records or SAMPLE_RECORDS)
    toolbox = InMemoryQueryToolBox(working_records)
    sql_logger = SQLLogger(logName=tools[tool_id].replace(" ", "_"))

    pre = Preprocessor(working_records)
    parts = pre.process()
    sql_logger.temp_table_info = {
        key: {
            "table": value.get("table"),
            "total": value.get("total"),
            "split_by": value.get("split_by"),
            "items": value.get("items"),
        }
        for key, value in parts.items()
    }

    dup = DedupQueryGenerator(working_records, pre)
    post = Postprocessor(working_records, pre)
    stack = Stacking(working_records, pre)
    stats = DuplicateStatistics(working_records)

    log: List[List[object]] = []
    plog: List[str] = []

    for part_id in sorted(parts):
        part = parts[part_id]
        temp_table_name = part["table"]
        analyze = part["total"] > (2 * pre.part_size)

        log.extend(toolbox.execute_query_list(pre.create_table_queries[part_id - 1]))

        queries, info_queries = dup.generate_queries(temp_table_name, part=part_id, analyze=analyze)
        if info_queries:
            log.extend(toolbox.execute_query_list(info_queries))

        toolbox.save_queries(queries, filename=f"{part_id}_{tools[tool_id].replace(' ', '_')}_Queries")
        log.extend(toolbox.execute_query_list(queries))

        additional_scripts = post.run_additional_scripts(temp_table_name)
        log.extend(toolbox.execute_query_list(additional_scripts))

        stack.run_stacks(temp_table_name)
        dup.delete_temps(temp_table_name)

    if len(parts) > 1:
        tables = [parts[key]["table"] for key in sorted(parts)]
        wrap_up = post.merge_temp_tables(tables, cleanup=True)
        log.extend(toolbox.execute_query_list(wrap_up))

    plog.extend(stats.run())
    toolbox.clean_console()
    plog.append(toolbox.save_output_table(tool_id=tool_id))

    sql_logger.writeLog(log, plog)
    return working_records


if __name__ == "__main__":
    run()
