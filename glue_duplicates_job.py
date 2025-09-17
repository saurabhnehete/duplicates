"""Standalone AWS Glue-compatible job for duplicate analysis.

This script consolidates the legacy multi-module implementation into a single
file that can be executed as an AWS Glue job.  It reads an input parquet dataset
from S3, applies duplicate-detection scenarios configured via job parameters,
computes stacking metrics, writes the enriched dataset back to parquet, and
produces a CSV statistics report.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass
from difflib import SequenceMatcher
from typing import Dict, Iterable, List, Optional, Tuple

try:  # Glue runtime
    from awsglue.context import GlueContext  # type: ignore
    from awsglue.utils import getResolvedOptions  # type: ignore
    from pyspark.context import SparkContext  # type: ignore

    GLUE_RUNTIME = True
except ImportError:  # Local / EMR runtime
    from pyspark.sql import SparkSession  # type: ignore

    GlueContext = None  # type: ignore
    SparkContext = None  # type: ignore
    getResolvedOptions = None  # type: ignore
    GLUE_RUNTIME = False

from pyspark.sql import DataFrame, Window  # type: ignore
from pyspark.sql import functions as F  # type: ignore


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------

ROW_ID_COLUMN = "__rowid__"
SPECIAL_FLAGS = {
    "_reject_",
    "_advanced_",
    "_dif_location_",
    "_sim_name_",
    "_location_",
}
def _is_specified(value: Optional[str]) -> bool:
    return bool(value) and value != "__not_specified_by_the_user__"


# ---------------------------------------------------------------------------
# Scenario parsing and configuration
# ---------------------------------------------------------------------------


@dataclass
class ScenarioDefinition:
    name: str
    reject: bool
    advanced: bool
    use_different_location: bool
    use_location: bool
    use_similar_names: bool
    base_fields: List[str]

    def contexts(
        self,
        address_fields: List[str],
        coordinate_fields: List[str],
    ) -> List[Tuple[str, List[str]]]:
        """Return list of (context, fields) pairs for processing."""
        if self.use_different_location:
            if not address_fields and not coordinate_fields:
                raise ValueError(
                    "_dif_location_ scenario requires address and/or coordinate fields"
                )
            dedup_fields = list(self.base_fields)
            return [("dif_location", dedup_fields)]

        if self.use_location:
            contexts: List[Tuple[str, List[str]]] = []
            if address_fields:
                contexts.append(("address", self.base_fields + address_fields))
            if coordinate_fields:
                contexts.append(("coordinates", self.base_fields + coordinate_fields))
            return contexts

        return [("none", list(self.base_fields))]


class ScenarioConfig:
    """Parses duplicate scenario configuration from job parameters."""

    def __init__(self, config: Dict[str, object], user_fields: Dict[str, str]):
        self.raw_config = config
        self.user_fields = user_fields

        lists = config.get("lists", {})
        self.address_fields = self._resolve_field_list(lists.get("_address_", []))
        self.coordinate_fields = self._resolve_field_list(
            lists.get("_coordinates_", [])
        )
        self.part_size = int(config.get("part_size", 1))
        self.split_if_greater = int(config.get("split_if_greater", 0))
        self.split_by = config.get("split_by")

        self.scenarios: Dict[str, ScenarioDefinition] = {}
        scenario_entries = config.get("scenarios", {})
        for name, raw_fields in scenario_entries.items():
            tokens = self._to_list(raw_fields)
            self.scenarios[name] = self._parse_scenario(name, tokens)

    def _to_list(self, values: object) -> List[str]:
        if isinstance(values, str):
            return [v.strip() for v in values.strip("[]").split(",") if v.strip()]
        if isinstance(values, Iterable):
            return [str(v).strip() for v in values]
        raise TypeError(f"Unsupported scenario configuration value: {values}")

    def _resolve_field_list(self, values: object) -> List[str]:
        fields = []
        for token in self._to_list(values):
            column = self.user_fields.get(token)
            if _is_specified(column):
                fields.append(column)
        return fields

    def _parse_scenario(self, name: str, tokens: List[str]) -> ScenarioDefinition:
        flags = set(tokens) & SPECIAL_FLAGS
        field_tokens = [tok for tok in tokens if tok not in SPECIAL_FLAGS]
        base_fields = [
            self.user_fields[tok]
            for tok in field_tokens
            if _is_specified(self.user_fields.get(tok))
        ]
        return ScenarioDefinition(
            name=name,
            reject="_reject_" in flags,
            advanced="_advanced_" in flags,
            use_different_location="_dif_location_" in flags,
            use_location="_location_" in flags,
            use_similar_names="_sim_name_" in flags,
            base_fields=base_fields,
        )

    def get_scenarios(self) -> List[ScenarioDefinition]:
        return list(self.scenarios.values())


# ---------------------------------------------------------------------------
# Similar name clustering utilities
# ---------------------------------------------------------------------------


class FuzzyCluster:
    """Utility for grouping similar strings using fuzzy matching."""

    def __init__(self, threshold: float = 0.75, stopwords_pattern: Optional[str] = None):
        self.threshold = threshold
        self.stopwords_pattern = stopwords_pattern

    def _apply_rules(self, value: str) -> str:
        value = re.sub(r"[,\.\"'\|\(\)-]+", " ", value)
        if self.stopwords_pattern:
            pattern = rf"\b({self.stopwords_pattern})\b"
            value = re.sub(pattern, " ", value, flags=re.IGNORECASE)
        value = re.sub(r"\s+", " ", value)
        return value.strip().lower()

    @staticmethod
    def _pairwise(items: List[str]) -> Iterable[Tuple[str, str]]:
        for i in range(len(items)):
            for j in range(i + 1, len(items)):
                yield items[i], items[j]

    @staticmethod
    def _similarity(a: str, b: str) -> float:
        return SequenceMatcher(None, a, b).ratio()

    def create_clusters(self, items: List[str]) -> List[List[str]]:
        if not items:
            return []
        cleaned = [(item, self._apply_rules(item)) for item in items]
        normalized = [item for _, item in cleaned]

        adjacency: Dict[str, List[str]] = {item: [] for item in normalized}
        for left, right in self._pairwise(normalized):
            if self._similarity(left, right) >= self.threshold:
                adjacency[left].append(right)
                adjacency[right].append(left)

        visited: set = set()
        clusters: List[List[str]] = []
        for original, normalized_value in zip(items, normalized):
            if normalized_value in visited:
                continue
            stack = [normalized_value]
            component: List[str] = []
            while stack:
                current = stack.pop()
                if current in visited:
                    continue
                visited.add(current)
                component.append(current)
                stack.extend(adjacency[current])

            # restore original values
            cluster_values = [
                original_item
                for original_item, normalized_item in cleaned
                if normalized_item in component
            ]
            clusters.append(cluster_values)
        return clusters


# ---------------------------------------------------------------------------
# Deduplication logic
# ---------------------------------------------------------------------------


class DeduplicationProcessor:
    def __init__(
        self,
        spark: "SparkSession",
        user_fields: Dict[str, str],
        tool_fields: Dict[str, str],
        scenario_config: ScenarioConfig,
        stopwords_pattern: Optional[str] = None,
    ) -> None:
        self.spark = spark
        self.user_fields = user_fields
        self.tool_fields = tool_fields
        self.config = scenario_config
        self.fuzzy = FuzzyCluster(stopwords_pattern=stopwords_pattern)

        self.reject_col = tool_fields["reject"]
        self.reject_group_col = tool_fields["reject_group"]
        self.sample_type_col = tool_fields["sample_type_code"]
        self.group_id_col = tool_fields["dedup_group_id"]
        self.stacking_group_id_col = tool_fields["stacking_group_id"]
        self.stack_count_col = tool_fields["Stack_Count"]
        self.stack_type_col = tool_fields["Stack_Type"]
        self.address_score_col = tool_fields["address_score_percentage"]

        self.org_reject_group_col = user_fields.get("org_reject_group")
        self.org_sample_type_col = user_fields.get("org_sample_type_code")

        self.poi_name_col = user_fields.get("src_poi_nm")
        self.full_addr_col = user_fields.get("src_full_addr")
        self.lat_col = user_fields.get("src_display_lat")
        self.long_col = user_fields.get("src_display_long")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(self, df: DataFrame) -> Tuple[DataFrame, DataFrame]:
        df = self._ensure_columns(df)

        for scenario in self.config.get_scenarios():
            df = self._apply_scenario(df, scenario)

        df = self._populate_org_columns(df)
        df = self._assign_stacking_groups(df)
        df = self._compute_stacking_metrics(df)

        stats_df = self._build_statistics(df)
        return df, stats_df

    # ------------------------------------------------------------------
    # Column preparation
    # ------------------------------------------------------------------

    def _ensure_columns(self, df: DataFrame) -> DataFrame:
        string_defaults = {
            self.reject_col: "",
            self.reject_group_col: "",
            self.sample_type_col: "",
            self.group_id_col: "",
            self.stacking_group_id_col: "",
            self.stack_type_col: "",
        }
        for column, default in string_defaults.items():
            if column not in df.columns:
                df = df.withColumn(column, F.lit(default))
        if self.stack_count_col not in df.columns:
            df = df.withColumn(self.stack_count_col, F.lit(None).cast("long"))
        if self.address_score_col not in df.columns:
            df = df.withColumn(self.address_score_col, F.lit(None).cast("double"))
        return df

    # ------------------------------------------------------------------
    # Scenario application
    # ------------------------------------------------------------------

    def _apply_scenario(self, df: DataFrame, scenario: ScenarioDefinition) -> DataFrame:
        contexts = scenario.contexts(
            self.config.address_fields, self.config.coordinate_fields
        )
        for context_name, fields in contexts:
            df = self._process_context(df, scenario, context_name, fields)

        if scenario.use_similar_names and self.poi_name_col:
            df = self._apply_similar_names(df, scenario)

        return df

    def _process_context(
        self,
        df: DataFrame,
        scenario: ScenarioDefinition,
        context_name: str,
        fields: List[str],
    ) -> DataFrame:
        if not fields:
            return df

        filtered = df
        for column in fields:
            filtered = filtered.filter(
                F.col(column).isNotNull() & (F.trim(F.col(column)) != "")
            )

        if scenario.reject:
            filtered = filtered.filter(
                (F.col(self.reject_col).isNull()) | (F.col(self.reject_col) == "")
            )
        else:
            filtered = filtered.filter(
                ((F.col(self.reject_col).isNull()) | (F.col(self.reject_col) == ""))
                & (
                    (F.col(self.sample_type_col).isNull())
                    | (F.col(self.sample_type_col) == "")
                )
            )

        if filtered.rdd.isEmpty():  # pragma: no cover - guard for empty partitions
            return df

        grouping = filtered.groupBy(*fields).agg(
            F.collect_list(ROW_ID_COLUMN).alias("rowids"),
            F.count("*").alias("rowcount"),
        )
        duplicates = grouping.filter(F.col("rowcount") > 1)
        if duplicates.rdd.isEmpty():  # no duplicates for this context
            return df

        order_columns = [F.col(column) for column in fields]
        window = Window.orderBy(*order_columns)
        prefix = "x" if context_name == "coordinates" else ""

        assignments = (
            duplicates.withColumn("rowids", F.array_distinct(F.col("rowids")))
            .withColumn("sorted_rowids", F.array_sort(F.col("rowids")))
            .withColumn("primary_rowid", F.element_at("sorted_rowids", 1))
            .withColumn(
                "group_index",
                F.row_number().over(window),
            )
            .withColumn(
                "group_value",
                F.concat(F.lit(prefix), F.col("group_index").cast("string")),
            )
            .select(
                F.col("group_value"),
                F.col("primary_rowid"),
                F.explode("sorted_rowids").alias(ROW_ID_COLUMN),
            )
            .withColumn("is_primary", F.col(ROW_ID_COLUMN) == F.col("primary_rowid"))
            .drop("primary_rowid")
        )

        join_cols = [ROW_ID_COLUMN]
        df = df.join(assignments, on=join_cols, how="left")

        # update reject/sample flags
        if scenario.reject:
            df = df.withColumn(
                self.reject_col,
                F.when(
                    F.col("group_value").isNotNull(),
                    F.when(F.col("is_primary"), F.lit(""))
                    .otherwise(F.lit("true")),
                ).otherwise(F.col(self.reject_col)),
            )
            df = df.withColumn(
                self.reject_group_col,
                F.when(
                    F.col("group_value").isNotNull(),
                    F.lit(scenario.name),
                ).otherwise(F.col(self.reject_group_col)),
            )
        else:
            df = df.withColumn(
                self.sample_type_col,
                F.when(
                    F.col("group_value").isNotNull(),
                    F.lit(scenario.name),
                ).otherwise(F.col(self.sample_type_col)),
            )

        df = df.withColumn(
            self.group_id_col,
            F.when(
                F.col("group_value").isNotNull(),
                F.col("group_value"),
            ).otherwise(F.col(self.group_id_col)),
        )

        df = df.drop("group_value", "is_primary")
        return df

    # ------------------------------------------------------------------
    # Similar names processing (optional)
    # ------------------------------------------------------------------

    def _apply_similar_names(self, df: DataFrame, scenario: ScenarioDefinition) -> DataFrame:
        if not self.poi_name_col or self.group_id_col not in df.columns:
            return df

        subset = df.filter(F.col(self.sample_type_col) == scenario.name)
        if subset.rdd.isEmpty():
            return df

        pandas_df = subset.select(self.group_id_col, self.poi_name_col, ROW_ID_COLUMN).toPandas()
        if pandas_df.empty:
            return df

        new_assignments = []
        for group_id, group_frame in pandas_df.groupby(self.group_id_col):
            names = group_frame[self.poi_name_col].astype(str).tolist()
            clusters = self.fuzzy.create_clusters(names)
            for cluster_index, cluster in enumerate(clusters, start=1):
                new_group_id = f"{group_id}_{cluster_index}"
                rowids = (
                    group_frame[group_frame[self.poi_name_col].isin(cluster)][
                        ROW_ID_COLUMN
                    ]
                    .astype(str)
                    .tolist()
                )
                for rowid in rowids:
                    new_assignments.append((rowid, new_group_id))

        if not new_assignments:
            return df

        assignment_df = self.spark.createDataFrame(new_assignments, [ROW_ID_COLUMN, "new_gid"])
        df = df.join(assignment_df, on=ROW_ID_COLUMN, how="left")
        df = df.withColumn(
            self.group_id_col,
            F.when(F.col("new_gid").isNotNull(), F.col("new_gid")).otherwise(F.col(self.group_id_col)),
        )
        df = df.drop("new_gid")
        return df

    # ------------------------------------------------------------------
    # Stacking metrics
    # ------------------------------------------------------------------

    def _assign_stacking_groups(self, df: DataFrame) -> DataFrame:
        if not (_is_specified(self.lat_col) and _is_specified(self.long_col)):
            return df

        lat_col = self.lat_col
        long_col = self.long_col

        window = Window.orderBy(F.col(lat_col), F.col(long_col))
        df = df.withColumn(
            self.stacking_group_id_col,
            F.when(
                (F.col(lat_col).isNotNull())
                & (F.col(long_col).isNotNull())
                & ((F.col(self.reject_col).isNull()) | (F.col(self.reject_col) == "")),
                F.concat(F.lit("x"), F.dense_rank().over(window).cast("string")),
            ).otherwise(F.col(self.stacking_group_id_col)),
        )

        group_window = Window.partitionBy(self.stacking_group_id_col)
        df = df.withColumn(
            self.stack_count_col,
            F.when(
                F.col(self.stacking_group_id_col).isNotNull(),
                F.count("*").over(group_window),
            ).otherwise(F.col(self.stack_count_col)),
        )
        return df

    def _compute_stacking_metrics(self, df: DataFrame) -> DataFrame:
        if not (_is_specified(self.full_addr_col) and _is_specified(self.lat_col) and _is_specified(self.long_col)):
            return df

        relevant = df.filter(
            (F.col(self.stacking_group_id_col).isNotNull())
            & (F.col(self.stack_count_col) >= 10)
            & (F.col(self.stack_count_col) <= 100)
        )
        if relevant.rdd.isEmpty():
            return df

        # Address score percentage
        address_counts = (
            relevant.groupBy(self.stacking_group_id_col, self.full_addr_col)
            .agg(F.count("*").alias("addr_count"))
            .withColumn(
                "normalized_addr",
                F.lower(F.regexp_replace(F.col(self.full_addr_col), r"\d+", "")),
            )
            .withColumn("normalized_addr", F.trim(F.col("normalized_addr")))
        )

        addr_window = Window.partitionBy(self.stacking_group_id_col)
        max_counts = address_counts.withColumn(
            "max_addr_count", F.max("addr_count").over(addr_window)
        )
        total_counts = relevant.groupBy(self.stacking_group_id_col).agg(
            F.first(self.stack_count_col).alias("stack_size")
        )

        address_scores = (
            max_counts.select(
                self.stacking_group_id_col,
                F.col("max_addr_count"),
            )
            .dropDuplicates([self.stacking_group_id_col])
            .join(total_counts, on=self.stacking_group_id_col, how="inner")
            .withColumn(
                self.address_score_col,
                F.when(
                    F.col("stack_size") > 0,
                    F.col("max_addr_count") / F.col("stack_size") * F.lit(100.0),
                ).otherwise(F.lit(0.0)),
            )
        )

        null_counts = (
            relevant.groupBy(self.stacking_group_id_col)
            .agg(
                F.sum(
                    F.when(F.col(self.full_addr_col).isNull(), F.lit(1)).otherwise(F.lit(0))
                ).alias("null_count"),
                F.sum(
                    F.when(F.col(self.full_addr_col).isNotNull(), F.lit(1)).otherwise(F.lit(0))
                ).alias("not_null_count"),
            )
            .withColumn(
                "drop_rate",
                F.when(
                    (F.col("null_count") + F.col("not_null_count")) > 0,
                    F.col("null_count")
                    / (F.col("null_count") + F.col("not_null_count"))
                    * F.lit(100.0),
                ).otherwise(F.lit(0.0)),
            )
        )

        metrics = (
            address_scores.join(null_counts, on=self.stacking_group_id_col, how="left")
            .withColumn(
                self.address_score_col,
                F.when(F.col("drop_rate") > 80, F.lit(0.0)).otherwise(F.col(self.address_score_col)),
            )
            .withColumn(
                self.stack_type_col,
                F.when(
                    F.col(self.address_score_col) == 0,
                    F.lit("Inconclusive"),
                ).when(
                    (F.col(self.address_score_col) > 48) & (F.col("drop_rate") < 40),
                    F.lit("Valid Stack"),
                ).otherwise(F.lit("Invalid Stack")),
            )
            .select(
                F.col(self.stacking_group_id_col),
                F.col(self.address_score_col).alias("new_address_score"),
                F.col(self.stack_type_col).alias("new_stack_type"),
            )
        )

        df = df.join(metrics, on=self.stacking_group_id_col, how="left")
        df = df.withColumn(
            self.address_score_col,
            F.coalesce(F.col("new_address_score"), F.col(self.address_score_col)),
        )
        df = df.withColumn(
            self.stack_type_col,
            F.coalesce(F.col("new_stack_type"), F.col(self.stack_type_col)),
        )
        df = df.drop("new_address_score", "new_stack_type")
        return df

    # ------------------------------------------------------------------
    # Final column adjustments
    # ------------------------------------------------------------------

    def _populate_org_columns(self, df: DataFrame) -> DataFrame:
        if _is_specified(self.org_reject_group_col):
            df = df.withColumn(
                self.org_reject_group_col,
                F.when(
                    (F.col(self.reject_col) == "true")
                    & (F.col(self.reject_group_col).isNotNull())
                    & (F.col(self.reject_group_col) != ""),
                    F.col(self.reject_group_col),
                ).otherwise(F.col(self.org_reject_group_col)),
            )
        if _is_specified(self.org_sample_type_col):
            df = df.withColumn(
                self.org_sample_type_col,
                F.when(
                    F.col(self.sample_type_col).isNotNull()
                    & (F.col(self.sample_type_col) != ""),
                    F.col(self.sample_type_col),
                ).otherwise(F.col(self.org_sample_type_col)),
            )
        return df

    # ------------------------------------------------------------------
    # Statistics
    # ------------------------------------------------------------------

    def _build_statistics(self, df: DataFrame) -> DataFrame:
        total_records = df.count()
        metrics: List[Tuple[str, int, float]] = []

        def percent(count: int) -> float:
            return (count / total_records * 100) if total_records else 0.0

        metrics.append(("Total number of records", total_records, 100.0))

        rejected = df.filter(F.col(self.reject_col) == "true").count()
        metrics.append(("Total rejected", rejected, percent(rejected)))

        sampled = df.filter(F.col(self.sample_type_col) != "").count()
        metrics.append(("Total sampled", sampled, percent(sampled)))

        for scenario in self.config.get_scenarios():
            if scenario.reject:
                count = df.filter(
                    (F.col(self.reject_group_col) == scenario.name)
                    & (F.col(self.reject_col) == "true")
                ).count()
                label = f"Rejected as '{scenario.name}'"
            else:
                count = df.filter(F.col(self.sample_type_col) == scenario.name).count()
                label = f"Sampled as '{scenario.name}'"
            metrics.append((label, count, percent(count)))

        stack_counts = {
            "Invalid Stack": "Invalid Stack Records",
            "Valid Stack": "Valid Stack Records",
            "Inconclusive": "Inconclusive Stack Records",
        }
        for stack_value, label in stack_counts.items():
            count = df.filter(F.col(self.stack_type_col) == stack_value).count()
            metrics.append((label, count, percent(count)))

        return self.spark.createDataFrame(metrics, ["metric", "value", "percentage"])


# ---------------------------------------------------------------------------
# Argument parsing and job execution
# ---------------------------------------------------------------------------


def _load_stopwords(spark: "SparkSession", path: Optional[str]) -> Optional[str]:
    if not path:
        return None
    try:
        data = spark.sparkContext.wholeTextFiles(path).collect()
    except Exception:  # pragma: no cover - runtime dependency
        return None
    if not data:
        return None
    return data[0][1].strip().replace("\n", "|")


def _parse_arguments(argv: List[str]) -> Dict[str, str]:
    required = [
        "input_s3",
        "output_s3",
        "stats_s3",
        "duplicate_scenarios_json",
        "tool_fieldname_json",
        "user_fieldname_json",
        "sim_name_stopwords_path",
    ]
    if GLUE_RUNTIME and getResolvedOptions:
        return getResolvedOptions(argv, required)  # type: ignore[arg-type]

    parser = argparse.ArgumentParser(description="Duplicate analysis Glue job")
    for name in required:
        parser.add_argument(f"--{name}", required=True)
    args = parser.parse_args(argv[1:])
    return vars(args)


def main(argv: Optional[List[str]] = None) -> None:
    argv = argv or sys.argv
    options = _parse_arguments(argv)

    duplicate_scenarios = json.loads(options["duplicate_scenarios_json"])
    tool_fieldnames = json.loads(options["tool_fieldname_json"])
    user_fieldnames = json.loads(options["user_fieldname_json"])

    if GLUE_RUNTIME:
        assert SparkContext is not None
        glue_context = GlueContext(SparkContext.getOrCreate())  # type: ignore
        spark = glue_context.spark_session
    else:
        spark = SparkSession.builder.appName("duplicate-analysis").getOrCreate()

    try:
        stopwords_pattern = _load_stopwords(spark, options["sim_name_stopwords_path"])
        scenario_config = ScenarioConfig(duplicate_scenarios, user_fieldnames)
        processor = DeduplicationProcessor(
            spark,
            user_fieldnames,
            tool_fieldnames,
            scenario_config,
            stopwords_pattern=stopwords_pattern,
        )

        input_path = options["input_s3"]
        output_path = options["output_s3"]
        stats_path = options["stats_s3"]

        df = spark.read.parquet(input_path)
        processed_df, stats_df = processor.run(df)

        processed_df.write.mode("overwrite").parquet(output_path)
        stats_df.coalesce(1).write.mode("overwrite").option("header", True).csv(
            stats_path
        )
    finally:
        if not GLUE_RUNTIME:
            spark.stop()


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    main()
