```markdown
# PySpark Data Engineering Mastery 🚀

[![PySpark CI](https://github.com/vi-srk/pyspark-data-engineering-mastery/actions/workflows/ci.yml/badge.svg)](https://github.com/vi-srk/pyspark-data-engineering-mastery/actions)
[![Python 3.10+](https://img.shields.io/badge/python-3.10%2+-blue.svg)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Welcome to the **PySpark Data Engineering Mastery** repository. This project is a production-grade, 8-week progressive curriculum designed to take engineers from single-machine thinking to distributed, cluster-aware optimization, advanced windowing, data skew mitigation, and modular unit testing.

Unlike toy notebook scripts, this repository implements testable transformation modules, Delta Lake ACID transactions, physical execution plan tuning, and automated CI/CD workflows week by week.

---

## 🏗️ Repository Architecture

```text
pyspark-data-engineering-mastery/
├── .github/
│   └── workflows/
│       └── ci.yml                     # GitHub Actions running pytest & linting
├── src/
│   ├── __init__.py
│   ├── week1_architecture/            # SparkSession & execution DAGs
│   ├── week2_io_parquet/              # Strict schemas & optimized columnar formats
│   ├── week3_windows/                 # Advanced windowing & sessionization
│   ├── week4_nested_json/             # Exploding arrays & parsing complex structs
│   ├── week5_catalyst/                # Physical plans, pushdown, & partition tuning
│   ├── week6_skew_joins/              # Broadcast joins & salting for data skew
│   ├── week7_delta_lake/              # ACID transactions & MERGE / upsert logic
│   └── week8_pipeline/                # Production orchestration module
├── tests/
│   ├── __init__.py
│   ├── test_transformations.py        # DataFrame unit tests using pytest & chispa
│   └── test_optimizations.py          # Validation tests for join/skew logic
├── .gitignore                         # Excludes .venv, .parquet, spark-warehouse
├── pyproject.toml                     # Modern dependency management via uv
├── uv.lock                            # Locked exact versions for reproducibility
└── README.md                          # Master professional documentation

```

---

## 🗺️ Detailed 8-Week Curriculum & Roadmap

### Phase 1: Foundations & Cluster Architecture

* **Week 1: Foundations & Architecture** (`week1_architecture/`)
* *Concepts:* Local development setup (`uv`, Java JDK, PySpark), `SparkSession` instantiation and configuration tuning, understanding the physical cluster boundary (Driver vs. Executors), Jobs, Stages, and Tasks.
* *Engineering Focus:* Grasping lazy evaluation, triggering actions vs. transformations, and visually inspecting execution Directed Acyclic Graphs (DAGs) in the Spark UI (`localhost:4040`).


* **Week 2: Optimized I/O & Explicit Schemas** (`week2_io_parquet/`)
* *Concepts:* Enforcing strict `StructType` and `StructField` schemas to avoid expensive implicit schema inference passes over raw data files.
* *Engineering Focus:* Ingesting messy raw CSV/JSON logs, handling null values and malformed records cleanly, and writing out compressed, columnar **Parquet** data partitioned by date to minimize disk I/O.



### Phase 2: Core Data Transformation & Interview Patterns

* **Week 3: Advanced Windows & Aggregations** (`week3_windows/`)
* *Concepts:* Window frames (`Window.partitionBy()`), ranking functions (`row_number`, `rank`, `dense_rank`), and analytical offsets (`lag`, `lead`).
* *Engineering Focus:* Solving classic data engineering coding interview challenges such as Top-$N$ items per category, running totals, moving averages, and clickstream sessionization.


* **Week 4: Complex & Nested Data Structures** (`week4_nested_json/`)
* *Concepts:* Working with nested schemas, `ArrayType`, `MapType`, and deeply nested JSON API logs.
* *Engineering Focus:* Flattening nested data payloads using `explode()`, `posexplode()`, `from_json()`, and `to_json()` while handling structural anomalies without data loss.



### Phase 3: Internals, Performance Tuning & Optimization

* **Week 5: Catalyst Optimizer & Partition Tuning** (`week5_catalyst/`)
* *Concepts:* The Catalyst Optimizer and Tungsten execution engine; reading physical plans via `.explain(True)`.
* *Engineering Focus:* Verifying predicate pushdown and column pruning, diagnosing the "Small Files Problem", and strategically managing partition counts using `repartition()` vs. `coalesce()`.


* **Week 6: Shuffles & Skew Mitigation** (`week6_skew_joins/`)
* *Concepts:* Shuffle mechanics, Sort-Merge Joins vs. Broadcast Hash Joins, and identifying straggler tasks caused by data skew.
* *Engineering Focus:* Eliminating network shuffles using `broadcast()`, implementing the **Salting Pattern** to distribute skewed keys, and utilizing Adaptive Query Execution (AQE).



### Phase 4: Production Engineering & Modern Data Lakes

* **Week 7: Delta Lake & ACID Transactions** (`week7_delta_lake/`)
* *Concepts:* Transitioning from flat Parquet files to open-source Lakehouses using **Delta Lake**.
* *Engineering Focus:* Implementing ACID transactions, handling schema evolution, writing idempotent `MERGE INTO` (upsert) pipelines, and leveraging Time Travel.


* **Week 8: Production Orchestration & Testing** (`week8_pipeline/`)
* *Concepts:* Modular software design, decoupling business transformation functions from cluster I/O, and automated CI/CD validation.
* *Engineering Focus:* Building end-to-end robust pipelines, writing comprehensive unit tests with `pytest` and `chispa`, and ensuring green builds via GitHub Actions.



---

## ⚡ Quick Start & Development

This repository uses [uv](https://github.com/astral-sh/uv) for lightning-fast dependency management and virtual environments.

### 1. Clone & Set Up Environment

```bash
git clone [https://github.com/vi-srk/pyspark-data-engineering-mastery.git](https://github.com/vi-srk/pyspark-data-engineering-mastery.git)
cd pyspark-data-engineering-mastery

# Sync dependencies using uv
uv sync

```

### 2. Run the Test Suite

Verify your installation and run the PySpark DataFrame unit tests locally using `pytest`:

```bash
uv run pytest -v

```

---

## 🧪 Testing Philosophy

Senior data engineers test Spark code without requiring heavy live clusters. This repo utilizes `pytest` and `chispa` to assert DataFrame schema and value equality locally:

```python
from pyspark.sql import SparkSession
from chispa.dataframe_comparer import assert_approx_df_equality
from src.week3_windows.transformations import calculate_tiers

def test_customer_tier_calculation(spark: SparkSession):
    input_df = spark.createDataFrame([("c1", 15000.0)], ["customer_id", "lifetime_spend"])
    result_df = calculate_tiers(input_df)
    
    expected_df = spark.createDataFrame([("c1", 15000.0, "VIP")], ["customer_id", "lifetime_spend", "tier"])
    assert_approx_df_equality(result_df, expected_df, precision=1e-5)

```

---

## 👤 Author

**Vishnu Soman**

* Aspiring Data Engineer building robust, testable, and scalable data systems.
* Check out my companion repositories:
* [Modern Python Data Engineering Mastery](https://www.google.com/search?q=https://github.com/vi-srk/modern-python-data-pipeline)
* [SQL Data Engineering Mastery](https://www.google.com/search?q=https://github.com/vi-srk/sql-data-engineering-mastery)



---

## 📜 License

Distributed under the **MIT License**. See `LICENSE` for more information.

```

```