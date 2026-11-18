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

