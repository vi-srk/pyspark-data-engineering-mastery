# Week 1: Foundations & Spark Architecture

## Overview
Before writing complex transformations or tuning clusters, you need a clear mental model of **how PySpark actually runs code**. Unlike standard Python or Pandas—which execute line by line on a single machine—PySpark coordinates work across a **Driver** and one or more **Executors**, builds an execution plan lazily, and runs tasks only when an **Action** demands a result.

In this module, we set up a clean local `SparkSession`, see lazy evaluation in action, and inspect the execution plan and **Spark UI** (`http://localhost:4040`) so you can connect what you write in Python to what happens under the hood.

---

## Key Concepts

* **`SparkSession`:** The single entry point into PySpark functionality. It manages configuration (such as app name, master URL, and SQL settings) and coordinates DataFrame/SQL execution.
* **Driver vs. Executors:**
  * **Driver:** Runs your main Python program, builds the logical/physical query plan, schedules tasks, and tracks progress.
  * **Executors:** Worker processes that hold data partitions in memory/disk and execute the actual tasks in parallel.
* **Transformations vs. Actions (Lazy Evaluation):**
  * **Transformations** (`select`, `filter`, `withColumn`) are *lazy*—they record *what* to do next without computing results immediately.
  * **Actions** (`show`, `count`, `collect`, `write`) trigger execution so Spark optimizes the plan and runs jobs.
* **Jobs, Stages, and Tasks:**
  * An **Action** kicks off one or more **Jobs**.
  * Jobs split into **Stages** at shuffle boundaries (where data must be redistributed across partitions).
  * Stages split into **Tasks**, where each task processes one data partition.
* **Spark UI (`localhost:4040`):** Available while a `SparkSession` is active—lets you inspect jobs, stages, task durations, and execution plans rather than guessing why code is slow.

---

## Files in This Module
* `README.md` — Concepts, architecture mental model, run instructions, and interview takeaways.
* `main.py` *(next step)* — Creates a local `SparkSession`, demonstrates lazy evaluation vs. actions, prints an execution plan, and pauses briefly so you can inspect the Spark UI.

---

## How to Run
Once `week1_architecture/main.py` is in place, run it from the repository root using `uv`:

```powershell
uv run python week1_architecture/main.py