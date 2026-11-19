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

## How `main.py` Works

1. **Create a local `SparkSession` (`create_spark_session`):** Initializes Spark in `local[*]` mode (using available CPU cores on your machine) with a custom app name and `spark.sql.shuffle.partitions` set to `4` so local shuffle operations stay lightweight.
2. **Create sample data & define lazy transformations (`demonstrate_lazy_evaluation`):** Builds a small DataFrame and chains `filter()`, `withColumn()`, and `select()`. Because transformations are lazy, Spark records these steps in a plan rather than computing them right away.
3. **Inspect the execution plan (`.explain(mode="simple")`):** Prints how Spark plans to execute the DataFrame operations before/as actions run.
4. **Trigger execution with Actions (`.show()` and `.count()`):** Calling an action forces Spark to plan, schedule, and run the actual job across partitions.
5. **Pause for Spark UI & clean shutdown (`main`):** Sleeps briefly so you can open `http://localhost:4040` and inspect the job/stages, then calls `spark.stop()` in a `finally` block so resources always release cleanly even if an error occurs.

---

## Using the Jupyter Notebook (`week1_architecture.ipynb`)

`week1_architecture.ipynb` mirrors `main.py` cell by cell so you can run each step interactively and inspect outputs one piece at a time instead of all at once:

1. **Cell 1 — Session setup:** Starts the local `SparkSession` and prints the Spark version and Spark UI URL (`http://localhost:4040`).
2. **Cell 2 — Lazy plan:** Creates the sample DataFrame, chains the transformations (`filter`, `withColumn`, `select`), and prints `.explain(mode="simple")` before triggering any action.
3. **Cell 3 — Actions & UI check:** Runs `.show()` and `.count()` to trigger Spark jobs while the session is still alive, letting you inspect `http://localhost:4040` in your browser.
4. **Cell 4 — Teardown:** Calls `spark.stop()` so local Spark resources are released cleanly.

> **Notebook Tip:** Because `SparkSession` stays active between cells, avoid re-running the session creation cell repeatedly without stopping first, and always run the final `spark.stop()` cell when you finish.
---

## How to Run
Once `week1_architecture/main.py` is in place, run it from the repository root using `uv`:

```powershell
uv run python week1_architecture/main.py