import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when


def create_spark_session(app_name: str = "Week1-Spark-Architecture") -> SparkSession:
    """Create and return a local SparkSession for learning and UI inspection."""
    return (
        SparkSession.builder.appName(app_name)
        # Run locally using all available cores; set shuffle partitions low for local dev
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def demonstrate_lazy_evaluation(spark: SparkSession) -> None:
    """Show that transformations build a plan without running until an action is called."""
    sample_rows = [
        ("evt-1", "US", 120.0),
        ("evt-2", "IN", 45.0),
        ("evt-3", "CA", 650.0),
        ("evt-4", "US", 15.0),
        ("evt-5", "DE", 980.0),
    ]
    df = spark.createDataFrame(sample_rows, ["event_id", "country", "amount"])

    # Lazy transformations: Spark builds the logical/physical plan here without executing
    transformed_df = (
        df.filter(col("amount") >= 50.0)
        .withColumn(
            "order_size",
            when(col("amount") >= 500.0, "large").otherwise("medium"),
        )
        .select("event_id", "country", "amount", "order_size")
    )

    print("\n--- Execution Plan ---")
    transformed_df.explain(mode="simple")

    # Actions trigger actual Spark jobs to compute and return results
    print("\n--- Action: show() triggers execution ---")
    transformed_df.show(truncate=False)

    print(f"Filtered row count (Action: count()): {transformed_df.count()}")


def main() -> None:
    spark = create_spark_session()
    try:
        print(f"Spark Version: {spark.version}")
        print("Spark UI available at: http://localhost:4040")
        demonstrate_lazy_evaluation(spark)

        # Keep the JVM/session alive briefly so you can inspect the Spark UI in a browser
        print("\nSleeping 15 seconds so you can inspect http://localhost:4040...")
        time.sleep(15)
    finally:
        # Always stop the session so local Spark processes don't linger
        spark.stop()
        print("SparkSession stopped cleanly.")


if __name__ == "__main__":
    main()