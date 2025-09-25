from pyspark.sql import SparkSession, functions as F, Window
import os
import logging
from config import (
    CLAIMS_DIR,
    CLAIMS_SCHEMA,
    TASK_4_RESULT
)


logger = logging.getLogger("hippo.task4")

def setup_logging() -> None:
    """Configure logging for console output. LOG_LEVEL env overrides default INFO."""
    level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)

    if logger.handlers:
        return  

    logger.setLevel(level)
    fmt = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    logger.addHandler(sh)


def top_quantities_per_drug(spark, input_path, result_path):
    """
    Identify up to five most frequently assigned dosages for each medication code (NDC).
    """
    logger.info("Reading claims from %s", input_path)
    claims = (
        spark.read.option("mode", "DROPMALFORMED")
        .schema(CLAIMS_SCHEMA)
        .json(input_path)
    )

    logger.info("Counting frequencies of (ndc, quantity)...")
    freq_table = claims.groupBy("ndc", "quantity").agg(F.count("*").alias("cnt"))

    logger.info("Ranking quantities within each ndc...")
    rank_win = Window.partitionBy("ndc").orderBy(F.desc("cnt"))
    ranked = freq_table.withColumn("pos", F.row_number().over(rank_win))

    logger.info("Selecting top 5 quantities per ndc...")
    top_quantities = (
        ranked.filter(F.col("pos") <= 5)
        .groupBy("ndc")
        .agg(F.collect_list("quantity").alias("top_quantities"))
    )

    logger.info("Saving results to %s", result_path)
    (
        top_quantities
        .coalesce(1)
        .write.mode("overwrite")
        .json(result_path)
    )
    logger.info("Job finished successfully")


if __name__ == "__main__":
    setup_logging()
    logger.info("Task 4 job started")

    session = (
        SparkSession.builder
        .appName("calc-task-4")
        .getOrCreate()
    )

    try:
        top_quantities_per_drug(session, CLAIMS_DIR, TASK_4_RESULT)
    except Exception:
        logger.exception("Unhandled error during job execution")
        raise
    finally:
        session.stop()
        logger.info("Spark session stopped")
