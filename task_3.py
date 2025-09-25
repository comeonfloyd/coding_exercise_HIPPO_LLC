"""
Utility for analyzing pharmacy-related claim records with Spark.
It extracts pharmacy chains offering the most competitive average prices
and organizes the results in a structured JSON format.
"""

import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from config import (
    CLAIMS_DIR,
    PHARMACY_DIR,
    CLAIMS_SCHEMA,
    PHARMACIES_SCHEMA,
    TASK_3_RESULT
)

logger = logging.getLogger("hippo.task3")

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


def find_best_chains(spark: SparkSession, claims_src: str, pharmacies_src: str, dest: str):
    """Identify two cheapest pharmacy chains per NDC code based on claim prices."""
    logger.info("Reading claims from %s", claims_src)
    claims = (
        spark.read.option("mode", "DROPMALFORMED")
        .json(claims_src, schema=CLAIMS_SCHEMA)
    )

    logger.info("Reading pharmacies from %s", pharmacies_src)
    pharmacies = (
        spark.read.option("mode", "DROPMALFORMED")
        .csv(pharmacies_src, schema=PHARMACIES_SCHEMA, header=True)
    )

    logger.info("Joining claims with pharmacies...")
    enriched = claims.join(F.broadcast(pharmacies), on="npi")
    logger.debug("Enriched sample:\n%s", enriched.limit(5).show())

    logger.info("Calculating average prices per chain & NDC...")
    avg_prices = (
        enriched.groupBy("ndc", "chain")
        .agg(F.round(F.mean("price"), 2).alias("mean_price"))
    )

    logger.info("Ranking chains...")
    rank_window = Window.partitionBy("ndc").orderBy(F.col("mean_price"))
    ranked = (
        avg_prices.withColumn("position", F.rank().over(rank_window))
        .where(F.col("position") <= 2)
    )

    logger.info("Aggregating results into JSON structure...")
    output = (
        ranked.groupBy("ndc")
        .agg(
            F.collect_list(
                F.struct(
                    F.col("chain").alias("name"),
                    F.col("mean_price").alias("avg_price"),
                )
            ).alias("chains")
        )
    )

    logger.info("Saving results to %s", dest)
    output.coalesce(1).write.mode("overwrite").json(dest)
    logger.info("Job completed successfully")


if __name__ == "__main__":
    setup_logging()
    logger.info("Task 3 job started")

    session = SparkSession.builder.appName("calc-task-3").getOrCreate()

    try:
        find_best_chains(
            spark=session,
            claims_src=CLAIMS_DIR,
            pharmacies_src=PHARMACY_DIR,
            dest=TASK_3_RESULT,
        )
    except Exception:
        logger.exception("Unhandled error during job execution")
        raise
    finally:
        session.stop()
        logger.info("Spark session stopped")
