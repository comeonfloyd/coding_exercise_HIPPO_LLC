"""
This module reads pharmacy, claims, and revert files from specified directories.
It validates the input using Pydantic schemas, computes claim and revert metrics,
and outputs the results in JSON format.
"""

import os
import json
import csv
import logging
from logging.handlers import RotatingFileHandler
from typing import Optional, List

from config import (
    TASK_1_2_RESULT,
    PHARMACIES_REQUIRED_FIELDS,
    CLAIMS_REQUIRED_FIELDS,
    REVERTS_REQUIRED_FIELDS,
    PHARMACY_DIR,
    CLAIMS_DIR,
    REVERTS_DIR,
)


LOGGER = logging.getLogger("hippo.tasks_1_2")

def setup_logging() -> None:
    """
    Configure logging for console + rotating file.
    LOG_LEVEL can be overridden via env, e.g. LOG_LEVEL=DEBUG.
    """
    level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)

    LOGGER.setLevel(level)
    LOGGER.propagate = False  # prevent double logs if root is configured elsewhere

    # Skip if already configured (useful for tests / re-imports)
    if LOGGER.handlers:
        return

    fmt = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Console
    sh = logging.StreamHandler()
    sh.setLevel(level)
    sh.setFormatter(fmt)
    LOGGER.addHandler(sh)

    # Rotating file logs/app.log (5MB x 3 backups)
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)
    fh = RotatingFileHandler(os.path.join(log_dir, "app.log"),
                             maxBytes=5 * 1024 * 1024,
                             backupCount=3,
                             encoding="utf-8")
    fh.setLevel(level)
    fh.setFormatter(fmt)
    LOGGER.addHandler(fh)



def read_json(file_path: str) -> Optional[List]:
    """Read and return data from a JSON file."""
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        LOGGER.debug("Loaded JSON: %s (%d records)", file_path, len(data) if isinstance(data, list) else 1)
        return data
    except json.JSONDecodeError as e:
        LOGGER.error("Error decoding JSON from %s: %s", file_path, e, exc_info=True)
        return None
    except OSError as e:
        LOGGER.error("I/O error reading JSON %s: %s", file_path, e, exc_info=True)
        return None

def read_csv(file_path: str) -> Optional[List]:
    """Read and return data from a CSV file."""
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))
        LOGGER.debug("Loaded CSV: %s (%d rows)", file_path, len(rows))
        return rows
    except csv.Error as e:
        LOGGER.error("Error reading CSV from %s: %s", file_path, e, exc_info=True)
        return None
    except OSError as e:
        LOGGER.error("I/O error reading CSV %s: %s", file_path, e, exc_info=True)
        return None

def read_file(file_path: str) -> Optional[List]:
    """Read and return data from a JSON or CSV file depending on extension."""
    if file_path.endswith(".json"):
        return read_json(file_path)
    if file_path.endswith(".csv"):
        return read_csv(file_path)
    LOGGER.warning("Unsupported file format: %s", file_path)
    return None



def load_json_files(directory: str, required_fields):
    """Load, validate, and return data from all files in a directory against a specified schema. Goal 1"""
    data: List[dict] = []
    LOGGER.info("Loading files from %s", directory)

    for fname in os.listdir(directory):
        filepath = os.path.join(directory, fname)
        try:
            records = read_file(filepath)
            if not records:
                LOGGER.warning("No records loaded from %s", filepath)
                continue

            for record in records:
                missing = [f for f in required_fields if f not in record or record[f] in (None, "")]
                if missing:
                    LOGGER.warning("Skipped record from %s: missing %s | record=%s", filepath, missing, record)
                else:
                    data.append(record)

        except Exception as e:
            # Catch-all so batch run continues; keep stack trace
            LOGGER.exception("Unexpected error reading %s: %s", filepath, e)

    LOGGER.info("Loaded %d valid records from %s", len(data), directory)
    return data

def save_results_to_json(output_path: str, results: List) -> None:
    """Save data to a JSON file with formatting."""
    try:
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=4, default=str)
        LOGGER.info("Result successfully saved to %s (%d rows)", output_path, len(results))
    except OSError as e:
        LOGGER.error("Error writing result to %s: %s", output_path, e, exc_info=True)

def calculate_metrics(claims: List, reverts: List, pharmacies: List) -> List:
    """Calculate metrics based on claims and reverts data. Goal 2"""
    LOGGER.info("Calculating metrics: claims=%d, reverts=%d, pharmacies=%d",
                len(claims), len(reverts), len(pharmacies))

    metrics = {}
    unique_npi = {record.get("npi") for record in pharmacies if "npi" in record}

    # Process claims
    for claim in claims:
        npi = claim.get("npi")
        ndc = claim.get("ndc")
        price = claim.get("price", 0.0)

        if npi not in unique_npi:
            continue

        key = (npi, ndc)
        if key not in metrics:
            metrics[key] = {"fills": 0, "reverted": 0, "total_price": 0.0, "avg_price": 0.0}

        metrics[key]["fills"] += 1
        try:
            metrics[key]["total_price"] += float(price)
        except (TypeError, ValueError):
            LOGGER.warning("Invalid price in claim: %s (treated as 0)", price)

    # Process reverts
    reverted_claims = {revert.get("claim_id") for revert in reverts}
    for claim in claims:
        if claim.get("id") in reverted_claims:
            key = (claim.get("npi"), claim.get("ndc"))
            if key in metrics:
                metrics[key]["reverted"] += 1

    # Calculate average price
    for key, d in metrics.items():
        if d["fills"] > 0:
            d["avg_price"] = d["total_price"] / d["fills"]

    # Format the results
    formatted_results = [
        {
            "npi": npi,
            "ndc": ndc,
            "fills": d["fills"],
            "reverted": d["reverted"],
            "avg_price": round(d["avg_price"], 2),
            "total_price": round(d["total_price"], 5),
        }
        for (npi, ndc), d in metrics.items()
    ]

    LOGGER.info("Metrics ready: %d groups", len(formatted_results))
    LOGGER.debug("Sample metric row: %s", formatted_results[0] if formatted_results else None)
    return formatted_results



if __name__ == "__main__":
    setup_logging()
    LOGGER.info("Job started")

    # Read JSONs
    pharmacy_data = load_json_files(PHARMACY_DIR, PHARMACIES_REQUIRED_FIELDS)
    claims_data = load_json_files(CLAIMS_DIR, CLAIMS_REQUIRED_FIELDS)
    reverts_data = load_json_files(REVERTS_DIR, REVERTS_REQUIRED_FIELDS)

    # Calculate metrics
    metrics_results = calculate_metrics(claims_data, reverts_data, pharmacy_data)

    # Save the data
    save_results_to_json(TASK_1_2_RESULT, metrics_results)

    LOGGER.info("Job finished")
