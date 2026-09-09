"""Command line / Databricks job entry point.

Installed by the wheel as ``mdm-match``, so a Databricks **Python wheel task** can run:

    mdm-match --country MY
    mdm-match --all
    mdm-match --country MY --country SG
    mdm-match --list

``--conf-dir`` points at a config folder outside the wheel (same as ``MDM_CONF_DIR``).
"""
from __future__ import annotations

import argparse
import os
import sys
from typing import List, Optional


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(prog="mdm-match", description="Run the MDM match engine.")
    parser.add_argument("--country", action="append", metavar="CC", help="Country code to run; repeatable.")
    parser.add_argument("--all", action="store_true", help="Run every country that has a config file.")
    parser.add_argument("--list", action="store_true", help="List the configured countries and exit.")
    parser.add_argument("--conf-dir", help="Config folder to use instead of the one bundled in the wheel.")
    args = parser.parse_args(argv)

    if args.conf_dir:
        os.environ["MDM_CONF_DIR"] = args.conf_dir

    from matching.config import available_countries

    if args.list:
        print("\n".join(available_countries()) or "(no country configs found)")
        return 0
    if not args.all and not args.country:
        parser.error("pass --country CC (repeatable), --all, or --list")

    from pyspark.sql import SparkSession

    # Session first: importing the engine builds Spark Columns, which needs a live session.
    spark = SparkSession.builder.getOrCreate()

    from matching.pipeline import run_all, run_country

    if args.all:
        run_all(spark)
    else:
        for country_code in args.country:
            run_country(spark, country_code)
    return 0


if __name__ == "__main__":
    sys.exit(main())
