#!/usr/bin/env python3
# scheduler.py — entry point. Run this on your VM. It loops forever.
#
# WHAT IT DOES:
#   Every 30 seconds:
#     1. Reads watermark from target DB ("what time did we last process?")
#     2. Fetches only NEW rows from source PostgreSQL since that time
#     3. Flattens JSONB, resolves append-only logic
#     4. Upserts/inserts into target DB
#     5. Updates watermark
#     6. Sleeps 30 seconds → repeats
#
# If source DB is unreachable: logs error, sleeps, retries — never crashes.
# If one table ETL fails: other tables still process — isolated failures.

import logging
import time
import sys
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("etl.log", mode="a")  # persistent log on VM
    ]
)
logger = logging.getLogger(__name__)

from config import POLL_INTERVAL_SEC
from pipeline import run_cycle


def main():
    logger.info("=" * 60)
    logger.info("  ClearSight 2.0 — ETL Pipeline Starting")
    logger.info(f"  Poll interval : {POLL_INTERVAL_SEC} seconds")
    logger.info(f"  Start time    : {datetime.utcnow().isoformat()}")
    logger.info("=" * 60)

    consecutive_failures = 0
    MAX_CONSECUTIVE_FAILURES = 10   # Alert after 10 straight failures

    while True:
        try:
            run_cycle()
            consecutive_failures = 0   # Reset on success

        except KeyboardInterrupt:
            logger.info("ETL stopped by user (KeyboardInterrupt)")
            sys.exit(0)

        except Exception as e:
            consecutive_failures += 1
            logger.error(
                f"❌ ETL cycle failed (failure #{consecutive_failures}): {e}",
                exc_info=True
            )
            if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                logger.critical(
                    f"🚨 {MAX_CONSECUTIVE_FAILURES} consecutive failures. "
                    f"Check source DB connectivity and logs."
                )
                # Could send an alert here (email, Slack webhook, etc.)

        # Always sleep before next cycle, even after failure
        logger.debug(f"Sleeping {POLL_INTERVAL_SEC}s before next cycle...")
        time.sleep(POLL_INTERVAL_SEC)


if __name__ == "__main__":
    main()
