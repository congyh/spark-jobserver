#!/usr/bin/env python3

import time
import logging

from . import join_framework_intg as intg

logger = logging.getLogger("join_framework")

if __name__ == "__main__":
    logging.basicConfig(
        format='%(asctime)s %(levelname)s %(name)s: %(message)s',
        level=logging.DEBUG)
    context_operation = intg.ContextOperation()

    # 1. Clear all existing context.

    context_operation.clear_all()
    logger.info("Waiting 30s before reboot...")
    time.sleep(30)

    # 2. Create a new default context.
    context_operation.create_default()

    # 3. Load dim table and check.
    job_operation = intg.JobOperation()
    job_operation.load_and_cache_table()
    ret_for_check_sql = job_operation.run_sql("SELECT * from dim_product_daily_item_sku LIMIT 10")
    if ret_for_check_sql