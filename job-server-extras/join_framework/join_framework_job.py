#!/usr/bin/env python3
"""
join-framework context management

This script will reboot all managed contexts on daily running.

1. This script should dependent on the buffalo task of loaded dim table;
2. Other task should dependent on this script to make sure dim table loaded.
"""

import time
import logging

from . import join_framework_intg as intg

logger = logging.getLogger("join-framework")

if __name__ == "__main__":
    logging.basicConfig(
        format='%(asctime)s %(levelname)s %(name)s: %(message)s',
        level=logging.INFO)
    context_operation = intg.ContextOperation()
    logger.info("Start re-booting join-framework.")

    # 1. Clear all existing context.

    context_operation.clear_all()
    logger.info("Requested clear all context, waiting 30s before re-create...")
    time.sleep(30)

    # 2. Create a new default context.
    ret_for_create_context = context_operation.create_default()
    if ret_for_create_context["status"] == "SUCCESS":
        logger.info("Default context created.")
    else:
        err_msg = "Error creating default context!"
        logger.error(err_msg)
        raise Exception(err_msg)

    # 3. Load dim table and check.
    job_operation = intg.JobOperation()
    ret_for_load_table = job_operation.load_and_cache_table()
    logger.info("Dim table loaded.")
    ret_for_check_sql = job_operation.run_sql("SELECT * from dim_product_daily_item_sku LIMIT 10")
    logger.info("Cache check passed.")
    logger.info("join-framework re-booted!")
