#!/usr/bin/env python3
"""
Contexts

GET /contexts               - lists all current contexts
GET /contexts/<name>        - gets info about a context, such as the spark UI url
POST /contexts/<name>       - creates a new context
DELETE /contexts/<name>     - stops a context and all jobs running in it. Additionally,
    you can pass ?force=true to stop a context forcefully. This is equivalent to killing
    the application from SparkUI (works for spark standalone only).
PUT /contexts?reset=reboot  - shuts down all contexts and re-loads only the contexts from config.
    Use ?sync=false to execute asynchronously.

Jobs

Jobs submitted to the job server must implement a SparkJob trait. It has a main runJob method
which is passed a SparkContext and a typesafe Config object. Results returned by the method
are made available through the REST API.

GET /jobs                - Lists the last N jobs
GET /jobs/<jobId>/config - Gets the job configuration
GET /jobs/<jobId>        - Gets the result or status of a specific job
POST /jobs               - Starts a new job, use ?sync=true to wait for results
DELETE /jobs/<jobId>     - Kills the specified job
"""

import json
import logging
import time
from functools import partial
from functools import wraps
from urllib import request, parse

logger = logging.getLogger("join_framework")
base_url = "http://10.198.47.106:8090"


def with_response(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        resp = request.urlopen(func(*args, **kwargs)).read().decode('utf-8')
        logger.debug("Response of {}: {}".format(func.__name__, resp))
        return resp

    return wrapper


def rest_request(request_type="GET"):
    def decorate(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            req = func(*args, **kwargs)
            req.get_method = lambda: request_type
            return req
        return wrapper
    return decorate


put_request = partial(rest_request, request_type="PUT")()
delete_request = partial(rest_request, request_type="DELETE")()


class Context:
    def __init__(self, name="join-framework-context-0"):
        self.name = name


class Operation:
    def __init__(self, category=""):
        self.url = base_url + category


class ContextOperation(Operation):
    def __init__(self):
        super().__init__("/contexts")

    @with_response
    def list(self):
        return request.Request(self.url)

    @with_response
    def get_info(self, name):
        return request.Request(self.url + "/" + name)

    @with_response
    def create(self, name):
        pass

    @with_response
    @delete_request
    def delete(self, name):
        return request.Request(self.url + "/" + name)

    @with_response
    @put_request
    def reboot_all(self):
        return request.Request(self.url + "?reset=reboot?sync=false")


class JobOperation(Operation):
    def __init__(self):
        super().__init__("/jobs")
        self.last_submit_id = ""
        self.default_context = Context("join-framework-context-0")

    @with_response
    def list(self):
        return request.Request(self.url)

    @with_response
    def get_info(self, id=""):
        """Get config of job (not running status)"""
        return request.Request(self.url + "/" + self.__get_job_id(id) + "/config")

    @with_response
    def get_status(self, id=""):
        """Get run info of job

        A typical use-case is to loop determine if a async job has finished."""
        return request.Request(self.url + "/" + self.__get_job_id(id))

    def __get_job_id(self, id):
        if len(id) > 0:
            return id
        elif len(self.last_submit_id) > 0:
            return self.last_submit_id
        else:
            raise Exception("Job id required!")

    @with_response
    def create(self, context=None):
        if context is None:
            context = self.default_context
        pass

    @with_response
    def delete(self, id=""):
        return request.Request(self.url + "/" + self.__get_job_id(id))

    def run_sql(self, sql, context=None, blocking=True):
        if context is None:
            context = self.default_context

        job_submit_ret_str = self.__run_sql_async(sql, context)

        if not blocking:
            return job_submit_ret_str
        else:
            job_id = json.loads(job_submit_ret_str)["jobId"]
            self.last_submit_id = job_id
            job_status = {}
            while True:
                job_status = json.loads(self.get_status(job_id))
                running_status = job_status["status"]
                if running_status != "FINISHED":
                    logger.info("Job id: [{job_id}] not finished yet, now in status: [{job_status}]"
                                   .format(job_id=job_id, job_status=running_status))
                    time.sleep(10)
                else:
                    break

            result = job_status["result"]
            logger.info("Job id: [{job_id}] finished!".format(job_id=job_id))
            logger.debug("Job id: [{job_id}] result: \n {result}.".format(job_id=job_id, result=result))
            return result

    @with_response
    def __run_sql_async(self, sql, context=None):
        """Run Sql in pre-created context

        It is recommended that you load and cache large dim table first."""
        query_params = {
            # appName is the path variable of uploaded jar.
            'appName': 'join-framework',
            'classPath': 'spark.jobserver.RunSqlWithOutputJob',
            'context': context.name,
            'sync': "false"
        }

        query_string = parse.urlencode(query_params)
        logger.debug("Encoded query string: {}".format(query_string))

        form = {
            "sql": sql
        }

        # Make a POST request
        return request.Request(self.url + '?' + query_string, data=json.dumps(form).encode("utf-8"))


if __name__ == "__main__":
    logging.basicConfig(
        format='%(asctime)s %(levelname)s %(name)s: %(message)s',
        level=logging.DEBUG)

    # context = None if len(sys.argv) < 2 else Context(sys.argv[1])
    #
    context_operation = ContextOperation()
    context_operation.list()
    #
    # job_operation = JobOperation()
    # sql = "select * from dim_product_daily_item_sku limit 10"
    # job_operation.run_sql(sql=sql, context=context)

    context_operation.delete("sql-context-0")