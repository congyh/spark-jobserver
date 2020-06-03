#!/usr/bin/env python3
"""
Context Operations

GET /contexts               - lists all current contexts
GET /contexts/<name>        - gets info about a context, such as the spark UI url
POST /contexts/<name>       - creates a new context
DELETE /contexts/<name>     - stops a context and all jobs running in it. Additionally,
    you can pass ?force=true to stop a context forcefully. This is equivalent to killing
    the application from SparkUI (works for spark standalone only).
PUT /contexts?reset=reboot  - shuts down all contexts and re-loads only the contexts from config.
    Use ?sync=false to execute asynchronously.

Job Operations

Jobs submitted to the job server must implement a SparkJob trait. It has a main runJob method
which is passed a SparkContext and a typesafe Config object. Results returned by the method
are made available through the REST API.

GET /jobs                - Lists the last N jobs
GET /jobs/<jobId>/config - Gets the job configuration
GET /jobs/<jobId>        - Gets the result or status of a specific job
POST /jobs               - Starts a new job, use ?sync=true to wait for results
DELETE /jobs/<jobId>     - Kills the specified job
"""

import copy
import json
import logging
import sys
import time
from functools import partial
from functools import wraps
from urllib import request, parse, error

logger = logging.getLogger("join_framework")
base_url = "http://10.198.47.106:8090"
default_context_name = "join-framework-context-0"
default_app_name = "join-framework"
load_and_cache_table_classpath = "spark.jobserver.LoadAndCacheTableJob"
run_sql_with_output_classpath = "spark.jobserver.RunSqlWithOutputJob"


def retry(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        for i in range(3):
            try:
                return func(*args, **kwargs)
            except error.HTTPError as e:
                logger.warning("Exception thrown when running [{func_name}], "
                               "retrying after 3s...".format(func_name=func.__name__))
                logger.warning(e)
                time.sleep(3)

    return wrapper


def with_response(func):
    """Read from request and decode to json"""

    @wraps(func)
    def wrapper(*args, **kwargs):
        resp = json.loads(request.urlopen(func(*args, **kwargs)).read().decode('utf-8'))
        logger.debug("Response of [{func_name}]: [{resp}]".format(
            func_name=func.__name__, resp=resp))
        return resp

    return wrapper


def rest_request(request_type="GET"):
    """Turn request to specified request type"""

    def decorate(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            req = func(*args, **kwargs)
            req.get_method = lambda: request_type
            return req

        return wrapper

    return decorate


post_request = partial(rest_request, request_type="POST")()
put_request = partial(rest_request, request_type="PUT")()
delete_request = partial(rest_request, request_type="DELETE")()


def bool_to_lower_string(boolean_var):
    return str(boolean_var).lower()


def merge_dicts(default_dict, newer_dict):
    """Merge two dicts

    merge rule::

    default_dict = {'a': 1, 'b': 2}
    newer_dict = {'b': 3, 'c': 4}

    return:
    {'a': 1, 'b': 3, 'c': 4}

    """
    ret_dict = copy.deepcopy(default_dict)
    ret_dict.update(newer_dict)

    return ret_dict


class Context:
    """Context of spark-jobserver"""

    def __init__(self, name=default_context_name):
        self.name = name


class RestOperation:
    def __init__(self, category=""):
        self.url = base_url + category

    @staticmethod
    def _encode_query_params(query_params):
        query_string = parse.urlencode(query_params)
        logger.debug("Encoded query string: [{query_str}]".format(query_str=query_string))
        return query_string


class ContextOperation(RestOperation):
    def __init__(self):
        super().__init__("/contexts")
        self._default_context = Context()

    @with_response
    def list(self):
        return request.Request(self.url)

    @with_response
    def get_info(self, name):
        return request.Request(self.url + "/" + name)

    @with_response
    @post_request
    def create(self, name="", query_params={}):
        if len(name) == 0:
            name = self._default_context.name
        query_params["context-factory"] = "spark.jobserver.context.SessionContextFactory"
        query_string = self._encode_query_params(query_params)
        return request.Request(self.url + "/" + name + "?" + query_string)

    def create_default(self):
        query_params = {
            "num-cpu-cores": "8",
            "memory-per-node": "40G",
            "launcher.spark.driver.memory": "16g",
            "launcher.spark.dynamicAllocation.enabled": "false",
            "launcher.spark.executor.instances": "25",
            "launcher.spark.executor.cores": "8",
            "launcher.spark.yarn.executor.memoryOverhead": "6144",
        }
        return self.create(query_params=query_params)

    @with_response
    @delete_request
    def delete(self, name):
        """Stop a context

        YARN app will be marked as FINISHED."""
        return request.Request(self.url + "/" + name)

    @retry
    @with_response
    @put_request
    def clear_all(self, blocking=False):
        return request.Request(self.url + "?reset=reboot&sync=" + bool_to_lower_string(blocking))


class JobOperation(RestOperation):
    def __init__(self):
        super().__init__("/jobs")
        self.last_submit_id = ""
        self._default_context = Context()
        self._default_query_params = {
            # appName is the path variable of uploaded jar.
            "appName": default_app_name,
            "context": self._default_context.name
        }

    @with_response
    def list(self):
        return request.Request(self.url)

    @with_response
    def get_info(self, id=""):
        """Get config of job (not running status)"""
        return request.Request(self.url + "/" + self._get_job_id(id) + "/config")

    @retry
    @with_response
    def get_status(self, id=""):
        """Get run info of job

        A typical use-case is to loop determine if a async job has finished."""
        return request.Request(self.url + "/" + self._get_job_id(id))

    def _get_job_id(self, id):
        if len(id) > 0:
            return id
        elif len(self.last_submit_id) > 0:
            return self.last_submit_id
        else:
            raise Exception("Job id required!")

    def run(self, query_params, blocking=True, form={}):
        """Submit a job

        Return immediately if blocking param is set to false or
        waiting for job to finish."""
        job_submit_ret = self._run_async(query_params, form)

        if not blocking:
            return job_submit_ret
        else:
            job_id = job_submit_ret["jobId"]
            self.last_submit_id = job_id
            job_status = {}
            while True:
                job_status = self.get_status(id=job_id)
                running_status = job_status["status"]
                if running_status == "ERROR":
                    raise Exception(job_status["result"])
                elif running_status in ["STARTED", "RUNNING"]:
                    logger.info("Job id: [{job_id}] not finished yet, now in status: [{job_status}]"
                                .format(job_id=job_id, job_status=running_status))
                    time.sleep(10)
                elif running_status == "FINISHED":
                    break
                else:
                    raise Exception(job_status["result"])

            result = job_status["result"]
            logger.info("Job id: [{job_id}] finished!".format(job_id=job_id))
            logger.debug("Job id: [{job_id}] result: \n [{result}].".format(job_id=job_id, result=result))
            return result

    @with_response
    @post_request
    def _run_async(self, query_params, form):
        """Run job in pre-created context

        It is recommended that you call load_and_cache_table first."""
        assert query_params is not None
        assert form is not None

        query_params["sync"] = "false"
        query_string = self._encode_query_params(query_params)
        logger.debug("Form: [{form}]".format(form=form))

        if len(form) == 0:
            return request.Request(self.url + '?' + query_string)
        else:
            return request.Request(self.url + '?' + query_string, data=json.dumps(form).encode("utf-8"))

    @with_response
    def delete(self, id=""):
        return request.Request(self.url + "/" + self._get_job_id(id))

    def _run_built_in_job(self, query_params, context, blocking=True, form={}):
        if context is not None:
            query_params["context"] = context.name
        query_params = merge_dicts(self._default_query_params, query_params)

        return self.run(query_params, blocking=blocking, form=form)

    def load_and_cache_table(self, context=None, blocking=True):
        """Pre-load large table to specified context

        All preceding query can benefit from cache."""
        query_params = {"classPath": load_and_cache_table_classpath}
        return self._run_built_in_job(query_params, context, blocking=blocking)

    def run_sql(self, sql, context=None, blocking=True):
        """Run job in pre-created context

        It is recommended that you call load_and_cache_table first."""
        query_params = {"classPath": run_sql_with_output_classpath}
        form = {"sql": sql}

        return self._run_built_in_job(query_params, context, blocking=blocking, form=form)


if __name__ == "__main__":
    logging.basicConfig(
        format='%(asctime)s %(levelname)s %(name)s: %(message)s',
        level=logging.DEBUG)

    context = None if len(sys.argv) < 2 else Context(name=sys.argv[1])

    context_operation = ContextOperation()
    context_operation.list()

    job_operation = JobOperation()
    job_operation.load_and_cache_table()
    # sql = "select * from dim_product_daily_item_sku limit 10"
    # job_operation.run_sql(sql=sql, context=context)

    # context_operation.delete("sql-context-0")
