#!/usr/bin/env python3
# TODO: python2 compatible.

import json

from urllib import request, parse

url = 'http://10.198.47.106:8090/jobs'

queryParams = {
    'appName': 'join-framework',
    'classPath': 'spark.jobserver.RunSqlWithOutputJob',
    'context': 'join-framework-context-0',
    'sync': 'true'
}

querystring = parse.urlencode(queryParams)

form = {
    "sql": "select * from dim_product_daily_item_sku limit 10"
}

# Make a POST request and read the response
u = request.urlopen(url + '?' + querystring, data=json.dumps(form).encode("utf-8"))
resp = u.read().decode('utf-8')
print(resp)
