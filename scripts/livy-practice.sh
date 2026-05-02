#!/bin/bash

# 建立一個互動式 Session 並執行 SQL (簡化範例)
# 註：Livy 較常用於提交 Batch Job，這裡示範邏輯
# 建立一個 PySpark Session，並記下回傳的 id (假設是 0)
curl -X POST -H "Content-Type: application/json" \
  http://localhost:8998/sessions \
  -d '{"kind": "pyspark"}'
# {"id":4,"name":null,"appId":null,"owner":null,"proxyUser":null,"state":"starting","kind":"pyspark","appInfo":{"driverLogUrl":null,"sparkUiUrl":null,"executorLogUrls":null},"log":["stdout: ","\nstderr: "],"ttl":null,"idleTimeout":null,"driverMemory":null,"driverCores":0,"executorMemory":null,"executorCores":0,"conf":{},"archives":[],"files":[],"heartbeatTimeoutInSecond":0,"jars":[],"numExecutors":0,"pyFiles":[],"queue":null}
curl -X POST -H "Content-Type: application/json" \
    http://localhost:8998/sessions/4/statements \
    -d '{
    "code": "spark.sql(\"SHOW NAMESPACES IN demo\").show()"
    }'
# {"id":0,"code":"spark.sql(\"SHOW NAMESPACES IN demo\").show()","state":"waiting","output":null,"progress":0.0,"started":0,"completed":0} 
 curl -X GET http://localhost:8998/sessions/4/statements/0
# {"id":0,"code":"spark.sql(\"SHOW NAMESPACES IN demo\").show()","state":"available","output":{"status":"ok","execution_count":0,"data":{"text/plain":"+---------+\n|namespace|\n+---------+\n|     demo|\n+---------+"}},"progress":1.0,"started":1777714431388,"completed":1777714433439}      
