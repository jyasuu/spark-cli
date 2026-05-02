#!/bin/bash

# 透過 docker exec 執行容器內的 spark-sql
# -e 參數可以直接帶入 SQL 指令
docker exec -it spark-iceberg spark-sql -e "SELECT     floor(age/10)*10 as age_group,     count(*) as count FROM demo.demo.users_generated GROUP BY 1 ORDER BY age_group;"

# Setting default log level to "WARN".
# To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
# 26/05/02 09:06:08 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
# 26/05/02 09:06:09 WARN Utils: Service 'SparkUI' could not bind on port 4040. Attempting port 4041.
# Spark Web UI available at http://323f14c36de4:4041
# Spark master: local[*], Application Id: local-1777712769742
# 10      470
# 20      2374
# 30      2297
# 40      2282
# 50      2326
# 60      251
# Time taken: 7.044 seconds, Fetched 6 row(s)
