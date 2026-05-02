# docker exec -it spark-iceberg spark-submit   --master "local[*]"   /home/iceberg/scripts/analyze_users.py

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("IcebergAnalysis").getOrCreate()

# 讀取 Iceberg 表
df = spark.read.table("demo.demo.users_generated")

# 數據過濾與計算
result = df.filter(df.age > 50).agg({"age": "avg"})

print("="*30)
print(f"Average age of users over 50:")
result.show()
print("="*30)


# 26/05/02 09:28:24 INFO SparkContext: Running Spark version 3.5.5
# 26/05/02 09:28:24 INFO SparkContext: OS info Linux, 6.1.43, amd64
# 26/05/02 09:28:24 INFO SparkContext: Java version 17.0.14
# 26/05/02 09:28:24 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
# 26/05/02 09:28:24 INFO ResourceUtils: ==============================================================
# 26/05/02 09:28:24 INFO ResourceUtils: No custom resources configured for spark.driver.
# 26/05/02 09:28:24 INFO ResourceUtils: ==============================================================
# 26/05/02 09:28:24 INFO SparkContext: Submitted application: IcebergAnalysis
# 26/05/02 09:28:24 INFO ResourceProfile: Default ResourceProfile created, executor resources: Map(cores -> name: cores, amount: 1, script: , vendor: , memory -> name: memory, amount: 1024, script: , vendor: , offHeap -> name: offHeap, amount: 0, script: , vendor: ), task resources: Map(cpus -> name: cpus, amount: 1.0)
# 26/05/02 09:28:24 INFO ResourceProfile: Limiting resource is cpu
# 26/05/02 09:28:24 INFO ResourceProfileManager: Added ResourceProfile id: 0
# 26/05/02 09:28:24 INFO SecurityManager: Changing view acls to: root
# 26/05/02 09:28:24 INFO SecurityManager: Changing modify acls to: root
# 26/05/02 09:28:24 INFO SecurityManager: Changing view acls groups to: 
# 26/05/02 09:28:24 INFO SecurityManager: Changing modify acls groups to: 
# 26/05/02 09:28:24 INFO SecurityManager: SecurityManager: authentication disabled; ui acls disabled; users with view permissions: root; groups with view permissions: EMPTY; users with modify permissions: root; groups with modify permissions: EMPTY
# 26/05/02 09:28:24 INFO Utils: Successfully started service 'sparkDriver' on port 39867.
# 26/05/02 09:28:24 INFO SparkEnv: Registering MapOutputTracker
# 26/05/02 09:28:24 INFO SparkEnv: Registering BlockManagerMaster
# 26/05/02 09:28:24 INFO BlockManagerMasterEndpoint: Using org.apache.spark.storage.DefaultTopologyMapper for getting topology information
# 26/05/02 09:28:24 INFO BlockManagerMasterEndpoint: BlockManagerMasterEndpoint up
# 26/05/02 09:28:24 INFO SparkEnv: Registering BlockManagerMasterHeartbeat
# 26/05/02 09:28:24 INFO DiskBlockManager: Created local directory at /tmp/blockmgr-c403f7b8-51d1-426b-9aa3-aa39005ce323
# 26/05/02 09:28:24 INFO MemoryStore: MemoryStore started with capacity 434.4 MiB
# 26/05/02 09:28:24 INFO SparkEnv: Registering OutputCommitCoordinator
# 26/05/02 09:28:25 INFO JettyUtils: Start Jetty 0.0.0.0:4040 for SparkUI
# 26/05/02 09:28:25 WARN Utils: Service 'SparkUI' could not bind on port 4040. Attempting port 4041.
# 26/05/02 09:28:25 INFO Utils: Successfully started service 'SparkUI' on port 4041.
# 26/05/02 09:28:25 INFO Executor: Starting executor ID driver on host 323f14c36de4
# 26/05/02 09:28:25 INFO Executor: OS info Linux, 6.1.43, amd64
# 26/05/02 09:28:25 INFO Executor: Java version 17.0.14
# 26/05/02 09:28:25 INFO Executor: Starting executor with user classpath (userClassPathFirst = false): ''
# 26/05/02 09:28:25 INFO Executor: Created or updated repl class loader org.apache.spark.util.MutableURLClassLoader@623f6abc for default.
# 26/05/02 09:28:25 INFO Utils: Successfully started service 'org.apache.spark.network.netty.NettyBlockTransferService' on port 34391.
# 26/05/02 09:28:25 INFO NettyBlockTransferService: Server created on 323f14c36de4:34391
# 26/05/02 09:28:25 INFO BlockManager: Using org.apache.spark.storage.RandomBlockReplicationPolicy for block replication policy
# 26/05/02 09:28:25 INFO BlockManagerMaster: Registering BlockManager BlockManagerId(driver, 323f14c36de4, 34391, None)
# 26/05/02 09:28:25 INFO BlockManagerMasterEndpoint: Registering block manager 323f14c36de4:34391 with 434.4 MiB RAM, BlockManagerId(driver, 323f14c36de4, 34391, None)
# 26/05/02 09:28:25 INFO BlockManagerMaster: Registered BlockManager BlockManagerId(driver, 323f14c36de4, 34391, None)
# 26/05/02 09:28:25 INFO BlockManager: Initialized BlockManager: BlockManagerId(driver, 323f14c36de4, 34391, None)
# 26/05/02 09:28:25 INFO SingleEventLogFileWriter: Logging events to file:/home/iceberg/spark-events/local-1777714105143.inprogress
# 26/05/02 09:28:25 INFO SharedState: Setting hive.metastore.warehouse.dir ('null') to the value of spark.sql.warehouse.dir.
# 26/05/02 09:28:25 INFO SharedState: Warehouse path is 'file:/opt/spark/spark-warehouse'.
# 26/05/02 09:28:26 INFO CatalogUtil: Loading custom FileIO implementation: org.apache.iceberg.aws.s3.S3FileIO
# ==============================
# Average age of users over 50:
# 26/05/02 09:28:27 INFO V2ScanRelationPushDown: 
# Pushing operators to demo.demo.users_generated
# Pushed Filters: age IS NOT NULL, age > 50
# Post-Scan Filters: isnotnull(age#1),(age#1 > 50)
         
# 26/05/02 09:28:27 INFO V2ScanRelationPushDown: 
# Output: age#1
         
# 26/05/02 09:28:27 INFO SnapshotScan: Scanning table demo.demo.users_generated snapshot 3928395527197996226 created at 2026-05-02T08:01:48.386+00:00 with filter (age IS NOT NULL AND age > (2-digit-int))
# 26/05/02 09:28:29 INFO BaseDistributedDataScan: Planning file tasks locally for table demo.demo.users_generated
# 26/05/02 09:28:29 INFO LoggingMetricsReporter: Received metrics report: ScanReport{tableName=demo.demo.users_generated, snapshotId=3928395527197996226, filter=(not_null(ref(name="age")) and ref(name="age") > "(2-digit-int)"), schemaId=0, projectedFieldIds=[2], projectedFieldNames=[age], scanMetrics=ScanMetricsResult{totalPlanningDuration=TimerResult{timeUnit=NANOSECONDS, totalDuration=PT1.341064485S, count=1}, resultDataFiles=CounterResult{unit=COUNT, value=5}, resultDeleteFiles=CounterResult{unit=COUNT, value=0}, totalDataManifests=CounterResult{unit=COUNT, value=5}, totalDeleteManifests=CounterResult{unit=COUNT, value=0}, scannedDataManifests=CounterResult{unit=COUNT, value=5}, skippedDataManifests=CounterResult{unit=COUNT, value=0}, totalFileSizeInBytes=CounterResult{unit=BYTES, value=251708}, totalDeleteFileSizeInBytes=CounterResult{unit=BYTES, value=0}, skippedDataFiles=CounterResult{unit=COUNT, value=0}, skippedDeleteFiles=CounterResult{unit=COUNT, value=0}, scannedDeleteManifests=CounterResult{unit=COUNT, value=0}, skippedDeleteManifests=CounterResult{unit=COUNT, value=0}, indexedDeleteFiles=CounterResult{unit=COUNT, value=0}, equalityDeleteFiles=CounterResult{unit=COUNT, value=0}, positionalDeleteFiles=CounterResult{unit=COUNT, value=0}, dvs=CounterResult{unit=COUNT, value=0}}, metadata={engine-version=3.5.5, iceberg-version=Apache Iceberg 1.8.1 (commit 9ce0fcf0af7becf25ad9fc996c3bad2afdcfd33d), app-id=local-1777714105143, engine-name=spark}}
# 26/05/02 09:28:29 INFO SparkPartitioningAwareScan: Reporting UnknownPartitioning with 2 partition(s) for table demo.demo.users_generated
# 26/05/02 09:28:29 INFO MemoryStore: Block broadcast_0 stored as values in memory (estimated size 32.0 KiB, free 434.4 MiB)
# 26/05/02 09:28:29 INFO MemoryStore: Block broadcast_0_piece0 stored as bytes in memory (estimated size 3.4 KiB, free 434.4 MiB)
# 26/05/02 09:28:29 INFO BlockManagerInfo: Added broadcast_0_piece0 in memory on 323f14c36de4:34391 (size: 3.4 KiB, free: 434.4 MiB)
# 26/05/02 09:28:29 INFO SparkContext: Created broadcast 0 from broadcast at SparkBatch.java:85
# 26/05/02 09:28:29 INFO MemoryStore: Block broadcast_1 stored as values in memory (estimated size 32.0 KiB, free 434.3 MiB)
# 26/05/02 09:28:29 INFO MemoryStore: Block broadcast_1_piece0 stored as bytes in memory (estimated size 3.4 KiB, free 434.3 MiB)
# 26/05/02 09:28:29 INFO BlockManagerInfo: Added broadcast_1_piece0 in memory on 323f14c36de4:34391 (size: 3.4 KiB, free: 434.4 MiB)
# 26/05/02 09:28:29 INFO SparkContext: Created broadcast 1 from broadcast at SparkBatch.java:85
# 26/05/02 09:28:30 INFO CodeGenerator: Code generated in 204.635655 ms
# 26/05/02 09:28:30 INFO DAGScheduler: Registering RDD 3 (showString at NativeMethodAccessorImpl.java:0) as input to shuffle 0
# 26/05/02 09:28:30 INFO DAGScheduler: Got map stage job 0 (showString at NativeMethodAccessorImpl.java:0) with 2 output partitions
# 26/05/02 09:28:30 INFO DAGScheduler: Final stage: ShuffleMapStage 0 (showString at NativeMethodAccessorImpl.java:0)
# 26/05/02 09:28:30 INFO DAGScheduler: Parents of final stage: List()
# 26/05/02 09:28:30 INFO DAGScheduler: Missing parents: List()
# 26/05/02 09:28:30 INFO DAGScheduler: Submitting ShuffleMapStage 0 (MapPartitionsRDD[3] at showString at NativeMethodAccessorImpl.java:0), which has no missing parents
# 26/05/02 09:28:30 INFO MemoryStore: Block broadcast_2 stored as values in memory (estimated size 20.3 KiB, free 434.3 MiB)
# 26/05/02 09:28:30 INFO MemoryStore: Block broadcast_2_piece0 stored as bytes in memory (estimated size 8.5 KiB, free 434.3 MiB)
# 26/05/02 09:28:30 INFO BlockManagerInfo: Added broadcast_2_piece0 in memory on 323f14c36de4:34391 (size: 8.5 KiB, free: 434.4 MiB)
# 26/05/02 09:28:30 INFO SparkContext: Created broadcast 2 from broadcast at DAGScheduler.scala:1585
# 26/05/02 09:28:30 INFO DAGScheduler: Submitting 2 missing tasks from ShuffleMapStage 0 (MapPartitionsRDD[3] at showString at NativeMethodAccessorImpl.java:0) (first 15 tasks are for partitions Vector(0, 1))
# 26/05/02 09:28:30 INFO TaskSchedulerImpl: Adding task set 0.0 with 2 tasks resource profile 0
# 26/05/02 09:28:30 INFO TaskSetManager: Starting task 0.0 in stage 0.0 (TID 0) (323f14c36de4, executor driver, partition 0, PROCESS_LOCAL, 15683 bytes) 
# 26/05/02 09:28:30 INFO TaskSetManager: Starting task 1.0 in stage 0.0 (TID 1) (323f14c36de4, executor driver, partition 1, PROCESS_LOCAL, 14060 bytes) 
# 26/05/02 09:28:30 INFO Executor: Running task 0.0 in stage 0.0 (TID 0)
# 26/05/02 09:28:30 INFO Executor: Running task 1.0 in stage 0.0 (TID 1)
# 26/05/02 09:28:30 INFO CodeGenerator: Code generated in 41.676551 ms
# 26/05/02 09:28:30 INFO VectorizedSparkParquetReaders: Enabling arrow.enable_unsafe_memory_access
# 26/05/02 09:28:30 INFO VectorizedSparkParquetReaders: Disabling arrow.enable_null_check_for_get
# 26/05/02 09:28:30 INFO BaseAllocator: Debug mode disabled. Enable with the VM option -Darrow.memory.debug.allocator=true.
# 26/05/02 09:28:30 INFO DefaultAllocationManagerOption: allocation manager type not specified, using netty as the default type
# 26/05/02 09:28:30 INFO CheckAllocator: Using DefaultAllocationManager at memory/DefaultAllocationManagerFactory.class
# 26/05/02 09:28:31 INFO Executor: Finished task 0.0 in stage 0.0 (TID 0). 4979 bytes result sent to driver
# 26/05/02 09:28:31 INFO Executor: Finished task 1.0 in stage 0.0 (TID 1). 4979 bytes result sent to driver
# 26/05/02 09:28:31 INFO TaskSetManager: Finished task 0.0 in stage 0.0 (TID 0) in 1001 ms on 323f14c36de4 (executor driver) (1/2)
# 26/05/02 09:28:31 INFO TaskSetManager: Finished task 1.0 in stage 0.0 (TID 1) in 959 ms on 323f14c36de4 (executor driver) (2/2)
# 26/05/02 09:28:31 INFO DAGScheduler: ShuffleMapStage 0 (showString at NativeMethodAccessorImpl.java:0) finished in 1.179 s
# 26/05/02 09:28:31 INFO DAGScheduler: looking for newly runnable stages
# 26/05/02 09:28:31 INFO DAGScheduler: running: Set()
# 26/05/02 09:28:31 INFO DAGScheduler: waiting: Set()
# 26/05/02 09:28:31 INFO DAGScheduler: failed: Set()
# 26/05/02 09:28:31 INFO TaskSchedulerImpl: Removed TaskSet 0.0, whose tasks have all completed, from pool 
# 26/05/02 09:28:31 INFO CodeGenerator: Code generated in 27.178399 ms
# 26/05/02 09:28:31 INFO SparkContext: Starting job: showString at NativeMethodAccessorImpl.java:0
# 26/05/02 09:28:31 INFO DAGScheduler: Got job 1 (showString at NativeMethodAccessorImpl.java:0) with 1 output partitions
# 26/05/02 09:28:31 INFO DAGScheduler: Final stage: ResultStage 2 (showString at NativeMethodAccessorImpl.java:0)
# 26/05/02 09:28:31 INFO DAGScheduler: Parents of final stage: List(ShuffleMapStage 1)
# 26/05/02 09:28:31 INFO DAGScheduler: Missing parents: List()
# 26/05/02 09:28:31 INFO DAGScheduler: Submitting ResultStage 2 (MapPartitionsRDD[6] at showString at NativeMethodAccessorImpl.java:0), which has no missing parents
# 26/05/02 09:28:31 INFO MemoryStore: Block broadcast_3 stored as values in memory (estimated size 14.1 KiB, free 434.3 MiB)
# 26/05/02 09:28:31 INFO MemoryStore: Block broadcast_3_piece0 stored as bytes in memory (estimated size 6.6 KiB, free 434.3 MiB)
# 26/05/02 09:28:31 INFO BlockManagerInfo: Added broadcast_3_piece0 in memory on 323f14c36de4:34391 (size: 6.6 KiB, free: 434.4 MiB)
# 26/05/02 09:28:31 INFO SparkContext: Created broadcast 3 from broadcast at DAGScheduler.scala:1585
# 26/05/02 09:28:31 INFO DAGScheduler: Submitting 1 missing tasks from ResultStage 2 (MapPartitionsRDD[6] at showString at NativeMethodAccessorImpl.java:0) (first 15 tasks are for partitions Vector(0))
# 26/05/02 09:28:31 INFO TaskSchedulerImpl: Adding task set 2.0 with 1 tasks resource profile 0
# 26/05/02 09:28:31 INFO TaskSetManager: Starting task 0.0 in stage 2.0 (TID 2) (323f14c36de4, executor driver, partition 0, NODE_LOCAL, 8999 bytes) 
# 26/05/02 09:28:31 INFO Executor: Running task 0.0 in stage 2.0 (TID 2)
# 26/05/02 09:28:31 INFO ShuffleBlockFetcherIterator: Getting 2 (132.0 B) non-empty blocks including 2 (132.0 B) local and 0 (0.0 B) host-local and 0 (0.0 B) push-merged-local and 0 (0.0 B) remote blocks
# 26/05/02 09:28:31 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 39 ms
# 26/05/02 09:28:31 INFO BlockManagerInfo: Removed broadcast_2_piece0 on 323f14c36de4:34391 in memory (size: 8.5 KiB, free: 434.4 MiB)
# 26/05/02 09:28:31 INFO CodeGenerator: Code generated in 45.109306 ms
# 26/05/02 09:28:31 INFO Executor: Finished task 0.0 in stage 2.0 (TID 2). 4106 bytes result sent to driver
# 26/05/02 09:28:31 INFO TaskSetManager: Finished task 0.0 in stage 2.0 (TID 2) in 164 ms on 323f14c36de4 (executor driver) (1/1)
# 26/05/02 09:28:31 INFO DAGScheduler: ResultStage 2 (showString at NativeMethodAccessorImpl.java:0) finished in 0.184 s
# 26/05/02 09:28:31 INFO DAGScheduler: Job 1 is finished. Cancelling potential speculative or zombie tasks for this job
# 26/05/02 09:28:31 INFO TaskSchedulerImpl: Removed TaskSet 2.0, whose tasks have all completed, from pool 
# 26/05/02 09:28:31 INFO TaskSchedulerImpl: Killing all running tasks in stage 2: Stage finished
# 26/05/02 09:28:31 INFO DAGScheduler: Job 1 finished: showString at NativeMethodAccessorImpl.java:0, took 0.210158 s
# 26/05/02 09:28:32 INFO BlockManagerInfo: Removed broadcast_3_piece0 on 323f14c36de4:34391 in memory (size: 6.6 KiB, free: 434.4 MiB)
# 26/05/02 09:28:32 INFO CodeGenerator: Code generated in 10.09766 ms
# +-----------------+
# |         avg(age)|
# +-----------------+
# |55.56763698630137|
# +-----------------+

# ==============================
# 26/05/02 09:28:32 INFO SparkContext: Invoking stop() from shutdown hook
# 26/05/02 09:28:32 INFO SparkContext: SparkContext is stopping with exitCode 0.
# 26/05/02 09:28:32 INFO SparkUI: Stopped Spark web UI at http://323f14c36de4:4041
# 26/05/02 09:28:32 INFO MapOutputTrackerMasterEndpoint: MapOutputTrackerMasterEndpoint stopped!
# 26/05/02 09:28:32 INFO MemoryStore: MemoryStore cleared
# 26/05/02 09:28:32 INFO BlockManager: BlockManager stopped
# 26/05/02 09:28:32 INFO BlockManagerMaster: BlockManagerMaster stopped
# 26/05/02 09:28:32 INFO OutputCommitCoordinator$OutputCommitCoordinatorEndpoint: OutputCommitCoordinator stopped!
# 26/05/02 09:28:32 INFO SparkContext: Successfully stopped SparkContext
# 26/05/02 09:28:32 INFO ShutdownHookManager: Shutdown hook called
# 26/05/02 09:28:32 INFO ShutdownHookManager: Deleting directory /tmp/spark-852e92f6-6f6d-4dd1-a597-e7dc4db12284
# 26/05/02 09:28:32 INFO ShutdownHookManager: Deleting directory /tmp/spark-0e053da0-9ea4-488a-a2ed-e042fd83a7da/pyspark-0ae3e15e-dc73-4468-9a04-a8f915783fab
# 26/05/02 09:28:32 INFO ShutdownHookManager: Deleting directory /tmp/spark-0e053da0-9ea4-488a-a2ed-e042fd83a7da
