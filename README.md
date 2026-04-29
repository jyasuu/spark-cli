# spark-cli

## Spark Control Plane CLI: Minimum Viable Specification

This specification outlines a Rust-based CLI designed to act as an orchestration and management layer for Apache Spark. It focuses on high-performance interaction with Spark clusters via existing APIs (REST, K8s, or Thrift) rather than reimplementing the Spark engine.

---

### 1. Core Objectives
* **Portability:** A single, zero-dependency binary for cross-platform use.
* **Efficiency:** High-speed interaction with cluster APIs (Livy, K8s Operator, Yarn).
* **Standardization:** Unified command structure for diverse environments (Local, Cloud, On-prem).

---

### 2. Functional Modules

#### **I. Job Lifecycle Management**
* **Submit:** Package and dispatch JAR/Python/R scripts to a target master.
* **Track:** Real-time status retrieval (Pending, Running, Succeeded, Failed).
* **Logs:** Stream Driver and Executor logs directly to the terminal.
* **Terminate:** Kill active applications by ID or Name.

#### **II. SQL & Metadata Operations**
* **Query:** Execute ad-hoc SQL commands against a Spark Thrift Server or Livy session.
* **Inspect:** Describe table schemas, list partitions, and browse databases.
* **Export:** Stream query results to local formats (CSV, JSON, Parquet).

#### **III. Context & Profile Management**
* **Environments:** Switch seamlessly between `dev`, `staging`, and `prod` configurations.
* **Auth:** Handle credentials (AWS IAM, Kerberos, OAuth) securely at the CLI level.
* **Alias:** Define shortcuts for complex `spark-submit` configurations.

#### **IV. Lakehouse & Storage Utilities**
* **File Ops:** Basic filesystem interactions (ls, cp, rm) for S3, HDFS, or ADLS.
* **Table Management:** Support for Lakehouse-specific tasks (e.g., Iceberg snapshot inspection or Delta Lake vacuuming).

#### **V. Diagnostics & Observability**
* **UI Proxy:** Quickly open the Spark Web UI via local port-forwarding or URL generation.
* **Health Check:** Summary of resource usage (CPU/Memory) and skew detection for active stages.

---

### 3. Technical Requirements (Rust Backend)
* **Asynchronous I/O:** Use non-blocking operations for simultaneous log streaming and status monitoring.
* **Strict Type Safety:** Ensure configuration and API responses are validated at runtime to prevent silent failures.
* **Low Overhead:** Maintain a minimal memory footprint to allow running alongside heavy data workloads.

---

### 4. Out of Scope
* **Compute Engine:** The CLI will not execute DAGs or process data partitions locally.
* **SQL Parsing:** All SQL syntax will be passed directly to the Spark engine for evaluation.
* **Resource Scheduling:** The tool will interface with, but not replace, YARN, Mesos, or K8s Schedulers.

---

### 5. Implementation Roadmap
1.  **Phase 1:** Environment/Profile management and basic Job Submission (API-based).
2.  **Phase 2:** SQL execution and metadata inspection.
3.  **Phase 3:** Advanced diagnostics (log parsing and skew detection).
