# REAL-TIME FRAUD MONITORING SYSTEM
## VM2 — Day 1 Setup and Technical Report

**Team Member:** Anirudh M  
**Role:** Kafka Consumer, Spark Master, Spark Driver and Fraud Detection Logic  
**Operating System:** Ubuntu 24.04.5 LTS  
**Date:** 7 November 2026

---

# 1. Introduction

The objective of Day 1 was to prepare VM2 as the central Spark processing machine for the Real-Time Fraud Monitoring System.

VM2 is responsible for:
- Running the Apache Spark Master.
- Hosting the Spark application and driver.
- Consuming transaction events from Kafka.
- Executing the fraud detection logic.
- Coordinating distributed processing with Spark workers on other VMs.

Before connecting to the other team members' VMs, the objective was to verify that the software environment on VM2 was correctly installed and configured.

# 2. Project Architecture and VM2's Role

The project uses a distributed architecture involving four virtual machines.

| VM | Team member | Responsibility |
|---|---|---|
| VM1 | Abdul | Kafka producer and broker |
| VM2 | Anirudh | Kafka consumer, Spark Master, Spark Driver and fraud detection logic |
| VM3 | Chiranthan | Spark Worker, HDFS, Iceberg and verification |
| VM4 | Prajwal | Spark Worker and Streamlit dashboard |

### How the architecture works

1. VM1 generates transaction events and publishes them to Kafka.
2. VM2 consumes the transaction events.
3. VM2's Spark application processes the incoming transactions.
4. The Spark Master coordinates resource allocation across available workers.
5. VM3 and VM4 provide distributed computing resources.
6. Processed transaction information and fraud results are stored using the project's storage components.
7. VM4 provides the dashboard for visualization.

**Important distinction:** The Spark Master coordinates the cluster. It does not itself perform all the distributed data processing. The Spark Driver coordinates the application, while executors perform the actual computation.

# 3. Software Environment Verification

The following software was already installed on VM2 and was verified during Day 1.

| Component | Installed version / location | Purpose |
|---|---|---|
| Ubuntu | 24.04.5 LTS | Operating system |
| Java | OpenJDK 11.0.32.1 | Runtime for Spark and Hadoop |
| Python | 3.10.22 | Python interpreter for Spark |
| Default Python | 3.12.3 | System Python |
| Apache Hadoop | 3.3.6 | Distributed storage infrastructure |
| Apache Spark | 3.3.4 | Distributed data processing |
| Apache Kafka | 3.9.2 | Real-time transaction messaging |
| Scala | 2.12.15 | JVM language used by Spark |

### 3.1 Why Java is required

Apache Spark is primarily implemented in Scala and runs on the Java Virtual Machine (JVM).

Java provides the runtime environment required to execute Spark's JVM components.

Our configuration uses:

```bash
JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
```

We verified Java using:

```bash
java -version
```

### 3.2 Why Python 3.10 is used

Our application uses PySpark, which allows us to write Spark applications using Python.

We configured Python 3.10 as the interpreter for both the Spark Driver and Python workers.

```bash
PYSPARK_PYTHON=/usr/bin/python3.10
PYSPARK_DRIVER_PYTHON=/usr/bin/python3.10
```

Python 3.10 was selected because it was available and successfully tested with our installed Spark version.

### 3.3 Hadoop configuration

Hadoop is installed at:

```bash
/opt/hadoop
```

We configured:

```bash
HADOOP_HOME=/opt/hadoop
HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
```

Spark uses Hadoop libraries and configuration when interacting with Hadoop-compatible storage systems.

---

# 4. Understanding Apache Spark and PySpark

This is one of the most important sections for understanding our setup.

## 4.1 What is Apache Spark?

Apache Spark is an open-source distributed computing framework used to process large amounts of data efficiently.

It allows computations to be divided into smaller tasks and executed across multiple machines.

For example, suppose we receive one million banking transactions.

Processing all transactions on a single machine may take considerable time.

Spark can divide the workload into smaller partitions and process them in parallel using multiple CPU cores and machines.

### Why are we using Spark?

Our project is a real-time fraud monitoring system.

Transactions continuously arrive from Kafka. We need to:
- Process incoming transactions.
- Identify suspicious patterns.
- Calculate transaction statistics.
- Apply fraud detection rules.
- Perform time-based aggregations.
- Scale processing across multiple machines.

Spark Structured Streaming provides the streaming processing capabilities needed for this architecture.

## 4.2 What is PySpark?

PySpark is the Python API for Apache Spark.

Apache Spark provides APIs in multiple languages, including Scala, Java, Python and R.

Instead of writing the application in Scala or Java, we can use Python to interact with Spark.

Example:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("FraudMonitoring") \
    .getOrCreate()
```

Here, we create a SparkSession, which acts as the entry point for working with Spark DataFrames and SQL.

**Difference:**

- Apache Spark: The distributed data processing framework.
- PySpark: The Python interface used to develop Spark applications.

PySpark does not mean Spark itself is written in Python. Its core execution engine runs on the JVM.

## 4.3 What is SparkSession?

SparkSession is the main entry point for interacting with Spark applications.

It allows us to:
- Create DataFrames.
- Execute SQL queries.
- Read and write data.
- Configure Spark applications.
- Work with Structured Streaming.

Example:

```python
spark = SparkSession.builder \
    .appName("FraudMonitoring") \
    .getOrCreate()
```

## 4.4 What is a DataFrame?

A DataFrame is a distributed collection of data organized into named columns.

It is conceptually similar to a relational database table.

For example:

| transaction_id | customer_id | amount |
|---|---|---:|
| T001 | C101 | 5000 |
| T002 | C102 | 75000 |
| T003 | C103 | 20000 |

In PySpark:

```python
data = [
    ("T001", "C101", 5000),
    ("T002", "C102", 75000),
    ("T003", "C103", 20000)
]

df = spark.createDataFrame(
    data,
    ["transaction_id", "customer_id", "amount"]
)

df.show()
```

Spark can distribute DataFrame partitions across different executors for parallel processing.

---

# 5. Understanding Spark Cluster Architecture

## 5.1 What is a Spark Cluster?

A Spark cluster is a collection of machines that work together to execute Spark applications.

Instead of depending on one computer, Spark can distribute computation across several machines.

Our project uses Spark's Standalone Cluster Manager.

## 5.2 What is the Spark Master?

The Spark Master is the central coordinator of a Spark Standalone cluster.

Its responsibilities include:
- Registering Spark workers.
- Tracking available resources.
- Managing worker information.
- Coordinating application resource allocation.
- Helping schedule applications across the cluster.

In our architecture, the Spark Master runs on VM2.

We started it using:

```bash
start-master.sh
```

The Master listens at:

```text
spark://192.168.1.7:7077
```

The Master Web UI is available at:

```text
http://192.168.1.7:8080
```

## 5.3 What is a Spark Worker?

A Spark Worker is a process that runs on a machine and provides resources to the Spark cluster.

It provides:
- CPU cores.
- Memory.
- Execution capacity for Spark applications.

When a worker starts, it registers itself with the Spark Master.

In our architecture:
- VM3 will run a Spark Worker.
- VM4 will run a Spark Worker.

The Master will then be able to see the available worker resources.

**Note:** We have not started any workers yet. This is intentional because Day 1 focused on independently preparing VM2.

## 5.4 What is the Spark Driver?

The Spark Driver is the process responsible for coordinating an individual Spark application.

Its responsibilities include:
- Creating the SparkSession and SparkContext.
- Converting application operations into execution plans.
- Dividing work into jobs, stages and tasks.
- Scheduling tasks on executors.
- Tracking application execution.

In our project, the Spark application and its driver will run from VM2.

## 5.5 What is a Spark Executor?

An Executor is a process responsible for executing tasks assigned by the Spark Driver.

Executors:
- Execute tasks.
- Process DataFrame partitions.
- Store intermediate computation results.
- Participate in distributed operations.

Workers provide the resources on which executors run.

A useful distinction:

| Component | Responsibility |
|---|---|
| Master | Coordinates the Spark cluster |
| Worker | Provides machine resources |
| Driver | Coordinates an application |
| Executor | Executes application tasks |

## 5.6 What are Jobs, Stages and Tasks?

Spark divides application execution into smaller units.

**Job:** Created when an action such as `count()` or `collect()` triggers execution.

**Stage:** A job is divided into stages based on dependencies between operations.

**Task:** The smallest unit of execution, generally operating on one partition.

Example:

Suppose we have 100 partitions.

Spark may create multiple tasks to process those partitions in parallel.

The Driver schedules those tasks on available executors.

---

# 6. PySpark Local Execution Test

Before starting the Spark Master, we tested whether PySpark could successfully execute an application.

### 6.1 Configuring the PySpark Python path

Spark already contained the PySpark Python libraries.

We configured:

```bash
export PYTHONPATH=/opt/spark/python/lib/pyspark.zip:/opt/spark/python/lib/py4j-0.10.9.5-src.zip:$PYTHONPATH
```

We verified the installation:

```bash
python3.10 -c "import pyspark; print(pyspark.__version__)"
```

Output:

```text
3.3.4
```

### 6.2 Creating a test application

We created:

```text
~/fraud-monitoring/test_spark.py
```

The application:
1. Created a SparkSession.
2. Created a sample DataFrame.
3. Displayed the records.
4. Counted the transactions.
5. Calculated the total transaction amount.

The test dataset contained three transactions.

### 6.3 Running the application

Command:

```bash
spark-submit --master 'local[2]' \
    --conf spark.pyspark.python=/usr/bin/python3.10 \
    test_spark.py
```

### 6.4 What does local[2] mean?

The `local[2]` setting instructs Spark to run locally using two execution threads.

It does not connect to a distributed Spark cluster.

We used this mode to verify that Spark could execute a PySpark application independently before configuring cluster execution.

### 6.5 Test results

| Operation | Result |
|---|---:|
| Number of transactions | 3 |
| Total transaction amount | 150000 |
| Spark version | 3.3.4 |
| Execution status | Successful |

The application printed:

```text
SPARK TEST SUCCESSFUL
```

This confirmed that our local PySpark execution environment was working.

---

# 7. Spark Environment Configuration

We backed up the original Spark configuration files before making changes.

```bash
cp /opt/spark/conf/spark-env.sh \
   /opt/spark/conf/spark-env.sh.backup

cp /opt/spark/conf/spark-defaults.conf \
   /opt/spark/conf/spark-defaults.conf.backup
```

This allows us to restore the original configuration if required.

## 7.1 spark-env.sh

File:

```text
/opt/spark/conf/spark-env.sh
```

This file contains environment variables used when launching Spark processes.

Our project-specific configuration:

```bash
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

export HADOOP_HOME=/opt/hadoop
export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop

export SPARK_HOME=/opt/spark

# Network configuration
export SPARK_LOCAL_IP=192.168.1.7
export SPARK_MASTER_HOST=192.168.1.7

# Master ports
export SPARK_MASTER_PORT=7077
export SPARK_MASTER_WEBUI_PORT=8080

# Local worker resources
export SPARK_WORKER_CORES=4
export SPARK_WORKER_MEMORY=6g

# Python configuration
export PYSPARK_PYTHON=/usr/bin/python3.10
export PYSPARK_DRIVER_PYTHON=/usr/bin/python3.10
```

### Explanation of important variables

| Variable | Purpose |
|---|---|
| JAVA_HOME | Identifies Java installation |
| HADOOP_HOME | Identifies Hadoop installation |
| HADOOP_CONF_DIR | Location of Hadoop configuration |
| SPARK_HOME | Location of Spark installation |
| SPARK_LOCAL_IP | IP address used by Spark on this machine |
| SPARK_MASTER_HOST | IP address on which Spark Master binds |
| SPARK_MASTER_PORT | Spark Master communication port |
| SPARK_MASTER_WEBUI_PORT | Master Web UI port |
| SPARK_WORKER_CORES | CPU cores allocated to a local worker |
| SPARK_WORKER_MEMORY | Memory allocated to a local worker |
| PYSPARK_PYTHON | Python interpreter for Spark workers |
| PYSPARK_DRIVER_PYTHON | Python interpreter for the driver |

The worker resource settings are configured for a possible local worker, but we have not started one.

## 7.2 spark-defaults.conf

File:

```text
/opt/spark/conf/spark-defaults.conf
```

This file contains default Spark application properties.

Our configuration:

```properties
spark.master                     spark://192.168.1.7:7077

spark.driver.host                192.168.1.7
spark.driver.bindAddress         0.0.0.0
spark.driver.port                7078
spark.blockManager.port          7079

spark.driver.memory              2g
spark.executor.memory            2g

spark.eventLog.enabled           false
spark.pyspark.python             /usr/bin/python3.10
spark.pyspark.driver.python      /usr/bin/python3.10
```

### Explanation

**spark.master**

Identifies the Spark Master to which an application should connect.

**spark.driver.host**

Specifies the address advertised by the driver to other Spark processes.

**spark.driver.bindAddress**

Specifies the local network address on which the driver listens. `0.0.0.0` allows binding to available network interfaces.

**spark.driver.port**

Sets the driver's communication port.

**spark.blockManager.port**

Sets the Block Manager communication port.

**spark.driver.memory**

Allocates 2 GB of memory to the driver JVM.

**spark.executor.memory**

Allocates 2 GB of memory per executor.

**Important:** These memory settings are Spark application defaults. They do not mean the Master daemon itself uses 2 GB.

---

# 8. Starting the Spark Master

After configuring Spark, we started the Master.

Command:

```bash
start-master.sh
```

Output:

```text
starting org.apache.spark.deploy.master.Master
```

We then checked running Java processes:

```bash
jps
```

Output:

```text
7088 Master
7214 Jps
```

This confirmed that the Spark Master process was running.

### Understanding jps

`jps` stands for Java Virtual Machine Process Status Tool.

It lists Java processes running on the machine.

Here:
- `7088 Master`: Spark Master Java process.
- `7214 Jps`: The Java process listing utility itself.

### Master startup logs

We inspected the logs:

```bash
cat /opt/spark/logs/*out | tail -30
```

Important output:

```text
Successfully started service 'sparkMaster' on port 7077.

Starting Spark master at spark://192.168.1.7:7077

Running Spark version 3.3.4

Successfully started service 'MasterUI' on port 8080.

MasterWebUI: Bound MasterWebUI to 192.168.1.7

I have been elected leader! New state: ALIVE
```

The final message:

```text
New state: ALIVE
```

confirms that the Master successfully started and is operational.

---

# 9. Spark Master Web UI Verification

We opened:

```text
http://192.168.1.7:8080
```

The Spark Master Web UI displayed:

| Parameter | Observed value |
|---|---|
| Spark version | 3.3.4 |
| Master URL | spark://192.168.1.7:7077 |
| Active Workers | 0 |
| Cores in use | 0 |
| Memory in use | 0 |
| Applications | 0 |
| Drivers | 0 |
| Status | ALIVE |

### Why are Workers showing zero?

This is expected.

The Spark Master is running, but VM3 and VM4 have not yet started their Spark Workers and registered with the Master.

The zero-worker state does not indicate an error.

---

# 10. Network and Port Verification

We checked whether Spark was listening on the expected ports.

Command:

```bash
sudo ss -lntp | grep -E ':7077|:8080'
```

Output:

```text
LISTEN [::ffff:192.168.1.7]:7077
LISTEN [::ffff:192.168.1.7]:8080
```

Both ports were successfully listening.

### Important ports

| Port | Purpose |
|---|---|
| 7077 | Spark Master communication |
| 8080 | Spark Master Web UI |
| 7078 | Spark Driver communication |
| 7079 | Spark Block Manager communication |

The ports 7078 and 7079 are configured in the Spark defaults file. We have not yet verified their application-level listeners because no cluster application is running.

---

# 11. Firewall Verification

We checked the firewall using:

```bash
sudo ufw status verbose
```

Output:

```text
Status: active

Default: deny (incoming), allow (outgoing), deny (routed)

22/tcp (OpenSSH) ALLOW IN Anywhere
9092/tcp on zt7nnicp4g ALLOW IN Anywhere
```

### What does this mean?

UFW is the Uncomplicated Firewall used to manage incoming and outgoing network traffic.

Our firewall:
- Allows incoming SSH connections on port 22.
- Allows Kafka port 9092 on the ZeroTier interface `zt7nnicp4g`.
- Blocks other incoming connections by default.
- Allows outgoing connections.

### What remains to be done?

Before connecting VM3 and VM4, we need to configure suitable firewall rules for Spark communication.

The relevant Spark ports are:
- 7077 — Master.
- 7078 — Driver.
- 7079 — Block Manager.
- 8080 — Web UI, if remote UI access is required.

We should restrict access to the team's actual private network/interface and VM IP addresses rather than unnecessarily exposing these ports to everyone.

**Firewall configuration for cross-VM communication is pending.**

---

# 12. Day 1 Completion Summary

| Task | Status |
|---|---|
| Verify Ubuntu environment | Completed |
| Verify Java installation | Completed |
| Verify Python 3.10 | Completed |
| Verify Hadoop installation | Completed |
| Verify Spark installation | Completed |
| Verify Kafka installation | Completed |
| Configure PySpark Python path | Completed |
| Execute local PySpark test | Completed |
| Back up Spark configuration | Completed |
| Configure spark-env.sh | Completed |
| Configure spark-defaults.conf | Completed |
| Start Spark Master | Completed |
| Verify Master process using jps | Completed |
| Verify Master logs | Completed |
| Verify Spark Master Web UI | Completed |
| Verify ports 7077 and 8080 | Completed |
| Inspect firewall | Completed |
| Configure cross-VM firewall access | Pending |
| Connect VM3 and VM4 workers | Not started |
| Test distributed cluster execution | Pending |

### Overall conclusion

The independent VM2 setup for Day 1 was successfully completed.

We verified that the installed Spark environment can execute a PySpark application locally and that the Spark Standalone Master starts successfully.

The Master is accessible locally through its Web UI and is listening on the configured communication ports.

The remaining work involves preparing network access for other VMs and validating distributed execution after the workers are connected.

---

# PART 2 — TECHNICAL VIVA PREPARATION

The following questions cover the concepts, commands and decisions involved in today's setup.

## A. Apache Spark Fundamentals

### Q1. What is Apache Spark?

Apache Spark is an open-source distributed computing framework used for processing large datasets efficiently through parallel and distributed execution.

### Q2. Why are we using Spark in our project?

We receive continuous transaction data from Kafka. Spark allows us to process transactions, perform aggregations and execute fraud detection logic efficiently.

### Q3. What is PySpark?

PySpark is the Python API for Apache Spark. It allows developers to write Spark applications using Python instead of Scala or Java.

### Q4. Is Spark written in Python?

No. Spark's core engine is implemented primarily in Scala and runs on the JVM. PySpark provides a Python interface to that engine.

### Q5. What is the difference between Spark and Hadoop?

Hadoop is an ecosystem for distributed storage and processing. Its traditional MapReduce framework processes data using a batch-oriented model.

Spark is a distributed processing engine that supports batch processing, SQL, streaming, machine learning and graph processing.

Spark can also use Hadoop-compatible storage such as HDFS.

### Q6. What is Spark Structured Streaming?

It is Spark's streaming processing engine. It allows us to process continuously arriving data using DataFrame and SQL-based operations.

We will use it to process transactions arriving from Kafka.

### Q7. What is a DataFrame?

A DataFrame is a distributed collection of data organized into named columns, similar to a relational database table.

### Q8. What is SparkSession?

SparkSession is the main entry point for creating DataFrames, executing SQL queries and interacting with Spark functionality.

### Q9. What is lazy evaluation in Spark?

Spark does not immediately execute every transformation.

Instead, it builds an execution plan and executes it when an action is triggered.

For example:

```python
df.filter(df.amount > 10000)
```

This transformation does not necessarily execute immediately.

But:

```python
df.count()
```

is an action that triggers execution.

### Q10. What are transformations and actions?

**Transformations:** Operations that create a new DataFrame or RDD, such as `filter()`, `select()` and `map()`.

**Actions:** Operations that trigger execution and return results or produce output, such as `count()`, `show()` and `collect()`.

---

## B. Spark Cluster Architecture

### Q11. What is a Spark cluster?

A Spark cluster is a group of machines working together to execute distributed Spark applications.

### Q12. What is the Spark Master?

The Master coordinates the Spark Standalone cluster by registering workers, tracking resources and helping allocate resources to applications.

### Q13. What is a Spark Worker?

A Worker is a process that provides CPU and memory resources to execute Spark applications.

### Q14. What is a Spark Driver?

The Driver coordinates an individual Spark application. It creates the SparkContext, builds execution plans and schedules tasks.

### Q15. What is an Executor?

An Executor is a process that executes tasks assigned by the Driver.

### Q16. What is the difference between Master and Driver?

The Master coordinates the cluster.

The Driver coordinates an individual application.

They have different responsibilities.

### Q17. Does the Master execute the actual fraud detection logic?

The Master primarily manages cluster resources and application scheduling. The actual application computations are performed by executors.

### Q18. What happens when a Worker starts?

It registers with the Master and advertises its available resources, such as CPU cores and memory.

### Q19. Why does our Master Web UI show zero workers?

Because VM3 and VM4 have not yet started their Spark Worker processes and registered with VM2.

### Q20. What are jobs, stages and tasks?

A job is triggered by an action.

A job is divided into stages based on dependencies.

Stages contain tasks, which execute on individual data partitions.

---

## C. PySpark and Execution

### Q21. What is `spark-submit`?

`spark-submit` is the command-line utility used to submit and execute Spark applications.

### Q22. What does `local[2]` mean?

It instructs Spark to run locally using two execution threads.

### Q23. What is the difference between `local[2]` and `spark://192.168.1.7:7077`?

`local[2]` executes Spark locally using two threads.

`spark://192.168.1.7:7077` connects the application to the Spark Standalone Master.

### Q24. Why did we test local execution before starting the Master?

To independently verify that Spark and PySpark were working before introducing cluster networking and worker dependencies.

### Q25. What is `PYTHONPATH`?

It specifies additional directories or archives that Python searches when importing modules.

We used it to expose Spark's bundled PySpark and Py4J libraries to Python.

### Q26. What is Py4J?

Py4J enables communication between Python and Java applications.

PySpark uses it to interact with Spark's JVM components.

### Q27. Why did we use Python 3.10 instead of Python 3.12?

Python 3.10 was available and successfully tested with our installed Spark 3.3.4 environment. Using a consistent interpreter avoids Python compatibility problems between the Driver and workers.

---

## D. Configuration and Networking

### Q28. What is `spark-env.sh`?

It is a configuration file containing environment variables used when starting Spark processes.

### Q29. What is `spark-defaults.conf`?

It contains default Spark application properties, such as the Master URL, memory settings and driver networking configuration.

### Q30. Why did we back up the configuration files?

To preserve the original configuration and allow recovery if our modifications cause problems.

### Q31. What is `SPARK_LOCAL_IP`?

It specifies the IP address Spark should use for local network binding.

### Q32. Why did we set `SPARK_MASTER_HOST`?

To explicitly configure the IP address on which the Spark Master should listen.

### Q33. Why did we set `spark.driver.host`?

The Driver advertises its address to other Spark processes. Remote executors need to reach this address to communicate with the Driver.

### Q34. Why did we set `spark.driver.bindAddress` to `0.0.0.0`?

It allows the Driver to bind to available network interfaces rather than only one specific interface.

### Q35. What is port 7077?

It is the default communication port of the Spark Standalone Master.

### Q36. What is port 8080?

It is the default Web UI port of the Spark Standalone Master.

### Q37. What are ports 7078 and 7079 used for?

We configured port 7078 for Spark Driver communication and port 7079 for Block Manager communication.

### Q38. What is the Block Manager?

The Block Manager manages storage and retrieval of data blocks, including cached data and intermediate data used during Spark execution.

### Q39. What is UFW?

UFW stands for Uncomplicated Firewall. It provides a simpler interface for configuring Linux firewall rules.

### Q40. Why can the Spark Master work locally even when UFW blocks incoming connections?

The Master is running on the same machine where we access the Web UI. Local access does not require opening the service to remote machines. Other VMs will need appropriate network access.

---

## E. Commands and Troubleshooting

### Q41. What does `jps` do?

It lists Java Virtual Machine processes running on the system.

### Q42. What does `start-master.sh` do?

It starts the Spark Standalone Master daemon.

### Q43. What does `ss -lntp` do?

It displays listening TCP/UDP sockets and associated processes.

### Q44. What does `cat /opt/spark/logs/*out` show?

It displays Spark daemon log output, which helps us identify startup errors and verify whether the Master started successfully.

### Q45. What does `ALIVE` mean in the Master Web UI?

It means the Master has successfully started and is operational.

### Q46. Why do we sometimes see SLF4J multiple-binding warnings?

It means multiple SLF4J logging implementations are present on the classpath. In our setup, Spark and Hadoop both contribute logging libraries.

The warning did not prevent the Master from starting.

### Q47. What does the native Hadoop library warning mean?

Spark could not load the native Hadoop library for the platform and is using built-in Java implementations instead.

This was non-fatal in our tests.

### Q48. Why is the Spark Master process showing PID 7088?

It is the process identifier assigned by Linux to the running Master Java process.

### Q49. Why is our Master URL using `192.168.1.7`?

It is the IP address of the network interface selected for the initial local setup. The team must later confirm that the other VMs can reach this address or select a common private network interface.

### Q50. What happens if the Master stops?

The cluster cannot use that Master for new resource coordination and application registration. Existing application behavior depends on the deployment and failure-recovery configuration.

---

# PART 3 — PROJECT-SPECIFIC VIVA QUESTIONS

### Q51. What is your responsibility in this project?

I am responsible for VM2, which hosts the Spark Master and Spark Driver. I will implement the Kafka consumer and the fraud detection processing logic using PySpark and Structured Streaming.

### Q52. Why are we using Kafka and Spark together?

Kafka acts as the distributed messaging system that receives and stores transaction events.

Spark consumes those events and performs distributed processing and fraud detection.

### Q53. What is the difference between Kafka and Spark?

Kafka is primarily a distributed event streaming and messaging platform.

Spark is a distributed data processing engine.

Kafka transports and retains transaction events; Spark processes them.

### Q54. What is a Kafka Consumer?

A Kafka Consumer reads messages from Kafka topics.

In our project, the consumer will be part of the Spark streaming application.

### Q55. What is a Kafka Topic?

A topic is a named stream of records in Kafka.

We publish and consume related messages through topics.

### Q56. What is a Kafka Partition?

A partition is an ordered, append-only sequence of records within a Kafka topic.

Partitions allow Kafka to distribute data and support parallel consumption.

### Q57. What is a watermark in Spark Structured Streaming?

A watermark specifies how long Spark will wait for late-arriving event-time data before considering older state eligible for cleanup.

Our project plan specifies a one-minute watermark.

### Q58. What is event time?

Event time is the timestamp at which an event actually occurred, rather than the time at which Spark processed or received it.

### Q59. Why is deduplication important in our project?

Duplicate transaction events can cause incorrect fraud counts, transaction totals and aggregations.

Deduplication helps prevent repeated transaction records from being processed as separate transactions.

### Q60. What is a sliding window?

A sliding window performs aggregation over a fixed time duration that advances at regular intervals.

Our project plan specifies two-minute windows sliding every 30 seconds.

### Q61. What is Apache Iceberg?

Apache Iceberg is an open table format designed for large analytical datasets. It provides table metadata management and supports features such as schema evolution and snapshot-based operations.

### Q62. What is HDFS?

HDFS stands for Hadoop Distributed File System. It stores large datasets across multiple machines.

### Q63. Why do we need HDFS and Iceberg?

HDFS provides distributed storage, while Iceberg provides table-level organization and management of analytical data stored on compatible storage systems.

### Q64. What is Streamlit?

Streamlit is a Python framework for building interactive web applications and dashboards.

In our project, VM4 is responsible for the dashboard.

### Q65. Why are multiple VMs used?

Multiple VMs allow us to distribute processing, storage and visualization responsibilities.

This provides parallelism and separates the different project components.

---

# PART 4 — IMPORTANT CONCEPTUAL DISTINCTIONS

These distinctions are especially useful during a viva.

| Common confusion | Correct explanation |
|---|---|
| Spark vs PySpark | Spark is the processing framework; PySpark is its Python API. |
| Master vs Driver | Master coordinates the cluster; Driver coordinates an application. |
| Worker vs Executor | Worker provides resources; Executor executes tasks. |
| Kafka vs Spark | Kafka transports events; Spark processes them. |
| HDFS vs Iceberg | HDFS is a storage system; Iceberg is a table format. |
| Transformation vs Action | Transformations build execution plans; actions trigger execution. |
| Local mode vs Cluster mode | Local mode executes on one machine; cluster mode uses distributed resources. |
| Event time vs Processing time | Event time is when the event occurred; processing time is when the system processed it. |
| Job vs Stage vs Task | A job contains stages; stages contain tasks. |

---

# PART 5 — IMPORTANT ITEMS TO REMEMBER

1. Our installed Spark version is **3.3.4**, not 3.5.x.
2. We successfully executed a PySpark test using Python 3.10.
3. The Spark Master is running at `spark://192.168.1.7:7077`.
4. The Web UI is available at `http://192.168.1.7:8080`.
5. The Master currently shows zero workers because the other VMs have not registered.
6. The local test used `local[2]`; it was not a distributed cluster execution test.
7. The firewall currently blocks incoming connections by default. Cross-VM Spark access still needs to be configured.
8. The Spark Worker resource configuration of four cores and six GB is configured but has not been activated by starting a worker.

### Version compatibility item to resolve

The installed Spark version is 3.3.4. The project plan includes `dropDuplicatesWithinWatermark`, an API introduced in Spark 3.5. Therefore, we must resolve the Spark version with the team before implementing that part of the pipeline.

We should coordinate compatible versions of Spark, Scala, the Kafka connector and Iceberg rather than independently upgrading one component.

---

# FINAL DAY 1 CONCLUSION

Today, I independently prepared VM2 for the Real-Time Fraud Monitoring System.

I verified Java, Python, Hadoop, Kafka and Spark installations, configured the PySpark environment, executed a sample PySpark application and confirmed successful DataFrame operations.

I then configured the Spark Standalone Master, started it successfully, inspected its logs, verified its Web UI and checked the listening ports.

The Master is operational, and VM2 is ready for the next phase of cluster integration, subject to the pending firewall and network configuration.

**Day 1 outcome: Independent VM2 setup successfully completed. Cross-VM integration remains pending.**


---

# PART 6 — NETWORK TRANSITION FOR DAY 2 (ZEROTIER)

## 1. Why the network configuration is changing

During the initial Day 1 setup, Spark Master and Driver networking were configured using the VM2 Wi-Fi address:

```text
192.168.1.7
```

The team has now selected the ZeroTier network `BigData2026_Team4` for communication between the four VMs. VM2's current ZeroTier address on that network is:

```text
192.168.193.39
```

Therefore, before starting distributed cluster integration on Day 2, Spark must advertise and use the ZeroTier address rather than the earlier Wi-Fi address.

**Historical note:** The earlier Day 1 configuration and test results are retained above as a record of what was configured and verified at that time. The values in this section describe the planned Day 2 network transition; they should only be marked complete after the changes have been applied and verified.

## 2. VM2 and VM3 network details

| Machine | Role | ZeroTier IP |
|---|---|---|
| VM2 | Spark Master, Driver and Kafka consumer | `192.168.193.39` |
| VM3 | Spark Worker, HDFS and Iceberg | `192.168.193.59` |
| VM1 | Kafka broker and producer | `192.168.193.163` |
| VM4 | Spark Worker and dashboard | To be confirmed |

The team must use the same authorized ZeroTier network, `BigData2026_Team4`.

## 3. Spark configuration changes required on VM2

The following settings currently refer to the old Wi-Fi address and must be changed before Day 2 cluster integration.

### 3.1 `/opt/spark/conf/spark-env.sh`

Change:

```bash
export SPARK_LOCAL_IP=192.168.1.7
export SPARK_MASTER_HOST=192.168.1.7
```

To:

```bash
export SPARK_LOCAL_IP=192.168.193.39
export SPARK_MASTER_HOST=192.168.193.39
```

Keep the existing port and Python settings unless the team explicitly agrees to change them:

```bash
export SPARK_MASTER_PORT=7077
export SPARK_MASTER_WEBUI_PORT=8080
export PYSPARK_PYTHON=/usr/bin/python3.10
export PYSPARK_DRIVER_PYTHON=/usr/bin/python3.10
```

### 3.2 `/opt/spark/conf/spark-defaults.conf`

Change the Master URL:

```properties
spark.master                     spark://192.168.1.7:7077
```

To:

```properties
spark.master                     spark://192.168.193.39:7077
```

Change the advertised Driver address:

```properties
spark.driver.host                192.168.1.7
```

To:

```properties
spark.driver.host                192.168.193.39
```

Keep the Driver bind address and configured ports:

```properties
spark.driver.bindAddress         0.0.0.0
spark.driver.port                7078
spark.blockManager.port          7079
```

` spark.driver.bindAddress=0.0.0.0` allows the Driver to bind to available interfaces, while `spark.driver.host` is the address advertised to remote Spark processes.

## 4. Restart and verify the Spark Master

After editing the configuration, stop the currently running Master that was started with the old address, then start it again so it loads the ZeroTier configuration.

```bash
stop-master.sh
start-master.sh
```

Verify the process and startup logs:

```bash
jps
grep -hE 'Starting Spark master at|Successfully started service|MasterWebUI|New state' /opt/spark/logs/*out | tail -30
```

The Master should advertise:

```text
spark://192.168.193.39:7077
```

The Master Web UI should be reachable at:

```text
http://192.168.193.39:8080
```

Also verify the listening ports:

```bash
sudo ss -lntp | grep -E ':7077|:8080'
```

Do not consider the network transition complete until the logs and listening sockets confirm the expected ZeroTier address.

## 5. Network connectivity must be verified before starting workers

From VM2, test VM3:

```bash
ping -c 3 192.168.193.59
nc -zv 192.168.193.59 9000
nc -zv 192.168.193.59 9866
```

The expected services are:

| VM3 port | Purpose |
|---|---|
| 9000 | HDFS NameNode RPC |
| 9866 | HDFS DataNode data transfer |

A successful route lookup alone does not prove that VM3 is reachable. The ping and TCP connection tests must be checked. If they fail, troubleshoot ZeroTier membership, peer connectivity and the VM3 firewall before proceeding.

Once VM3 is reachable, Chiranthan can start the VM3 Spark Worker using:

```bash
start-worker.sh spark://192.168.193.39:7077
```

The exact worker startup command may vary depending on the installed Spark scripts and team setup. VM4 must also use the same Master URL after its ZeroTier address and connectivity are confirmed.

## 6. Firewall requirements

VM2's firewall currently uses a default-deny incoming policy. Do not disable UFW.

Before distributed execution, allow the required Spark communication ports on the ZeroTier interface and, preferably, restrict the rules to the known team VM addresses.

| Port | Purpose | Requirement |
|---|---|---|
| 7077 | Spark Master | Required for workers and applications to connect |
| 7078 | Spark Driver | Required for remote executor-to-driver communication when this configured port is used |
| 7079 | Spark Block Manager | Required for Spark block transfer when this configured port is used |
| 8080 | Spark Master Web UI | Optional for remote browser access |

Confirm VM4's ZeroTier IP before creating its firewall rules. Do not expose these ports to all network interfaces or the public internet.

## 7. Other configuration files: what should and should not change

The ZeroTier transition affects Spark's advertised/bind networking settings. It does not mean every occurrence of the old address in the historical report must be rewritten.

| File or setting | Action |
|---|---|
| `/opt/spark/conf/spark-env.sh` | Change `SPARK_LOCAL_IP` and `SPARK_MASTER_HOST` to `192.168.193.39` |
| `/opt/spark/conf/spark-defaults.conf` | Change `spark.master` and `spark.driver.host` to the ZeroTier address |
| VM3 worker startup command | Use `spark://192.168.193.39:7077` |
| VM4 worker startup command | Use `spark://192.168.193.39:7077` after VM4 connectivity is confirmed |
| Kafka bootstrap server in the VM2 application | Use VM1's confirmed ZeroTier broker address and port; expected address supplied by VM1 is `192.168.193.163:9092` |
| VM2 Iceberg configuration | Use the VM3 HDFS warehouse address and the agreed Iceberg catalog settings; this is a storage/catalog setting, not a Spark Master address |
| HDFS NameNode URI | Keep `hdfs://192.168.193.59:9000` |
| Historical Day 1 logs and screenshots | Retain as historical evidence of the initial setup |

For the VM2 Iceberg configuration, the team has supplied these catalog properties:

```properties
spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
spark.sql.catalog.iceberg_catalog=org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.iceberg_catalog.type=hadoop
spark.sql.catalog.iceberg_catalog.warehouse=hdfs://192.168.193.59:9000/warehouse
spark.sql.session.timeZone=UTC
```

The matching Iceberg runtime JAR must also be supplied to Spark. VM3 reported using:

```text
iceberg-spark-runtime-3.3_2.12-1.8.1.jar
```

Confirm the JAR's actual location on VM2 before using it in `spark-submit`.

## 8. Day 2 readiness checklist

Do not mark these items complete until each has been tested.

- [ ] VM2 is connected to `BigData2026_Team4` with IP `192.168.193.39`.
- [ ] VM3 is connected to the same network with IP `192.168.193.59`.
- [ ] VM2 can reach VM3 on HDFS ports 9000 and 9866.
- [ ] `SPARK_LOCAL_IP` is set to `192.168.193.39`.
- [ ] `SPARK_MASTER_HOST` is set to `192.168.193.39`.
- [ ] `spark.master` is set to `spark://192.168.193.39:7077`.
- [ ] `spark.driver.host` is set to `192.168.193.39`.
- [ ] Spark Master has been restarted and its logs show the ZeroTier address.
- [ ] Required Spark firewall rules are configured narrowly for team VM addresses.
- [ ] VM3 and VM4 workers can register with the Master.
- [ ] VM2 can reach VM1's Kafka broker on its confirmed ZeroTier address.
- [ ] VM2 has the compatible Iceberg runtime JAR and catalog configuration.
- [ ] A VM2-to-VM3 Iceberg streaming write test succeeds with its checkpoint on HDFS.

**Status at the time this report was updated:** The ZeroTier IPs and VM3 service configuration have been shared by the team, but VM2-to-VM3 connectivity has not yet been demonstrated. The Spark configuration transition, Master restart, worker registration and VM2-to-VM3 streaming write test remain pending verification.
