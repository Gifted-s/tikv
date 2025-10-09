
SETUP (Benchmark on a single node)

| Parameter          | Value                        |
|--------------------|------------------------------|
| Command            | ./bin/go-ycsb run tikv        |
| Workload file      | workloads/workloada           |
| PD address         | 127.0.0.1:2379                |
| TiKV type          | raw                           |
| Record count       | 10,000,000                    |
| Operation count    | 10,000,000                    |
| Thread count       | 12                            |




| Operation | Count      | Min (µs / ms)     | Max (µs / ms)      | Avg (µs / ms)     | 50th (µs / ms)     | 90th (µs / ms)     | 95th (µs / ms)     | 99th (µs / ms)     | 99.9th (µs / ms)     | 99.99th (µs / ms)     |
|------------|------------|------------------|--------------------|-------------------|--------------------|--------------------|--------------------|--------------------|----------------------|-----------------------|
| **READ**   | 4,997,544  | 58 / 0.058       | 83,839 / 83.839    | 626 / 0.626       | 569 / 0.569        | 938 / 0.938        | 1,103 / 1.103      | 1,641 / 1.641      | 3,413 / 3.413        | 14,127 / 14.127       |
| **UPDATE** | 5,002,456  | 181 / 0.181      | 195,071 / 195.071  | 1,528 / 1.528     | 1,395 / 1.395      | 2,043 / 2.043      | 2,369 / 2.369      | 3,979 / 3.979      | 11,831 / 11.831      | 29,311 / 29.311       |
| **TOTAL**  | 10,000,000 | 58 / 0.058       | 195,071 / 195.071  | 1,077 / 1.077     | 977 / 0.977        | 1,782 / 1.782      | 2,061 / 2.061      | 3,049 / 3.049      | 9,831 / 9.831        | 23,151 / 23.151       |

## Observation
Significant spike in the tail latency for update
## Why Write Latency Can Spike in TiKV During Region Splits

Several factors related to TiKV's **Region splits** can cause dramatic spikes in write latency:

---
### 1. Region Splits

TiKV stores data in **regions**, which are chunks of data (about **96 MB** by default).

* **The Process:** When a region exceeds its size limit, TiKV splits it into two. This involves:
    * Writing new **region metadata**.
    * Updating the **Placement Driver (PD)** service.
    * Potentially moving some **in-memory state**.
* **Latency Spike:** Writes to the splitting region can **stall** (or be delayed) until the split operation is complete, leading to a huge spike in write latency.
* **Single-Node Impact:** **Single-node setups** are particularly vulnerable. Since all splits happen locally without parallel distribution, the **tail latency** (the latency experienced by the slowest writes) can jump dramatically.

---
### 2. Raft Log Flush

Every write in TiKV, even on a single-node setup, goes through **Raft consensus** for correctness.

* **Latency Impact:** Large writes or a burst of writes happening **during a split** can delay the flushing of the **Raft log** to disk, which directly increases write latency.

---
### 3. Storage IO or WAL Flush

TiKV persists writes to the **Write-Ahead Log (WAL)** to ensure durability.

* **Latency Impact:** If the disk is busy (due to other operations) or a required **`fsync`** (forced synchronization to disk) operation happens concurrently with a **region split**, the write latency can spike.

---
### 4. Snapshot Creation

* **Latency Impact:** **Region splits** can sometimes trigger the creation of **region snapshots**. Snapshot creation can briefly **block writes** to the region, contributing to temporary latency spikes.