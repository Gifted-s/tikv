
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



## Workload A

| Operation | Count      | Min (ms) | Max (ms) | Avg (ms) | 50th (ms) | 90th (ms) | 95th (ms) | 99th (ms) | 99.9th (ms) | 99.99th (ms) |
|------------|------------|----------|----------|----------|-----------|-----------|-----------|-----------|-------------|--------------|
| **READ**   | 4,997,544  | 0.058    | 83.839   | 0.626    | 0.569     | 0.938     | 1.103     | 1.641     | 3.413       | 14.127       |
| **UPDATE** | 5,002,456  | 0.181    | 195.071  | 1.528    | 1.395     | 2.043     | 2.369     | 3.979     | 11.831      | 29.311       |
| **TOTAL**  | 10,000,000 | 0.058    | 195.071  | 1.077    | 0.977     | 1.782     | 2.061     | 3.049     | 9.831       | 23.151       |


## Observation
Significant spike in the tail latency for update


## Workload B 

| Operation | Count     | Min (ms) | Max (ms) | Avg (ms) | 50th (ms) | 90th (ms) | 95th (ms) | 99th (ms) | 99.9th (ms) | 99.99th (ms) |
|------------|-----------|----------|----------|-----------|------------|------------|------------|------------|--------------|---------------|
| READ       | 9,501,026 | 0.042    | 21.743   | 0.395     | 0.375      | 0.522      | 0.594      | 0.931      | 1.908        | 2.375         |
| UPDATE     | 498,974   | 0.168    | 26.655   | 1.059     | 1.014      | 1.284      | 1.425      | 2.413      | 5.539        | 8.719         |
| TOTAL      | 10,000,000| 0.042    | 26.655   | 0.428     | 0.381      | 0.587      | 0.879      | 1.279      | 2.167        | 4.447         |

## Workload E

| Operation | Count      | Min (ms) | Max (ms) | Avg (ms) | 50th (ms) | 90th (ms) | 95th (ms) | 99th (ms) | 99.9th (ms) | 99.99th (ms) |
|------------|------------|----------|----------|----------|-----------|-----------|-----------|-----------|-------------|--------------|
| **INSERT** | 500,197    | 0.131    | 18.911   | 0.748    | 0.681     | 1.052     | 1.229     | 1.814     | 5.987       | 8.767        |
| **SCAN**   | 9,499,803  | 0.043    | 77.631   | 0.487    | 0.435     | 0.748     | 0.887     | 1.213     | 1.683       | 2.233        |
| **TOTAL**  | 10,000,000 | 0.043    | 77.631   | 0.500    | 0.443     | 0.775     | 0.916     | 1.254     | 1.818       | 4.699        |



## Benchmarking per client  
Goal: What is the max throughput/client and avarage latency for these requests
Note: This experiment was ran on a personal machine not an isolated environment (To fix later).
Different batch sizes was tested

Runs: 3

Config 
### ⚙️ Go-YCSB TiKV Load Configuration

| Parameter | Value |
|------------|--------|
| Command | `./bin/go-ycsb load tikv -P workloads/workload_insert` |
| `tikv.pd` | `127.0.0.1:2379` |
| `threadcount` | `12` |
| `batchsize` | `3500000` |
| `tikv.batchsize` | `3500000` |
| `tikv.conncount` | `1` |
| `recordcount` | `3500000` |
| `operationcount` | `1` |
| `workload` | `core` |
| `readallfields` | `true` |
| `readproportion` | `0` |
| `updateproportion` | `0` |
| `scanproportion` | `0` |
| `insertproportion` | `1` |
| `requestdistribution` | `uniform` |



### 🧮 TiKV Go-YCSB Insert Benchmark (3.5M records, 12 threads, 1 Client)

| Run | Duration (s) | OPS | Avg (µs) | Min (µs) | Max (µs) | 50th (µs) | 90th (µs) | 95th (µs) | 99th (µs) | 99.9th (µs) | 99.99th (µs) |
|-----|---------------|------|-----------|-----------|-----------|-------------|-------------|-------------|-------------|---------------|----------------|
| 1 | 179.9 | 19,454.7 | 610 | 101 | 104,319 | 382 | 1,639 | 1,811 | 2,341 | 8,759 | 33,119 |
| 2 | 178.9 | 19,569.4 | 607 | 101 | 190,591 | 384 | 1,631 | 1,802 | 2,309 | 8,327 | 29,983 |
| 3 | 178.8 | 19,580.2 | 606 | 99 | 105,855 | 384 | 1,624 | 1,795 | 2,307 | 8,415 | 27,551 |
| **Average** | **179.2** | **19,534.8** | **607.7** | **100.3** | **133,588.3** | **383.3** | **1,631.3** | **1,802.7** | **2,319.0** | **8,500.3** | **30,218.0** |
=======