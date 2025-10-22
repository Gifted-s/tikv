# TiKV Write Request Flow: From Client to RocksDB Persistence

## Table of Contents
1. [Overview](#overview)
2. [Architecture Overview](#architecture-overview)
3. [Detailed Write Flow](#detailed-write-flow)
4. [Thread Pool Analysis](#thread-pool-analysis)
5. [FSM State Transitions](#fsm-state-transitions)
6. [RocksDB Persistence Details](#rocksdb-persistence-details)
7. [Performance Considerations](#performance-considerations)
8. [Error Handling](#error-handling)
9. [Code References](#code-references)
10. [Summary](#summary)

## Overview

This document provides a comprehensive analysis of how write requests flow through TiKV's RaftStore system, from the initial client request to final persistence in RocksDB. The analysis covers all thread pools, Finite State Machines (FSMs), and the intricate coordination required to maintain both high performance and strong consistency guarantees.

### Key Design Principles
- **Sequential processing per region**: Maintains Raft consensus ordering
- **Parallel processing across regions**: Enables high throughput
- **Batch processing**: Reduces overhead and improves efficiency
- **Two-phase commit**: Raft log first, then state machine application
- **Dedicated worker threads**: Specialized handling for different operations

## Architecture Overview

### Thread Pool Hierarchy

```
┌─────────────────────────────────────────────────────────────┐
│                    TiKV Server Threads                      │
├─────────────────────────────────────────────────────────────┤
│  Main Server Threads                                        │
│  ├── Client Request Handler                                 │
│  └── HTTP/gRPC Interface                                    │
├─────────────────────────────────────────────────────────────┤
│  Batch System Pollers                                       │
│  ├── Store FSM Poller Threads                              │
│  ├── Peer FSM Poller Threads                               │
│  └── Apply FSM Poller Threads                              │
├─────────────────────────────────────────────────────────────┤
│  Specialized Workers                                        │
│  ├── Async Write Workers                                   │
│  ├── Async Read Workers                                    │
│  ├── Background Workers                                    │
│  └── High Priority Pool                                    │
└─────────────────────────────────────────────────────────────┘
```

### FSM Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Store FSM     │    │    Peer FSM     │    │   Apply FSM     │
│                 │    │                 │    │                 │
│ • Route msgs    │───▶│ • Raft consensus│───▶│ • State machine │
│ • Manage regions│    │ • Propose cmds  │    │ • Apply entries │
│ • Handle ticks  │    │ • Handle ready  │    │ • Write to DB   │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## Detailed Write Flow

### Stage 1: Client Request Entry

**Thread Pool**: Main Server Thread  
**Files**: `src/server/raftkv/mod.rs`, `src/storage/mod.rs`

```rust
// Client write request entry point
async fn async_write(
    &self,
    ctx: &Context,
    batch: WriteData,
    subscribed: u8,
    on_applied: Option<OnAppliedCb>,
) -> WriteRes {
    // Validate request
    if batch.modifies.is_empty() {
        return Err(KvError::from(KvErrorInner::EmptyRequest));
    }
    
    // Convert to RaftCmdRequest
    let mut cmd = RaftCmdRequest::default();
    cmd.set_header(new_request_header(ctx));
    cmd.set_requests(reqs.into());
    
    // Schedule the command
    let (tx, rx) = WriteResFeed::pair();
    let proposed_cb = self.schedule_raft_command(cmd, tx);
}
```

**Process**:
1. Client sends write request via gRPC/HTTP
2. Request is validated and converted to `WriteData`
3. `RaftCmdRequest` is constructed with proper headers
4. Request is scheduled for processing by the appropriate region

### Stage 2: Store FSM Processing

**Thread Pool**: Store FSM Poller Thread  
**Files**: `components/raftstore/src/store/fsm/store.rs`

```rust
// Store FSM message handling
fn handle_msgs(&mut self, msgs: &mut Vec<StoreMsg<EK>>) {
    let timer = SlowTimer::from_millis(100);
    let count = msgs.len();
    
    for m in msgs.drain(..) {
        distribution[m.discriminant()] += 1;
        match m {
            StoreMsg::Tick(tick) => self.on_tick(tick),
            StoreMsg::RaftMessage(msg) => {
                if !self.ctx.coprocessor_host.on_raft_message(&msg.msg) {
                    continue;
                }
                if let Err(e) = self.on_raft_message(msg) {
                    // Handle errors appropriately
                }
            }
            // ... other message types
        }
    }
}

// Route message to specific region
fn on_raft_message(&mut self, msg: Box<InspectedRaftMessage>) -> Result<()> {
    let region_id = msg.msg.get_region_id();
    let msg = match self.ctx.router.send(region_id, PeerMsg::RaftMessage(msg, None)) {
        Ok(()) => {
            forwarded.set(true);
            return Ok(());
        }
        Err(TrySendError::Disconnected(PeerMsg::RaftMessage(im, None))) => im.msg,
        // ... handle other errors
    };
}
```

**Process**:
1. Store FSM receives the write request
2. Message is validated and processed
3. Request is routed to the specific region's Peer FSM
4. Uses the router to send `PeerMsg::RaftCommand` to target region

### Stage 3: Peer FSM Processing

**Thread Pool**: Peer FSM Poller Thread  
**Files**: `components/raftstore/src/store/fsm/peer.rs`

```rust
// Peer FSM message handling
fn handle_msgs(&mut self, msgs: &mut Vec<PeerMsg<EK, ER>>) {
    for m in msgs.drain(..) {
        if self.fsm.stopped && !matches!(&m, PeerMsg::RaftCommand(_)) {
            continue;
        }
        
        match m {
            PeerMsg::RaftMessage(msg, sent_time) => {
                if let Some(sent_time) = sent_time {
                    let wait_time = sent_time.saturating_elapsed().as_secs_f64();
                    self.ctx.raft_metrics.process_wait_time.observe(wait_time);
                }
                
                if !self.ctx.coprocessor_host.on_raft_message(&msg.msg) {
                    continue;
                }
                
                if let Err(e) = self.on_raft_message(msg) {
                    // Handle errors
                }
            }
            PeerMsg::RaftCommand(cmd) => {
                let propose_time = cmd.send_time.saturating_elapsed();
                self.ctx.raft_metrics.propose_wait_time.observe(propose_time.as_secs_f64());
                
                // Propose the command to Raft
                self.propose_raft_command(msg, cb, diskfullopt);
            }
            // ... other message types
        }
    }
}
```

**Process**:
1. Peer FSM receives the `RaftCommand`
2. Request is validated and permissions are checked
3. `propose_raft_command()` is called to submit to Raft consensus
4. Callback is stored for later execution

### Stage 4: Raft Proposal and Log Entry

**Thread Pool**: Peer FSM Poller Thread  
**Files**: `components/raftstore/src/store/peer.rs`

```rust
// Propose command to Raft
fn propose_raft_command_internal(
    &mut self,
    mut msg: RaftCmdRequest,
    cb: Callback<EK::Snapshot>,
    diskfullopt: DiskFullOpt,
) {
    // Pre-propose validation
    match self.pre_propose_raft_command(&msg) {
        Ok(Some(resp)) => {
            cb.invoke_with_response(resp);
            return;
        }
        Err(e) => {
            cb.invoke_with_response(new_error(e));
            return;
        }
        _ => (),
    }
    
    // Propose to Raft
    let mut resp = RaftCmdResponse::default();
    let term = self.fsm.peer.term();
    bind_term(&mut resp, term);
    
    if self.fsm.peer.propose(self.ctx, cb, msg, resp, diskfullopt) {
        self.fsm.has_ready = true;
    }
}

// Actual Raft proposal
fn propose_normal(&mut self, poll_ctx: &mut PollContext<EK, ER, T>, req: RaftCmdRequest) -> Result<()> {
    let data = req.write_to_bytes()?;
    poll_ctx.raft_metrics.propose_log_size.observe(data.len() as f64);
    
    if data.len() as u64 > poll_ctx.cfg.raft_entry_max_size.0 {
        return Err(Error::RaftEntryTooLarge {
            region_id: self.region_id,
            entry_size: data.len() as u64,
        });
    }
    
    let propose_index = self.next_proposal_index();
    self.raft_group.propose(ctx.to_vec(), data)?;
    
    // Store callback for later execution
    self.pending_cmds.append_normal(PendingCmd {
        index: propose_index,
        term: self.term(),
        cb,
        // ... other fields
    });
    
    Ok(())
}
```

**Process**:
1. Request is serialized and validated
2. Entry size is checked against limits
3. Command is proposed to Raft consensus
4. Callback is stored in `pending_cmds` queue
5. Raft ensures the entry is replicated and committed

### Stage 5: Raft Ready Processing

**Thread Pool**: Peer FSM Poller Thread  
**Files**: `components/raftstore/src/store/peer_storage.rs`

```rust
// Handle Raft ready with committed entries
pub fn handle_raft_ready(
    &mut self,
    ready: &mut Ready,
    destroy_regions: Vec<metapb::Region>,
) -> Result<(HandleReadyResult, WriteTask<EK, ER>)> {
    let region_id = self.get_region_id();
    let prev_raft_state = self.raft_state().clone();
    
    let mut write_task = WriteTask::new(region_id, self.peer_id, ready.number());
    
    // Handle snapshot if present
    let mut res = if ready.snapshot().is_empty() {
        HandleReadyResult::SendIoTask
    } else {
        let last_first_index = self.first_index().unwrap();
        let (snap_region, for_witness) = self.apply_snapshot(ready.snapshot(), &mut write_task, &destroy_regions)?;
        HandleReadyResult::Snapshot(Box::new(HandleSnapshotResult {
            msgs: ready.take_persisted_messages(),
            snap_region,
            destroy_regions,
            last_first_index,
            for_witness,
        }))
    };
    
    // Append committed entries
    if !ready.entries().is_empty() {
        self.append(ready.take_entries(), &mut write_task);
    }
    
    // Update Raft state
    if self.raft_state().get_last_index() > 0 {
        if let Some(hs) = ready.hs() {
            self.raft_state_mut().set_hard_state(hs.clone());
        }
    }
    
    // Save Raft state if changed
    if prev_raft_state != *self.raft_state() || !ready.snapshot().is_empty() {
        write_task.raft_state = Some(self.raft_state().clone());
    }
    
    Ok((res, write_task))
}
```

**Process**:
1. Raft ready contains committed log entries
2. Log entries are appended to the write task
3. Raft state is updated and saved
4. Write task is prepared for async write workers

### Stage 6: Async Write Worker Processing

**Thread Pool**: Async Write Workers  
**Files**: `components/raftstore/src/store/async_io/write.rs`

```rust
// Async write worker main loop
fn run(&mut self) {
    let mut stopped = false;
    while !stopped {
        let handle_begin = match self.receiver.recv() {
            Ok(msg) => {
                let now = Instant::now();
                stopped |= self.handle_msg(msg);
                now
            }
            Err(_) => return,
        };
        
        // Batch multiple write tasks
        while self.batch.get_raft_size() < self.raft_write_size_limit {
            match self.receiver.try_recv() {
                Ok(msg) => {
                    stopped |= self.handle_msg(msg);
                }
                Err(TryRecvError::Empty) => {
                    if self.batch.should_wait() {
                        self.batch.wait_for_a_while();
                        continue;
                    } else {
                        break;
                    }
                }
                Err(TryRecvError::Disconnected) => {
                    stopped = true;
                    break;
                }
            };
        }
        
        if self.batch.is_empty() {
            self.clear_latency_inspect();
            continue;
        }
        
        // Write to database
        self.write_to_db(true);
        self.clear_latency_inspect();
    }
}

// Write to RocksDB and RaftDB
pub fn write_to_db(&mut self, notify: bool) {
    if self.batch.is_empty() {
        return;
    }
    
    let timer = Instant::now();
    self.batch.before_write_to_db(&self.metrics);
    
    // Write KV data to RocksDB
    let mut write_kv_time = 0f64;
    if let ExtraBatchWrite::V1(kv_wb) = &mut self.batch.extra_batch_write {
        if !kv_wb.is_empty() {
            let mut write_opts = WriteOptions::new();
            write_opts.set_sync(true);
            kv_wb.write_opt(&write_opts).unwrap_or_else(|e| {
                panic!("store {}: {} failed to write to kv engine: {:?}", 
                       self.store_id, self.tag, e);
            });
            write_kv_time = duration_to_sec(now.saturating_elapsed());
            STORE_WRITE_KVDB_DURATION_HISTOGRAM.observe(write_kv_time);
        }
    }
    
    // Write Raft log to RaftDB
    let mut write_raft_time = 0f64;
    if !self.batch.raft_wbs[0].is_empty() {
        let now = Instant::now();
        self.perf_context.start_observe();
        for i in 0..self.batch.raft_wbs.len() {
            self.raft_engine.consume_and_shrink(
                &mut self.batch.raft_wbs[i],
                true,
                RAFT_WB_SHRINK_SIZE,
                RAFT_WB_DEFAULT_SIZE,
            ).unwrap_or_else(|e| {
                panic!("store {}: {} failed to write to raft engine: {:?}", 
                       self.store_id, self.tag, e);
            });
        }
        write_raft_time = duration_to_sec(now.saturating_elapsed());
        STORE_WRITE_RAFTDB_DURATION_HISTOGRAM.observe(write_raft_time);
    }
    
    self.batch.after_write_all();
}
```

**Process**:
1. Write tasks are collected and batched together
2. KV data is written to the main RocksDB instance
3. Raft log entries are written to the RaftDB instance
4. Both writes are persisted with appropriate sync options
5. Performance metrics are recorded

### Stage 7: Apply FSM Processing

**Thread Pool**: Apply FSM Poller Thread  
**Files**: `components/raftstore/src/store/fsm/apply.rs`

```rust
// Apply committed entries to state machine
fn process_raft_cmd(
    &mut self,
    apply_ctx: &mut ApplyContext<EK>,
    index: u64,
    term: u64,
    req: RaftCmdRequest,
) -> ApplyResult<EK::Snapshot> {
    if index == 0 {
        panic!("{} processing raft command needs a none zero index", self.tag);
    }
    
    // Set sync log hint if required
    apply_ctx.sync_log_hint |= should_sync_log(&req);
    
    // Pre-apply hooks
    apply_ctx.host.pre_apply(&self.region, &req);
    
    // Execute the actual write operation
    let (mut cmd, exec_result, should_write) = self.apply_raft_cmd(apply_ctx, index, term, req);
    
    if let ApplyResult::WaitMergeSource(_) = exec_result {
        return exec_result;
    }
    
    debug!(
        "applied command";
        "region_id" => self.region_id(),
        "peer_id" => self.id(),
        "index" => index
    );
    
    // Bind term and find callback
    cmd_resp::bind_term(&mut cmd.response, self.term);
    let cmd_cb = self.find_pending(index, term, is_conf_change_cmd(&cmd.request));
    
    // Add to applied batch
    apply_ctx.applied_batch.push(cmd_cb, cmd, &self.observe_info, self.region_id());
    
    if should_write {
        // Write apply state and commit
        self.write_apply_state(apply_ctx.kv_wb_mut());
        apply_ctx.commit(self);
    }
    
    exec_result
}

// Apply context commit
pub fn commit(&mut self, delegate: &mut ApplyDelegate<EK>) {
    if delegate.last_flush_applied_index < delegate.apply_state.get_applied_index() {
        delegate.maybe_write_apply_state(self);
    }
    self.commit_opt(delegate, true);
}

fn commit_opt(&mut self, delegate: &mut ApplyDelegate<EK>, persistent: bool) {
    delegate.update_metrics(self);
    if persistent {
        if let (_, Some(seqno)) = self.write_to_db() {
            delegate.unfinished_write_seqno.push(seqno);
        }
        self.prepare_for(delegate);
        delegate.last_flush_applied_index = delegate.apply_state.get_applied_index();
        delegate.has_pending_ssts = false;
    }
    self.kv_wb_last_bytes = self.kv_wb().data_size() as u64;
    self.kv_wb_last_keys = self.kv_wb().count() as u64;
}
```

**Process**:
1. Committed Raft entries are applied to the state machine
2. Actual KV operations (Put/Delete) are executed
3. Changes are accumulated in write batches
4. Apply state is updated to reflect the applied index

### Stage 8: Final RocksDB Persistence

**Thread Pool**: Apply FSM Poller Thread  
**Files**: `components/raftstore/src/store/fsm/apply.rs`

```rust
// Final write to RocksDB
pub fn write_to_db(&mut self) -> (bool, Option<SequenceNumber>) {
    let need_sync = self.sync_log_hint && !self.disable_wal;
    let mut seqno = None;
    
    // Handle pending SSTs first to maintain order
    if !self.pending_ssts.is_empty() {
        let tag = self.tag.clone();
        self.importer.ingest(&self.pending_ssts, &self.engine).unwrap_or_else(|e| {
            panic!("{} failed to ingest ssts {:?}: {:?}", tag, self.pending_ssts, e);
        });
        self.pending_ssts = vec![];
    }
    
    // Write KV data to RocksDB
    if !self.kv_wb_mut().is_empty() {
        self.perf_context.start_observe();
        let mut write_opts = engine_traits::WriteOptions::new();
        write_opts.set_sync(need_sync);
        write_opts.set_disable_wal(self.disable_wal);
        
        if self.disable_wal {
            let sn = SequenceNumber::pre_write();
            seqno = Some(sn);
        }
        
        let seq = self.kv_wb_mut().write_opt(&write_opts).unwrap_or_else(|e| {
            panic!("failed to write to engine: {:?}", e);
        });
        
        if let Some(seqno) = seqno.as_mut() {
            seqno.post_write(seq)
        }
        
        // Report performance metrics
        let trackers: Vec<_> = self
            .applied_batch
            .cb_batch
            .iter()
            .flat_map(|(cb, _)| cb.write_trackers())
            .flat_map(|trackers| trackers.as_tracker_token())
            .collect();
        self.perf_context.report_metrics(&trackers);
        
        self.sync_log_hint = false;
        
        // Shrink write batch if too large
        let data_size = self.kv_wb().data_size();
        if data_size > APPLY_WB_SHRINK_SIZE {
            let kv_wb = self.engine.write_batch_with_cap(DEFAULT_APPLY_WB_SIZE);
            let kv_wb = self.host.on_create_apply_write_batch(kv_wb);
            self.kv_wb = kv_wb;
        } else {
            self.kv_wb_mut().clear();
        }
        
        self.kv_wb_last_bytes = 0;
        self.kv_wb_last_keys = 0;
    }
    
    (need_sync, seqno)
}
```

**Process**:
1. Write batch containing all KV operations is written to RocksDB
2. WAL (Write-Ahead Log) is optionally synced for durability
3. Sequence number is returned for consistency tracking
4. Write batch is cleared or shrunk for memory efficiency
5. Performance metrics are reported

## Thread Pool Analysis

### Thread Pool Hierarchy and Responsibilities

| **Thread Pool** | **Purpose** | **Key Operations** | **Files** |
|----------------|-------------|-------------------|-----------|
| **Main Server Threads** | Client interface | Request validation, gRPC handling | `src/server/raftkv/mod.rs` |
| **Store FSM Poller** | Store-level coordination | Message routing, region management | `components/raftstore/src/store/fsm/store.rs` |
| **Peer FSM Poller** | Raft consensus | Command proposal, ready handling | `components/raftstore/src/store/fsm/peer.rs` |
| **Apply FSM Poller** | State machine application | Entry application, final writes | `components/raftstore/src/store/fsm/apply.rs` |
| **Async Write Workers** | Raft log persistence | RaftDB writes, batching | `components/raftstore/src/store/async_io/write.rs` |
| **Async Read Workers** | Read operations | Local reads, snapshots | `components/raftstore/src/store/worker/read.rs` |
| **Background Workers** | Maintenance tasks | Cleanup, compaction | Various worker files |

### Thread Coordination

```rust
// Batch system coordination
pub fn poll(&mut self) {
    let mut batch = Batch::with_capacity(self.max_batch_size);
    let mut reschedule_fsms = Vec::with_capacity(self.max_batch_size);
    
    while run && self.fetch_fsm(&mut batch) {
        // Process control FSM first
        if batch.control.is_some() {
            let len = self.handler.handle_control(batch.control.as_mut().unwrap());
            // Handle control FSM results
        }
        
        // Process normal FSMs (regions) in batch
        for (i, p) in batch.normals.iter_mut().enumerate() {
            let res = self.handler.handle_normal(p);
            // Handle normal FSM results
        }
        
        // Batch processing complete
        self.handler.end(&mut batch.normals);
    }
}
```

## FSM State Transitions

### Store FSM States

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   Idle      │───▶│ Processing  │───▶│   Ready     │
│             │    │ Messages    │    │             │
└─────────────┘    └─────────────┘    └─────────────┘
       ▲                   │                   │
       └───────────────────┴───────────────────┘
```

### Peer FSM States

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   Follower  │───▶│   Candidate │───▶│    Leader   │
│             │    │             │    │             │
└─────────────┘    └─────────────┘    └─────────────┘
       ▲                   │                   │
       └───────────────────┴───────────────────┘
```

### Apply FSM States

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   Waiting   │───▶│  Applying   │───▶│  Committed  │
│             │    │             │    │             │
└─────────────┘    └─────────────┘    └─────────────┘
```

## RocksDB Persistence Details

### Two-Phase Write Process

1. **Phase 1: Raft Log Persistence**
   - Raft log entries are written to RaftDB
   - Ensures durability of consensus decisions
   - Required for Raft protocol compliance

2. **Phase 2: State Machine Application**
   - Committed entries are applied to the state machine
   - Actual KV operations are executed
   - Changes are written to the main RocksDB instance

### Write Batch Optimization

```rust
// Write batch management
pub struct WriteTaskBatch<EK, ER> {
    raft_wbs: Vec<ER::LogBatch>,
    raft_states: HashMap<u64, RaftLocalState>,
    extra_batch_write: ExtraBatchWrite<EK>,
    tasks: Vec<WriteTask<EK, ER>>,
    // ... other fields
}

impl<EK, ER> WriteTaskBatch<EK, ER> {
    fn add_write_task(&mut self, raft_engine: &ER, mut task: WriteTask<EK, ER>) {
        // Split large batches if needed
        if self.raft_wb_split_size > 0
            && self.raft_wbs.last().unwrap().persist_size() >= self.raft_wb_split_size
        {
            self.flush_states_to_raft_wb();
            self.raft_wbs.push(raft_engine.log_batch(RAFT_WB_DEFAULT_SIZE));
        }
        
        // Merge write batches
        let raft_wb = self.raft_wbs.last_mut().unwrap();
        if let Some(wb) = task.raft_wb.take() {
            raft_wb.merge(wb).unwrap();
        }
        
        // Append entries
        raft_wb.append(
            task.region_id,
            task.overwrite_to,
            std::mem::take(&mut task.entries),
        ).unwrap();
    }
}
```

## Performance Considerations

### Batch Processing Benefits

1. **Reduced Context Switching**: Multiple operations per thread
2. **Better Cache Locality**: Related operations stay together
3. **Efficient Resource Utilization**: Minimize thread overhead
4. **Controlled Parallelism**: Balance throughput vs. ordering

### Memory Management

```rust
// Write batch shrinking
if data_size > APPLY_WB_SHRINK_SIZE {
    let kv_wb = self.engine.write_batch_with_cap(DEFAULT_APPLY_WB_SIZE);
    let kv_wb = self.host.on_create_apply_write_batch(kv_wb);
    self.kv_wb = kv_wb;
} else {
    self.kv_wb_mut().clear();
}
```

### Performance Metrics

```rust
// Key performance metrics
STORE_WRITE_KVDB_DURATION_HISTOGRAM.observe(write_kv_time);
STORE_WRITE_RAFTDB_DURATION_HISTOGRAM.observe(write_raft_time);
STORE_APPLY_LOG_HISTOGRAM.observe(duration_to_sec(elapsed));
FSM_POLL_DURATION.get(N::FSM_TYPE).observe(timer.saturating_elapsed_secs());
```

## Error Handling

### Error Propagation

1. **Client Level**: Request validation errors
2. **Store Level**: Routing and region errors
3. **Peer Level**: Raft consensus errors
4. **Apply Level**: State machine application errors
5. **Write Level**: Persistence errors

### Error Recovery

```rust
// Error handling in write operations
kv_wb.write_opt(&write_opts).unwrap_or_else(|e| {
    panic!("store {}: {} failed to write to kv engine: {:?}", 
           self.store_id, self.tag, e);
});

// Graceful error handling in FSM
if let Err(e) = self.on_raft_message(msg) {
    if matches!(&e, Error::RegionNotRegistered { .. }) {
        info!("handle raft message failed"; "err" => ?e);
    } else {
        error!(?e; "handle raft message failed");
    }
}
```

## Code References

### Key Files and Functions

| **Component** | **File** | **Key Functions** |
|---------------|----------|-------------------|
| **Client Interface** | `src/server/raftkv/mod.rs` | `async_write()` |
| **Store FSM** | `components/raftstore/src/store/fsm/store.rs` | `handle_msgs()`, `on_raft_message()` |
| **Peer FSM** | `components/raftstore/src/store/fsm/peer.rs` | `handle_msgs()`, `propose_raft_command()` |
| **Raft Proposal** | `components/raftstore/src/store/peer.rs` | `propose_normal()` |
| **Raft Ready** | `components/raftstore/src/store/peer_storage.rs` | `handle_raft_ready()` |
| **Async Write** | `components/raftstore/src/store/async_io/write.rs` | `run()`, `write_to_db()` |
| **Apply FSM** | `components/raftstore/src/store/fsm/apply.rs` | `process_raft_cmd()`, `write_to_db()` |
| **Batch System** | `components/batch-system/src/batch.rs` | `poll()`, `handle_normal()` |

### Configuration Parameters

```rust
// Key configuration parameters
pub struct Config {
    pub max_batch_size: usize,           // Maximum FSMs per batch
    pub messages_per_tick: usize,        // Messages per region per tick
    pub raft_write_size_limit: usize,    // Raft write batch size limit
    pub raft_write_batch_size_hint: usize, // Raft write batch hint
    pub raft_write_wait_duration: Duration, // Wait duration for batching
}
```

## Summary

### Key Insights

1. **Multi-Stage Processing**: Write requests flow through multiple specialized thread pools and FSMs
2. **Sequential per Region**: Each region's writes are processed sequentially to maintain Raft ordering
3. **Parallel across Regions**: Different regions can be processed concurrently for high throughput
4. **Batch Optimization**: Multiple operations are batched together for efficiency
5. **Two-Phase Commit**: Raft log is written first, then state machine is applied
6. **Dedicated Workers**: Specialized threads handle different aspects of the write process

### Performance Characteristics

- **High Throughput**: Parallel processing across regions
- **Strong Consistency**: Sequential processing within regions
- **Low Latency**: Batch processing reduces overhead
- **Scalability**: Multiple regions can be processed concurrently
- **Durability**: Two-phase write ensures data persistence

### Design Trade-offs

**Advantages**:
- Maintains Raft consensus guarantees
- High performance through parallelization
- Efficient resource utilization
- Scalable architecture

**Challenges**:
- Complex thread coordination
- Potential for head-of-line blocking
- Configuration complexity
- Debugging difficulty

This architecture demonstrates how TiKV successfully balances the competing demands of high performance and strong consistency in a distributed storage system, making it suitable for production workloads that require both high throughput and reliable data consistency.

---

*This document was generated by analyzing the TiKV codebase, specifically focusing on the write request flow through RaftStore thread pools and FSMs. For the most up-to-date information, please refer to the official TiKV documentation and source code.*
