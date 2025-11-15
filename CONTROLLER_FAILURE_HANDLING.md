# Controller Failure Handling Implementation

This document explains how controller failures are handled in the distributed key-value store system.

## Overview

The system now includes comprehensive controller failure handling that ensures:
1. **State Persistence**: Controller state is saved to disk periodically
2. **State Recovery**: Controller can recover its state after restart
3. **Worker Resilience**: Workers can detect controller failures and continue operating
4. **Automatic Recovery**: System automatically re-establishes connections when controller recovers

## Architecture

### Controller Side

#### 1. State Persistence Service (`StatePersistenceService.java`)

- **Purpose**: Saves and loads controller state to/from disk
- **State File**: `controller-state.json` (configurable via `controller.state.file`)
- **What's Saved**:
  - Worker URLs and their configurations
  - Worker IDs and active status
  - Last heartbeat timestamps
  - Replica assignments for each worker
  - Worker URL ordering

- **Features**:
  - Atomic writes (uses temporary file + rename)
  - Automatic state recovery on startup
  - Graceful handling of missing state files (initializes from config)

#### 2. Worker Manager Integration

- **State Saving Triggers**:
  - Periodically (every 30 seconds by default, configurable via `controller.state.save.interval`)
  - After worker heartbeat updates
  - After worker failure detection
  - After replica configuration changes

- **State Recovery**:
  - On application startup, tries to load state from disk first
  - If state exists, recovers worker configurations and replica assignments
  - If no state exists, initializes from configuration file
  - After recovery, re-establishes connections with workers

### Worker Side

#### 1. Controller Health Monitor (`ControllerHealthMonitor.java`)

- **Purpose**: Monitors controller availability and detects failures
- **Features**:
  - Periodic health checks (every 5 seconds by default)
  - Timeout detection (10 seconds by default)
  - Automatic failure detection
  - Recovery detection when controller comes back online

- **Configuration**:
  - `controller.health.check.interval`: Health check frequency (default: 5000ms)
  - `controller.health.timeout`: Timeout before considering controller down (default: 10000ms)

#### 2. Retry Logic

- **Failure Reporter**: Enhanced with retry logic and exponential backoff
- **Heartbeat Sending**: Workers can retry sending heartbeats with exponential backoff
- **Controller Communication**: All controller communications include retry logic

## How It Works

### Normal Operation

1. Controller saves state periodically and on state changes
2. Workers monitor controller health continuously
3. Workers send heartbeats to controller (handled by controller's HeartbeatMonitor)

### Controller Failure Scenario

1. **Detection**:
   - Workers detect controller is not responding to health checks
   - Workers log the failure and continue operating

2. **Worker Behavior**:
   - Workers continue serving GET/PUT requests (they have their data)
   - Workers queue failure reports (will retry when controller recovers)
   - Workers periodically retry connecting to controller

3. **Controller Recovery**:
   - Controller restarts and loads state from `controller-state.json`
   - Controller recovers worker configurations and replica assignments
   - Controller re-establishes connections with workers
   - Controller's HeartbeatMonitor detects workers and updates their status
   - Workers detect controller is back online and resume normal operations

### State Recovery Process

When controller restarts:

1. **State Loading**:
   ```
   Controller starts → Loads controller-state.json → Recovers worker list and configurations
   ```

2. **Worker Reconnection**:
   ```
   Controller's HeartbeatMonitor checks workers → Updates worker status → Reconfigures replicas if needed
   ```

3. **Data Consistency**:
   - Controller's ReplicationManager ensures data consistency
   - Missing replicas are re-replicated if needed

## Configuration

### Controller Configuration (`application.properties`)

```properties
# State persistence
controller.state.file=controller-state.json
controller.state.save.interval=30000  # Save every 30 seconds
```

### Worker Configuration (`application.properties`)

```properties
# Controller URL
controller.url=http://localhost:8080

# Health monitoring
controller.health.check.interval=5000   # Check every 5 seconds
controller.health.timeout=10000         # 10 second timeout
```

## Benefits

1. **Fault Tolerance**: System continues operating even when controller is down
2. **State Recovery**: Controller can recover its state after restart
3. **No Data Loss**: Worker data persists independently
4. **Automatic Recovery**: System automatically re-establishes connections
5. **Minimal Downtime**: Workers continue serving requests during controller outages

## Limitations

1. **Single Controller**: System assumes one controller (no controller election)
2. **State File**: State is stored in a single file (consider backup strategies for production)
3. **Partitioning Changes**: If workers change while controller is down, manual intervention may be needed

## Testing Controller Failure

To test controller failure handling:

1. **Start the system**: Start controller and workers
2. **Verify state file**: Check that `controller-state.json` is created
3. **Stop controller**: Kill the controller process
4. **Observe workers**: Workers should log controller failure but continue operating
5. **Restart controller**: Start controller again
6. **Verify recovery**: Controller should load state and reconnect with workers

## Future Enhancements

Potential improvements:
- Controller election among workers
- Distributed state storage (e.g., database)
- State versioning and rollback
- Health check metrics and monitoring
- Automatic failover to backup controller

