package com.worker.service;

import com.worker.model.KeyValue;
import com.worker.model.ReplicaInfo;
import com.worker.model.ReplicaType;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.Set;

@Service
public class RecoveryService {

    private final KeyValueStore keyValueStore;
    private final ReplicationService replicationService;
    private final WorkerRegistrar workerRegistrar;

    public RecoveryService(KeyValueStore keyValueStore,
                           ReplicationService replicationService,
                           WorkerRegistrar workerRegistrar) {
        this.keyValueStore = keyValueStore;
        this.replicationService = replicationService;
        this.workerRegistrar = workerRegistrar;
    }

    public void applyRecovery() {
        String myUrl = workerRegistrar.getWorkerUrl();
        Set<String> alive = replicationService.getAliveWorkers();
        int aliveCount = alive.size();

        keyValueStore.getAll().forEach((key, kv) -> {
            ReplicaInfo info = kv.getReplicaInfo();
            if (info == null || kv.getReplicaType() != ReplicaType.PRIMARY) return;

            String value = kv.getValue();
            String sync = info.getSyncReplica();
            String async = info.getAsyncReplica();

            // === 1 → 2 RECOVERY: missing syncReplica ===
            if (sync == null && aliveCount >= 2) {
                try {
                    Map<String, String> res = replicationService.syncReplicaCreate(key, value, null);
                    if (res == null) return;

                    String syncReplica = res.get("syncReplica");
                    String asyncReplica = (aliveCount >= 3) ? res.get("asyncReplica") : null;

                    info.setSyncReplica(syncReplica);
                    info.setAsyncReplica(asyncReplica);

                    if (asyncReplica != null && aliveCount >= 3) {
                        boolean ok = replicationService.replicateAsync(key, value, asyncReplica, syncReplica);
                        if (!ok) info.setAsyncReplica(null);
                    }

                    return; // sync was rebuilt
                } catch (Exception e) {
                    return;
                }
            }

            // === 2 → 3 RECOVERY: sync exists but async missing ===
            if (sync != null && async == null && aliveCount >= 3) {

                String newAsync = replicationService.chooseAsyncCandidate(myUrl, sync);
                if (newAsync == null) return;

                boolean updated = replicationService.syncUpdate(key, value, sync, newAsync);
                if (!updated) return;

                boolean queued = replicationService.replicateAsync(key, value, newAsync, sync);
                if (!queued) return;

                info.setAsyncReplica(newAsync);
            }
        });
    }
}
