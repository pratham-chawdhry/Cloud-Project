package com.worker.service;

import com.worker.model.KeyValue;
import com.worker.model.ReplicaInfo;
import com.worker.model.ReplicaType;
import org.springframework.stereotype.Service;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Service
public class FailoverService {
    private final KeyValueStore keyValueStore;
    private final ReplicationService replicationService;
    private final WorkerRegistrar workerRegistrar;

    public FailoverService(KeyValueStore store,
                           ReplicationService replicationService,
                           WorkerRegistrar registrar) {
        this.keyValueStore = store;
        this.replicationService = replicationService;
        this.workerRegistrar = registrar;
    }

    public void applyFailover(List<String> deadWorkers) {
        Set<String> dead = (deadWorkers == null) ? new HashSet<>() : new HashSet<>(deadWorkers);
        String myUrl = workerRegistrar.getWorkerUrl();

        keyValueStore.getAll().forEach((key, kv) -> {
            try {
                ReplicaInfo info = kv.getReplicaInfo();
                if (info == null) return;

                String value = kv.getValue();
                String primary = info.getPrimaryReplica();
                String sync = info.getSyncReplica();
                String async = info.getAsyncReplica();

                Set<String> alive = replicationService.getAliveWorkers();
                if (alive == null) alive = new HashSet<>();
                int aliveCount = alive.size();

                if (aliveCount == 1 && alive.contains(myUrl)) {
                    info.setPrimaryReplica(myUrl);
                    info.setSyncReplica(null);
                    info.setAsyncReplica(null);
                    kv.setReplicaType(ReplicaType.PRIMARY);
                    replicationService.notifyPrimaryToController(key, myUrl);
                    return;
                }

                boolean primaryDead = primary != null && dead.contains(primary);
                boolean syncDead    = sync != null && dead.contains(sync);
                boolean asyncDead   = async != null && dead.contains(async);
                ReplicaType type = kv.getReplicaType();

                if (type == ReplicaType.SYNC && primaryDead) {
                    handleSyncBecomesPrimary(key, value, info, kv);
                    return;
                }

                if (type == ReplicaType.ASYNC && primaryDead && syncDead) {
                    handleAsyncBecomesPrimary(key, value, info, kv);
                    return;
                }

                if (type == ReplicaType.PRIMARY) {

                    if (syncDead && async != null && !asyncDead) {
                        handlePrimarySyncDeadOnly(key, value, info);
                        return;
                    }

                    if (!syncDead && (async == null || asyncDead)) {
                        handlePrimaryAsyncDeadOnly(key, value, info);
                        return;
                    }

                    if (syncDead && (async == null || asyncDead)) {
                        handlePrimaryBothDead(key, value, info, kv);
                        return;
                    }
                }

                alive = replicationService.getAliveWorkers();
                if (alive == null) alive = new HashSet<>();
                aliveCount = alive.size();

                sync = info.getSyncReplica();
                async = info.getAsyncReplica();

                if (kv.getReplicaType() == ReplicaType.PRIMARY) {

                    if (sync == null && aliveCount > 1) {
                        try {
                            Map<String, String> res = replicationService.syncReplicaCreate(key, value, async);
                            if (res != null) {
                                String syncReplica = res.get("syncReplica");
                                String asyncReplica = res.get("asyncReplica");
                                if (asyncReplica != null && asyncReplica.equals(syncReplica)) {
                                    asyncReplica = null;
                                }
                                info.setSyncReplica(syncReplica);
                                info.setAsyncReplica(asyncReplica);
                                if (asyncReplica != null) {
                                    boolean ok = replicationService.replicateAsync(key, value, asyncReplica, syncReplica);
                                    if (!ok) info.setAsyncReplica(null);
                                }
                            }
                        } catch (Exception ignored) {}
                        return;
                    }

                    if (sync != null && (async == null || async.isBlank()) && aliveCount > 2) {
                        String newAsync = replicationService.chooseAsyncCandidate(myUrl, sync);
                        if (newAsync != null) {
                            boolean updated = replicationService.syncUpdate(key, value, sync, newAsync);
                            if (updated) {
                                boolean queued = replicationService.replicateAsync(key, value, newAsync, sync);
                                if (queued) info.setAsyncReplica(newAsync);
                                else info.setAsyncReplica(null);
                            } else {
                                info.setAsyncReplica(null);
                            }
                        }
                    }
                }

            } catch (Exception ex) {
                keyValueStore.remove(key);
            }
        });
    }

    private void handleSyncBecomesPrimary(String key, String value, ReplicaInfo info, KeyValue kv) {
        String myUrl = workerRegistrar.getWorkerUrl();
        int aliveCount = replicationService.getAliveWorkers().size();

        Map<String, String> res;
        try {
            res = replicationService.syncReplicaCreate(key, value, null);
        } catch (Exception e) {
            keyValueStore.remove(key);
            return;
        }

        if (res == null) {
            keyValueStore.remove(key);
            return;
        }

        String syncReplica = res.get("syncReplica");
        String asyncReplica = res.get("asyncReplica");

        if (aliveCount < 3) asyncReplica = null;

        info.setPrimaryReplica(myUrl);
        info.setSyncReplica(syncReplica);
        info.setAsyncReplica(asyncReplica);

        if (aliveCount >= 3 && asyncReplica != null) {
            boolean ok = replicationService.replicateAsync(key, value, asyncReplica, syncReplica);
            if (!ok) info.setAsyncReplica(null);
        }

        kv.setReplicaType(ReplicaType.PRIMARY);
        replicationService.notifyPrimaryToController(key, myUrl);
    }

    private void handleAsyncBecomesPrimary(String key, String value, ReplicaInfo info, KeyValue kv) {
        String myUrl = workerRegistrar.getWorkerUrl();
        int aliveCount = replicationService.getAliveWorkers().size();

        Map<String, String> res;
        try {
            res = replicationService.syncReplicaCreate(key, value, null);
        } catch (Exception e) {
            keyValueStore.remove(key);
            return;
        }

        if (res == null) {
            keyValueStore.remove(key);
            return;
        }

        String syncReplica = res.get("syncReplica");
        String asyncReplica = res.get("asyncReplica");

        if (aliveCount < 3) asyncReplica = null;

        info.setPrimaryReplica(myUrl);
        info.setSyncReplica(syncReplica);
        info.setAsyncReplica(asyncReplica);

        if (aliveCount >= 3 && asyncReplica != null) {
            boolean ok = replicationService.replicateAsync(key, value, asyncReplica, syncReplica);
            if (!ok) info.setAsyncReplica(null);
        }

        kv.setReplicaType(ReplicaType.PRIMARY);
        replicationService.notifyPrimaryToController(key, myUrl);
    }

    private void handlePrimarySyncDeadOnly(String key, String value, ReplicaInfo info) {
        int aliveCount = replicationService.getAliveWorkers().size();

        Map<String, String> res;
        try {
            res = replicationService.syncReplicaCreate(key, value, info.getAsyncReplica());
        } catch (Exception e) {
            info.setSyncReplica(null);
            info.setAsyncReplica(null);
            return;
        }

        if (res == null) {
            info.setSyncReplica(null);
            info.setAsyncReplica(null);
            return;
        }

        String syncReplica = res.get("syncReplica");
        String asyncReplica = res.get("asyncReplica");

        if (aliveCount < 3) asyncReplica = null;

        info.setSyncReplica(syncReplica);
        info.setAsyncReplica(asyncReplica);

        if (aliveCount >= 3 && asyncReplica != null) {
            boolean ok = replicationService.replicateAsync(key, value, asyncReplica, syncReplica);
            if (!ok) info.setAsyncReplica(null);
        }
    }

    private void handlePrimaryAsyncDeadOnly(String key, String value, ReplicaInfo info) {
        int aliveCount = replicationService.getAliveWorkers().size();

        if (aliveCount < 3) {
            info.setAsyncReplica(null);
            replicationService.syncUpdate(key, value, info.getSyncReplica(), info.getAsyncReplica());
            return;
        }

        String sync = info.getSyncReplica();
        String newAsync = replicationService.chooseAsyncCandidate(workerRegistrar.getWorkerUrl(), sync);

        if (newAsync == null) {
            info.setAsyncReplica(null);
            return;
        }

        boolean updated = replicationService.syncUpdate(key, value, sync, newAsync);
        if (!updated) {
            info.setAsyncReplica(null);
            return;
        }

        boolean queued = replicationService.replicateAsync(key, value, newAsync, sync);
        if (!queued) {
            info.setAsyncReplica(null);
            return;
        }

        info.setAsyncReplica(newAsync);
    }

    private void handlePrimaryBothDead(String key, String value, ReplicaInfo info, KeyValue kv) {
        int aliveCount = replicationService.getAliveWorkers().size();

        Map<String, String> res;

        try {
            res = replicationService.syncReplicaCreate(key, value, null);
        } catch (Exception e) {
            keyValueStore.remove(key);
            return;
        }

        if (res == null) {
            keyValueStore.remove(key);
            return;
        }

        String syncReplica = res.get("syncReplica");
        String asyncReplica = res.get("asyncReplica");

        if (aliveCount < 3) asyncReplica = null;

        info.setSyncReplica(syncReplica);
        info.setPrimaryReplica(workerRegistrar.getWorkerUrl());
        info.setAsyncReplica(asyncReplica);

        if (aliveCount >= 3 && asyncReplica != null) {
            boolean ok = replicationService.replicateAsync(key, value, asyncReplica, syncReplica);
            if (!ok) info.setAsyncReplica(null);
        }

        kv.setReplicaType(ReplicaType.PRIMARY);
    }
}
