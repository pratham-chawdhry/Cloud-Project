package com.worker.controller;

import com.worker.model.ApiResponse;
import com.worker.model.KeyValue;
import com.worker.model.ReplicaInfo;
import com.worker.model.ReplicaType;
import com.worker.service.KeyValueStore;
import com.worker.service.ReplicationService;
import com.worker.service.WorkerRegistrar;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/")
public class KeyValueController {

    @Autowired
    private KeyValueStore keyValueStore;

    @Autowired
    private ReplicationService replicationService;

    @Autowired
    private WorkerRegistrar workerRegistrar;

    @PostMapping("/put")
    public ResponseEntity<ApiResponse<Object>> put(@RequestBody Map<String, String> body) {

        String key = body.get("key");
        String value = body.get("value");

        if (key == null || value == null) {
            return ResponseEntity.badRequest()
                    .body(ApiResponse.fail(400, "Key and value required"));
        }

        KeyValue existing = keyValueStore.get(key);
        boolean isUpdate = (existing != null);

        String oldValue = null;
        ReplicaInfo oldInfo = null;

        if (isUpdate) {
            oldValue = existing.getValue();
            oldInfo = new ReplicaInfo(existing.getReplicaInfo());
        }

        try {
            String primaryUrl = workerRegistrar.getWorkerUrl();
            String syncTarget;
            String asyncTarget;

            if (!isUpdate) {
                // NEW KEY → assign replicas
                Map<String, String> syncMeta =
                        replicationService.syncReplicaCreate(key, value, null);

                syncTarget = syncMeta.get("syncReplica");
                asyncTarget = syncMeta.get("asyncReplica");

                if (asyncTarget != null) {
                    replicationService.replicateAsync(key, value, asyncTarget, syncTarget);
                }

                replicationService.notifyPrimaryToController(key, primaryUrl);

            } else {
                // UPDATE on existing primary
                String oldSync = oldInfo.getSyncReplica();
                String oldAsync = oldInfo.getAsyncReplica();

                boolean ok = replicationService.syncUpdate(key, value, oldSync, oldAsync);
                if (!ok) throw new Exception("SYNC update failed");

                syncTarget = oldSync;
                asyncTarget = oldAsync;

                if (oldAsync != null) {
                    replicationService.replicateAsync(key, value, oldAsync, syncTarget);
                }
            }

            ReplicaInfo newInfo = new ReplicaInfo(
                    primaryUrl,
                    syncTarget,
                    asyncTarget
            );

            keyValueStore.put(new KeyValue(key, ReplicaType.PRIMARY, value, newInfo));

            return ResponseEntity.ok(ApiResponse.success(200, Map.of(
                    "primaryReplica", primaryUrl,
                    "syncReplica", syncTarget,
                    "asyncReplica", asyncTarget
            )));

        } catch (Exception e) {

            if (!isUpdate) {
                keyValueStore.remove(key);
            } else {
                keyValueStore.put(new KeyValue(key, ReplicaType.PRIMARY, oldValue, oldInfo));
            }

            return ResponseEntity.status(503)
                    .body(ApiResponse.fail(
                            503,
                            "SYNC replication failed, rolled back: " + e.getMessage()
                    ));
        }
    }

    @PostMapping("/replicate")
    public ResponseEntity<ApiResponse<String>> replicate(@RequestBody Map<String, Object> body) {
        try {
            String key        = (String) body.get("key");
            String value      = (String) body.get("value");
            String primaryUrl = (String) body.get("primaryUrl");
            String syncUrl    = (String) body.get("syncUrl");
            String asyncUrl   = (String) body.get("asyncUrl"); // may be null

            if (key == null || value == null || primaryUrl == null || syncUrl == null) {
                return ResponseEntity.badRequest()
                        .body(ApiResponse.fail(400, "key, value, primaryUrl, and syncUrl are required"));
            }

            ReplicaInfo info = new ReplicaInfo();
            info.setPrimaryReplica(primaryUrl);
            info.setSyncReplica(syncUrl);
            info.setAsyncReplica(asyncUrl);

            KeyValue kv = new KeyValue();
            kv.setKey(key);
            kv.setValue(value);
            kv.setReplicaType(ReplicaType.SYNC);
            kv.setReplicaInfo(info);

            keyValueStore.put(kv);

            return ResponseEntity.ok(
                    ApiResponse.success(200,
                            "Replicated key=" + key + " from " + primaryUrl)
            );

        } catch (Exception e) {
            return ResponseEntity.internalServerError()
                    .body(ApiResponse.fail(500, "Error in /replicate: " + e.getMessage()));
        }
    }

    @PostMapping("/get")
    public ResponseEntity<ApiResponse<KeyValue>> get(@RequestBody Map<String, String> body) {
        try {
            String key = body.get("key");
            if (key == null || key.isBlank()) {
                return ResponseEntity.badRequest()
                        .body(ApiResponse.fail(400, "Key required"));
            }

            KeyValue kv = keyValueStore.get(key);

            if (kv == null) {
                return ResponseEntity.status(404)
                        .body(ApiResponse.fail(404, "Key not found"));
            }

            return ResponseEntity.ok(ApiResponse.success(200, kv));

        } catch (Exception e) {
            return ResponseEntity.internalServerError()
                    .body(ApiResponse.fail(500, e.getMessage()));
        }
    }
}
