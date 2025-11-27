package com.worker.controller;

import com.worker.model.ApiResponse;
import com.worker.model.WorkerAssignment;
import com.worker.service.ReplicationService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/replicas")
public class ReplicaController {

    private final ReplicationService replicationService;

    @Autowired
    public ReplicaController(ReplicationService replicationService) {
        this.replicationService = replicationService;
    }

    @PostMapping("/update")
    public ResponseEntity<ApiResponse<String>> configureReplicas(@RequestBody java.util.List<String> replicas) {
        try {
            String sync = (replicas != null && replicas.size() > 0) ? replicas.get(0) : null;
            String async = (replicas != null && replicas.size() > 1) ? replicas.get(1) : null;

            replicationService.updateReplicaTargets(sync, async);
            return ResponseEntity.ok(ApiResponse.success(200, "Replica targets updated: sync=" + sync + ", async=" + async));
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(ApiResponse.fail(500, e.getMessage()));
        }
    }

    @GetMapping("/get")
    public ResponseEntity<ApiResponse<WorkerAssignment>> getReplicas() {
        try {
            var assignment = new WorkerAssignment();
            assignment.setSyncReplica(replicationService.getSyncReplica());
            assignment.setAsyncReplica(replicationService.getAsyncReplica());
            return ResponseEntity.ok(ApiResponse.success(200, assignment));
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(ApiResponse.fail(500, e.getMessage()));
        }
    }
}
