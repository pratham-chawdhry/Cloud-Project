package com.worker.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ReplicaInfo {
    private String primaryReplica;   // may be null
    private String syncReplica;      // may be null
    private String asyncReplica;     // may be null

    public ReplicaInfo(ReplicaInfo replicaInfo) {
        this.primaryReplica = replicaInfo.getPrimaryReplica();
        this.syncReplica = replicaInfo.getSyncReplica();
        this.asyncReplica = replicaInfo.getAsyncReplica();
    }
}
