package com.worker.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class KeyValue {
    private String key;
    private ReplicaType replicaType;
    private String value;
    private ReplicaInfo replicaInfo;
}
