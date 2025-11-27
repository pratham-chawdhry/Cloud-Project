package com.controller.service;

import org.springframework.stereotype.Service;

import java.util.*;

@Service
public class ReplicaAssignmentService {

    private final WorkerRegistry registry;

    public ReplicaAssignmentService(WorkerRegistry registry) {
        this.registry = registry;
    }

    public synchronized Map<String, List<String>> recomputeReplicas() {
        List<String> ids = registry.getAliveWorkers();
        int n = ids.size();
        Map<String, List<String>> result = new HashMap<>();

        if (n == 0)
            return result;

        if (n < 2) {
            for (String id : ids) {
                result.put(
                        registry.getUrl(id),
                        Arrays.asList(null, null)   // <-- SAFE
                );
            }
            return result;
        }

        for (int i = 0; i < n; i++) {
            String id = ids.get(i);

            String syncId = ids.get((i + 1) % n);
            String asyncId = (n >= 3) ? ids.get((i + 2) % n) : null;

            String url = registry.getUrl(id);
            String syncUrl = registry.getUrl(syncId);
            String asyncUrl = asyncId == null ? null : registry.getUrl(asyncId);

            result.put(url, Arrays.asList(syncUrl, asyncUrl));
        }

        return result;
    }
}
