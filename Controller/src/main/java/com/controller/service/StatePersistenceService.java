package com.controller.service;

import com.controller.model.WorkerNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.File;
import java.nio.file.Files;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@Service
public class StatePersistenceService {

    private final ObjectMapper mapper = new ObjectMapper().findAndRegisterModules()
            .enable(SerializationFeature.INDENT_OUTPUT);

    @Value("${controller.state.file:controller-state.json}")
    private String stateFilePath;

    @Value("${controller.state.fallback-dir:./}")
    private String fallbackDir;

    public static class ControllerState {
        private Map<String, WorkerNode> workers;
        private List<String> workerUrls;
        private long lastSaved;

        public ControllerState() {}
        public ControllerState(Map<String, WorkerNode> workers, List<String> workerUrls) {
            this.workers = workers;
            this.workerUrls = workerUrls;
            this.lastSaved = System.currentTimeMillis();
        }
        public Map<String, WorkerNode> getWorkers() { return workers == null ? Collections.emptyMap() : workers; }
        public List<String> getWorkerUrls() { return workerUrls == null ? Collections.emptyList() : workerUrls; }
        public long getLastSaved() { return lastSaved; }
    }

    public synchronized void saveState(Map<String, WorkerNode> workers, List<String> workerUrls) {
        ControllerState state = new ControllerState(workers, workerUrls);
        File f = new File(stateFilePath);
        try {
            if (f.getParentFile() != null) f.getParentFile().mkdirs();
            mapper.writeValue(f, state);
        } catch (Exception e) {
            try {
                Files.createDirectories(new File(fallbackDir).toPath());
                File fallback = new File(fallbackDir, "controller-state-fallback-" + System.currentTimeMillis() + ".json");
                mapper.writeValue(fallback, state);
            } catch (Exception ignored) {}
        }
    }

    public synchronized ControllerState loadState() {
        File f = new File(stateFilePath);
        if (!f.exists()) return null;
        try {
            ControllerState s = mapper.readValue(f, ControllerState.class);
            if (s.getWorkers() == null) s = new ControllerState(Collections.emptyMap(), s.getWorkerUrls());
            return s;
        } catch (Exception e) {
            return null;
        }
    }
}
