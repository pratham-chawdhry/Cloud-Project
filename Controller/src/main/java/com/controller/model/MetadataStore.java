package com.controller.service;

import com.controller.model.KeyMetadata;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class MetadataStore {

    private final ConcurrentHashMap<String, KeyMetadata> table = new ConcurrentHashMap<>();
    private final ObjectMapper mapper = new ObjectMapper();
    private final Object writeLock = new Object();

    private final File metadataFile = new File("metadata.json");

    public MetadataStore() {
        load();
    }

    public KeyMetadata get(String key) {
        return table.get(key);
    }

    public void update(String key, KeyMetadata meta) {
        table.put(key, meta);
        saveAtomic();
    }

    private void load() {
        try {
            if (metadataFile.exists() && metadataFile.length() > 0) {
                Map<String, KeyMetadata> data = mapper.readValue(
                        metadataFile,
                        mapper.getTypeFactory().constructMapType(
                                Map.class,
                                String.class,
                                KeyMetadata.class
                        )
                );
                table.putAll(data);
            }
        } catch (Exception e) {
            System.err.println("Failed to load metadata.json: " + e.getMessage());
        }
    }

    private void saveAtomic() {
        synchronized (writeLock) {
            try {
                File tmp = new File("metadata.json.tmp");

                mapper.writerWithDefaultPrettyPrinter().writeValue(tmp, table);

                try (FileOutputStream fos = new FileOutputStream(tmp, true)) {
                    fos.getFD().sync();
                }

                Files.move(
                        tmp.toPath(),
                        metadataFile.toPath(),
                        StandardCopyOption.REPLACE_EXISTING,
                        StandardCopyOption.ATOMIC_MOVE
                );

            } catch (Exception e) {
                System.err.println("Failed to write metadata.json: " + e.getMessage());
            }
        }
    }
}
