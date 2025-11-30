package com.worker.service;

import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

@Component
public class WorkerInfoProvider {

    private final Environment env;

    @Value("${worker.host:localhost}")
    private String host;

    private String myUrl;

    public WorkerInfoProvider(Environment env) {
        this.env = env;
    }

    @PostConstruct
    public void init() {
        // Spring exposes actual assigned port as 'local.server.port'
        String port = env.getProperty("local.server.port");

        myUrl = "http://" + host + ":" + port;
        System.out.println("Worker resolved its URL → " + myUrl);
    }

    public String getMyUrl() {
        return myUrl;
    }
}
