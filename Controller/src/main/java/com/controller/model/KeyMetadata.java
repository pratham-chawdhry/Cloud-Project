package com.controller.model;

public class KeyMetadata {
    private String primaryReplica;
    private String syncReplica;
    private String asyncReplica;

    public KeyMetadata(String primaryReplica, String syncReplica, String asyncReplica) {
        this.primaryReplica = primaryReplica;
        this.syncReplica = syncReplica;
        this.asyncReplica = asyncReplica;
    }

    public String getPrimaryReplica() { return primaryReplica; }
    public String getSyncReplica() { return syncReplica; }
    public String getAsyncReplica() { return asyncReplica; }

    public void setPrimaryReplica(String p) { this.primaryReplica = p; }
    public void setSyncReplica(String s) { this.syncReplica = s; }
    public void setAsyncReplica(String a) { this.asyncReplica = a; }
}
