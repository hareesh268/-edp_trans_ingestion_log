package com.optum.exts.cdb.stream.transformer.dataobjects;

import com.google.gson.JsonElement;

import java.util.Map;

public class IngestionLogJsonStructureDO {

    private Map<String, JsonElement> newJson;
    private Map<String, JsonElement> oldJson;
    private Map<String, JsonElement> headers;

    public void setNewJson(Map<String, JsonElement> newJson) {
        this.newJson = newJson;
    }

    public void setOldJson(Map<String, JsonElement> oldJson) {
        this.oldJson = oldJson;
    }

    public void setHeaders(Map<String, JsonElement> headers) {
        this.headers = headers;
    }

    public Map<String, JsonElement> getNewJson() {
        return newJson;
    }

    public Map<String, JsonElement> getOldJson() {
        return oldJson;
    }

    public Map<String, JsonElement> getHeaders() {
        return headers;
    }

}
