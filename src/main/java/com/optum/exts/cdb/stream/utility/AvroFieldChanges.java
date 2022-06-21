package com.optum.exts.cdb.stream.utility;

import java.util.HashMap;
import java.util.Map;

public class AvroFieldChanges {

    private Map<String, Object> fieldValues = new HashMap<>();


    public void addChanges(String field, Object oldValue, Object newValue) {

        this.fieldValues.put(field, oldValue.toString()+ ", " + newValue.toString());
    }

    public String getChanges() {

        StringBuilder change = new StringBuilder("{");

        for(Map.Entry entry : fieldValues.entrySet()) {
            change.append(entry.getKey()).append(": ").append(entry.getValue()).append(" ");
        }
        change.append("}");

        return change.toString();
    }




}
