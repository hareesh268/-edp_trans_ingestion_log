package com.optum.exts.cdb.stream.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.HashMap;

/**
 * Created by rgupta59
 */
@Configuration
@ConfigurationProperties(prefix = "input")
public class UtilityConfig {
    @Valid
    private  HashMap<String, String> queryLookup;

    @NotNull
    String postgresUrl;

    @NotNull
    String postgresUser;

    @NotNull
    String postgresPwd;

    @NotNull
    String schema;

    @NotNull
    String physicalDelValue;

    @NotNull
    String emptyJsonValue;

    @NotNull
    String srcSysId;

    //@Null
    String groupName ;

    public String getGroupName() {
        return groupName;
    }

    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }

    public String getPhysicalDelValue() {
        return physicalDelValue;
    }

    public void setPhysicalDelValue(String physicalDelValue) {
        this.physicalDelValue = physicalDelValue;
    }

    public String getEmptyJsonValue() {
        return emptyJsonValue;
    }

    public void setEmptyJsonValue(String emptyJsonValue) {
        this.emptyJsonValue = emptyJsonValue;
    }

    public String getSrcSysId() {
        return srcSysId;
    }

    public void setSrcSysId(String srcSysId) {
        this.srcSysId = srcSysId;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getPostgresUser() {
        return postgresUser;
    }

    public void setPostgresUser(String postgresUser) {
        this.postgresUser = postgresUser;
    }

    public String getPostgresPwd() {
        return postgresPwd;
    }

    public void setPostgresPwd(String postgresPwd) {
        this.postgresPwd = postgresPwd;
    }

    public String getPostgresUrl() {
        return postgresUrl;
    }

    public void setPostgresUrl(String postgresUrl) {
        this.postgresUrl = postgresUrl;
    }

    public HashMap<String, String> getQueryLookup() {
        return queryLookup;
    }

    public void setQueryLookup(HashMap<String, String> queryLookup) {
        this.queryLookup = queryLookup;
    }
}