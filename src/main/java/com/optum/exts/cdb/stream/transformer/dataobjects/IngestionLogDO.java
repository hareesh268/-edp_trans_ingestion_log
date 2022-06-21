package com.optum.exts.cdb.stream.transformer.dataobjects;

import java.sql.Timestamp;

public class IngestionLogDO {

    private String row_key;
    private String hst_val_row_key;
    private String row_sts_cd;
    private String cdc_flag;
    private Timestamp cdc_ts;
    private String table_name;
    private java.sql.Timestamp ingestion_ts;
    private String hst_val;
    private String new_val;
    private long row_tmstmp;




    private long hst_row_tmstmp;


    public long getHst_row_tmstmp() {
        return hst_row_tmstmp;
    }

    public void setHst_row_tmstmp(long hst_row_tmstmp) {
        this.hst_row_tmstmp = hst_row_tmstmp;
    }

    public boolean isRowKeyChanged() {
        return isRowKeyChanged;
    }

    public void setRowKeyChanged(boolean rowKeyChanged) {
        this.isRowKeyChanged = rowKeyChanged;
    }

    private boolean isRowKeyChanged;


    public long getRow_tmstmp() {
        return row_tmstmp;
    }

    public void setRow_tmstmp(long row_tmstmp) {
        this.row_tmstmp = row_tmstmp;
    }

    public String getRow_key() {
        return row_key;
    }

    public void setRow_key(String row_key) {
        this.row_key = row_key;
    }

    public String getHst_val_row_key() {
        return hst_val_row_key;
    }

    public void setHst_val_row_key(String hst_val_row_key) {
        this.hst_val_row_key = hst_val_row_key;
    }

    public String getRow_sts_cd() {
        return row_sts_cd;
    }

    public void setRow_sts_cd(String row_sts_cd) {
        this.row_sts_cd = row_sts_cd;
    }

    public String getCdc_flag() {
        return cdc_flag;
    }

    public void setCdc_flag(String cdc_flag) {
        this.cdc_flag = cdc_flag;
    }

    public Timestamp getCdc_ts() {
        return cdc_ts;
    }

    public void setCdc_ts(Timestamp cdc_ts) {
        this.cdc_ts = cdc_ts;
    }

    public String getTable_name() {
        return table_name;
    }

    public void setTable_name(String table_name) {
        this.table_name = table_name;
    }

    public java.sql.Timestamp getIngestion_ts() {
        return ingestion_ts;
    }

    public void setIngestion_ts(java.sql.Timestamp ingestion_ts) {
        this.ingestion_ts = ingestion_ts;
    }

    public String getHst_val() {
        return hst_val;
    }

    public void setHst_val(String hst_val) {
        this.hst_val = hst_val;
    }

    public String getNew_val() {
        return new_val;
    }

    public void setNew_val(String new_val) {
        this.new_val = new_val;
    }
}
