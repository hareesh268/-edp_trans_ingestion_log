package com.optum.exts.cdb.stream.transformer.dataobjects;

public class HeadersDO {

    private String operation;
    private String changeSequence;
    private String timestamp;
    private String transactionId;
    private String transactionEventCounter;
    private String transactionLastEvent;

    public String getOperation() {
        return operation;
    }

    public void setOperation(String operation) {
        this.operation = operation;
    }

    public String getChangeSequence() {
        return changeSequence;
    }

    public void setChangeSequence(String changeSequence) {
        this.changeSequence = changeSequence;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    public String getTransactionEventCounter() {
        return transactionEventCounter;
    }

    public void setTransactionEventCounter(String transactionEventCounter) {
        this.transactionEventCounter = transactionEventCounter;
    }

    public String getTransactionLastEvent() {
        return transactionLastEvent;
    }

    public void setTransactionLastEvent(String transactionLastEvent) {
        this.transactionLastEvent = transactionLastEvent;
    }
}
