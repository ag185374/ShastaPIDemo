package com.example.shastadataflow;

import org.apache.avro.reflect.Nullable;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

@DefaultCoder(AvroCoder.class)
public class BigtableFailedDoc {
    private String rowKey;
    @Nullable private  String erroredRowKey;
    private String payload;
    @Nullable private String errorMessage;
    @Nullable private String stacktrace;

    public String getRowKey(){return rowKey;};

    public String getPayload() {
        return payload;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public String getStacktrace() {
        return stacktrace;
    }

    public String getErroredRowKey() {
        return erroredRowKey;
    }

    public void setErroredRowKey(String erroredRowKey) {
        this.erroredRowKey = erroredRowKey;
    }

    public BigtableFailedDoc setRowKey(String rowKey) {
        this.rowKey = rowKey;
        return this;
    }

    public BigtableFailedDoc setPayload(String payload) {
        this.payload = payload;
        return this;
    }

    public BigtableFailedDoc setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
        return this;
    }

    public BigtableFailedDoc setStacktrace(String stacktrace) {
        this.stacktrace = stacktrace;
        return this;
    }
}
