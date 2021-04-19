package com.example.shastadataflow;

import org.apache.avro.reflect.Nullable;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

@DefaultCoder(AvroCoder.class)
public class BigtableFailedDoc {
    private long timestamp;
    private String payload;
    private String errorType;
    @Nullable private String erroredRowKey;
    @Nullable private String errorMessage;
    @Nullable private String stacktrace;

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getErrorType() {
        return errorType;
    }

    public void setErrorType(String errorType) {
        this.errorType = errorType;
    }


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

    public String getRowKey(){
        long reversedTimeStamp = Long.MAX_VALUE - this.timestamp;
        String rowKey = "PI#dataflow#ERROR#" + errorType + "#" + reversedTimeStamp;
        return rowKey;
    }
}
