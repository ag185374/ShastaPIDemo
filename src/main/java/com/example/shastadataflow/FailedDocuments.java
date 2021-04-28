package com.example.shastadataflow;

import org.apache.avro.reflect.Nullable;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

@DefaultCoder(AvroCoder.class)
public class FailedDocuments {

    private String payload;
    @Nullable private String errorMessage;
    @Nullable private String stacktrace;



    public String getPayload() {
        return payload;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public String getStacktrace() {
        return stacktrace;
    }

    public FailedDocuments setPayload(String payload) {
        this.payload = payload;
        return this;
    }
    public FailedDocuments setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
        return this;
    }

    public FailedDocuments setStacktrace(String stacktrace) {
        this.stacktrace = stacktrace;
        return this;
    }

}
