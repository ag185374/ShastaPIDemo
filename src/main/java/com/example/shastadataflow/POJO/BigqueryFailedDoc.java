package com.example.shastadataflow.POJO;

import org.apache.avro.reflect.Nullable;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

@DefaultCoder(AvroCoder.class)
public class BigqueryFailedDoc {

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

    public BigqueryFailedDoc setPayload(String payload) {
        this.payload = payload;
        return this;
    }
    public BigqueryFailedDoc setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
        return this;
    }

    public BigqueryFailedDoc setStacktrace(String stacktrace) {
        this.stacktrace = stacktrace;
        return this;
    }

}
