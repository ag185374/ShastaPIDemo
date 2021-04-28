package com.example.shastadataflow.common;

import avro.shaded.com.google.common.base.Throwables;
import com.example.shastadataflow.FailedDocuments;
import com.google.api.client.json.JsonFactory;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.extensions.gcp.util.Transport;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryInsertError;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.DateTimeZone;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

public class BigqueryConverter {

    private static final JsonFactory JSON_FACTORY = Transport.getJsonFactory();

    public static class PubsubMessageToTableRow extends DoFn<PubsubMessage, TableRow> {
        /** The tag for the main output of the json transformation. */
        private TupleTag<TableRow> successTag;

        public PubsubMessageToTableRow setFailureTag(TupleTag<FailedDocuments> failureTag) {
            this.failureTag = failureTag;
            return this;
        }

        /** The tag for the dead-letter output of the json to table row transform. */
        private TupleTag<FailedDocuments> failureTag;

        public PubsubMessageToTableRow setSuccessTag(TupleTag<TableRow> successTag){
            this.successTag = successTag;
            return this;
        }

        @ProcessElement
        public void processElement(@Element PubsubMessage message, @Timestamp Instant ts, MultiOutputReceiver out){
            String payload = new String(message.getPayload(), StandardCharsets.UTF_8);
            FailedDocuments failedDocuments = new FailedDocuments();
            // Parse the JSON into a {TableRow} object.
            // try-with-resources
            try {
                TableRow row = convertJsonToTableRow(payload);
                System.out.println("newBOH ******************** " + row.toString());
                out.get(successTag).output(row);
            } catch (Exception e) {
                failedDocuments.setPayload(payload);
                failedDocuments.setErrorMessage(e.getMessage());
                failedDocuments.setStacktrace(Throwables.getStackTraceAsString(e));
                System.out.println("Document failed parsing");
                out.get(failureTag).output(failedDocuments);
            }
        }
    }

    public static TableRow convertJsonToTableRow(String json) {
        TableRow row;
        // Parse the JSON into a {TableRow} object.
        try (InputStream inputStream = new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8))) {
            row = TableRowJsonCoder.of().decode(inputStream, Coder.Context.OUTER);
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize json to table row: " + json, e);
        }
        return row;
    }

    /**
     * The {FailedPubsubMessageToTableRowFn} converts {PubsubMessage} objects which have
     * failed processing into {TableRow} objects which can be output to a dead-letter table.
     */
    public static class FailedPubsubMessageToTableRowFn extends DoFn<FailedDocuments, TableRow> {

        private static final DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");

        @ProcessElement
        public void processElement(@Element FailedDocuments failedDoc, @Timestamp Instant ts, OutputReceiver<TableRow> out) {

            // Format the timestamp for insertion
            String timestamp = TIMESTAMP_FORMATTER.print(ts.toDateTime(DateTimeZone.UTC));

            // Build the table row
            final TableRow failedRow =
                    new TableRow()
                            .set("timestamp", timestamp)
                            .set("errorMessage", failedDoc.getErrorMessage())
                            .set("stacktrace", failedDoc.getStacktrace());

            // Only set the payload if it's populated on the message.
            if (failedDoc.getPayload() != null) {
                failedRow.set("payload", failedDoc.getPayload());
            }

            out.output(failedRow);
        }
    }


    public static FailedDocuments wrapBigQueryInsertError(BigQueryInsertError insertError) {
        try {

            String rowPayload = JSON_FACTORY.toString(insertError.getRow());
            String errorMessage = JSON_FACTORY.toString(insertError.getError());

            FailedDocuments faildDocument = new FailedDocuments();
            faildDocument.setPayload(rowPayload);
            faildDocument.setErrorMessage(errorMessage);
            return faildDocument;

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


}
