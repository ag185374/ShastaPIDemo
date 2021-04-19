package com.example.shastadataflow;

import avro.shaded.com.google.common.base.Throwables;
import com.google.api.client.json.JsonFactory;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.extensions.gcp.util.Transport;
import org.apache.beam.sdk.io.gcp.bigquery.*;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.*;
import org.joda.time.DateTimeZone;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

public class PubSubToBigQuery {

    private static final JsonFactory JSON_FACTORY = Transport.getJsonFactory();

    /** The tag for the main output of the json transformation. */
    public static final TupleTag<TableRow> TRANSFORM_OUT = new TupleTag<TableRow>() {};

    /** The tag for the dead-letter output of the json to table row transform. */
    public static final TupleTag<FailedDocuments> TRANSFORM_FAILED =new TupleTag<FailedDocuments>() {};

    private static String inputSubscription = "projects/ret-shasta-cug01-dev/subscriptions/Shasta-PI-outbound-sub";

    public static void main(String[] args){
        // [START apache_beam_create_pipeline]
        PipelineOptions pipelineOptions = PipelineOptionsFactory.fromArgs(args).withValidation().as(PipelineOptions.class);
        Pipeline pipeline = Pipeline.create(pipelineOptions);
        // [END apache_beam_create_pipeline]

        TableReference tableSpec =
                new TableReference()
                        .setProjectId("ret-shasta-cug01-dev")
                        .setDatasetId("ItemDocuments")
                        .setTableId("itemBOH");
        /*
         * Step #1: Read messages in from Pub/Sub Subscription
         */
        PCollection<PubsubMessage> messages = null;
        messages =
                pipeline.apply(
                        "ReadPubSubSubscription",
                        PubsubIO.readMessagesWithAttributes()
                                .fromSubscription(inputSubscription));

        /*
         * Step #2: Transform the PubsubMessages into TableRows
         */
        PCollectionTuple convertedTableRows =
                messages
                        .apply("ConvertMessageToTableRow", ParDo
                            .of(new PubsubMessageToTableRow())
                            .withOutputTags(TRANSFORM_OUT, TupleTagList.of(TRANSFORM_FAILED))
                        );

        /*
         * Step #3: Write the successful records out to BigQuery
         */
        WriteResult writeResult =
                convertedTableRows
                        .get(TRANSFORM_OUT)
                        .apply(
                                "WriteSuccessfulRecords",
                                BigQueryIO.writeTableRows()
                                        .withoutValidation()
                                        .withCreateDisposition(CreateDisposition.CREATE_NEVER)
                                        .withWriteDisposition(WriteDisposition.WRITE_APPEND)
                                        .withExtendedErrorInfo()
                                        .withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS)
                                        .withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors())
                                        .to(tableSpec));

        /*
         * Step 3 Contd.
         * Elements that failed inserts into BigQuery are extracted and converted to FailedDocuments
         */
        PCollection<FailedDocuments> failedInserts = writeResult
                        .getFailedInsertsWithErr()
                        .apply("WrapInsertionErrors",
                                MapElements.into(TypeDescriptor.of(FailedDocuments.class))
                                        .via((BigQueryInsertError e) -> wrapBigQueryInsertError(e)));


        /*
         * Step #4: Write records that failed table row transformation
         * or conversion out to BigQuery error table.
         */
        convertedTableRows
                .get(TRANSFORM_FAILED)
                .apply("WriteFailedRecords", new WritePubsubMessageErrors());

        /*
         * Step #4: Insert records that failed insert into error table
         */
        failedInserts.apply("WriteFailedRecords", new WritePubsubMessageErrors());


        pipeline.run();

    }

    static FailedDocuments wrapBigQueryInsertError(BigQueryInsertError insertError) {

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

    static class PubsubMessageToTableRow extends DoFn<PubsubMessage, TableRow> {

        @ProcessElement
        public void processElement(@Element PubsubMessage message, MultiOutputReceiver out){
            String payload = new String(message.getPayload(), StandardCharsets.UTF_8);
            FailedDocuments failedDocuments = new FailedDocuments();
            // Parse the JSON into a {TableRow} object.
            // try-with-resources
            try {
                TableRow row = convertJsonToTableRow(payload);
                out.get(TRANSFORM_OUT).output(row);
            } catch (Exception e) {
                failedDocuments.setPayload(payload);
                failedDocuments.setErrorMessage(e.getMessage());
                failedDocuments.setStacktrace(Throwables.getStackTraceAsString(e));
                out.get(TRANSFORM_FAILED).output(failedDocuments);
            }
        }
    }

    /**
     * The {WritePubsubMessageErrors} class is a transform which can be used to write messages
     * which failed processing to an error records table. Each record is saved to the error table is
     * enriched with the timestamp of that record and the details of the error including an error
     * message and stacktrace for debugging.
     */
    public static class WritePubsubMessageErrors extends PTransform<PCollection<FailedDocuments>, WriteResult> {
        private final TableReference tableSpec = new TableReference()
                        .setProjectId("ret-shasta-cug01-dev")
                        .setDatasetId("ItemDocuments")
                        .setTableId("itemBOH_error_records");

        @Override
        public WriteResult expand(PCollection<FailedDocuments> failedRecords) {

            return failedRecords
                    .apply("FailedRecordToTableRow", ParDo.of(new FailedPubsubMessageToTableRowFn()))
                    .apply(
                            "WriteFailedRecordsToBigQuery",
                            BigQueryIO.writeTableRows()
                                    .to(tableSpec)
                                    .withCreateDisposition(CreateDisposition.CREATE_NEVER)
                                    .withWriteDisposition(WriteDisposition.WRITE_APPEND));
        }
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
}
