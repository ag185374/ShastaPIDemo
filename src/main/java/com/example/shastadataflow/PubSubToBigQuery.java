package com.example.shastadataflow;

import com.example.shastadataflow.POJO.BigqueryFailedDoc;
import com.example.shastadataflow.common.BigqueryConverter.FailedPubsubMessageToTableRowFn;
import com.example.shastadataflow.common.BigqueryConverter.PubsubMessageToTableRow;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryInsertError;
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.*;

import static com.example.shastadataflow.common.BigqueryConverter.wrapBigQueryInsertError;

public class PubSubToBigQuery {

    /** The tag for the main output of the json transformation. */
    public static final TupleTag<TableRow> TRANSFORM_OUT = new TupleTag<TableRow>() {};

    /** The tag for the dead-letter output of the json to table row transform. */
    public static final TupleTag<BigqueryFailedDoc> TRANSFORM_FAILED =new TupleTag<BigqueryFailedDoc>() {};

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
                            .of(new PubsubMessageToTableRow().setSuccessTag(TRANSFORM_OUT).setFailureTag(TRANSFORM_FAILED))
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
        PCollection<BigqueryFailedDoc> failedInserts = writeResult
                        .getFailedInsertsWithErr()
                        .apply("WrapInsertionErrors",
                                MapElements.into(TypeDescriptor.of(BigqueryFailedDoc.class))
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

    /**
     * The {WritePubsubMessageErrors} class is a transform which can be used to write messages
     * which failed processing to an error records table. Each record is saved to the error table is
     * enriched with the timestamp of that record and the details of the error including an error
     * message and stacktrace for debugging.
     */
    public static class WritePubsubMessageErrors extends PTransform<PCollection<BigqueryFailedDoc>, WriteResult> {
        private final TableReference tableSpec = new TableReference()
                        .setProjectId("ret-shasta-cug01-dev")
                        .setDatasetId("ItemDocuments")
                        .setTableId("itemBOH_error_records");

        @Override
        public WriteResult expand(PCollection<BigqueryFailedDoc> failedRecords) {

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
}
