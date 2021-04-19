package com.example.shastadataflow;

import avro.shaded.com.google.common.base.Throwables;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.bigtable.repackaged.com.google.api.gax.rpc.ServerStream;
import com.google.bigtable.repackaged.com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.bigtable.repackaged.com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.bigtable.repackaged.com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.bigtable.repackaged.com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.bigtable.repackaged.com.google.cloud.bigtable.data.v2.models.*;
import com.google.bigtable.repackaged.com.google.cloud.bigtable.data.v2.models.Row;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sorter.BufferedExternalSorter;
import org.apache.beam.sdk.extensions.sorter.SortValues;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.*;
import org.joda.time.Duration;
import org.joda.time.Instant;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class DataflowToBigtable {

    private static String inputSubscription = "projects/ret-shasta-cug01-dev/subscriptions/Shasta-PI-Inbound-Test-sub";
    private static String outputTopic = "projects/ret-shasta-cug01-dev/topics/Shasta-PI-outbound";

    /** The tag for the main output of the json transformation. */
    public static final TupleTag<KV<String, KV<String, BigtableInventory>>> TRANSFORM_OUT = new TupleTag<KV<String, KV<String, BigtableInventory>>>() {};

    /** The tag for the dead-letter output of the json to table row transform. */
    public static final TupleTag<BigtableFailedDoc> TRANSFORM_FAILED =new TupleTag<BigtableFailedDoc>() {};

    /** The tag for the main output of InventorySum to json transform. */
    public static final TupleTag<String> PUBSUB_OUT = new TupleTag<String>() {};

    /** The tag for the dead-letter output of the InventorySum to json transform. */
    public static final TupleTag<BigtableFailedDoc> PUBSUB_OUT_FAILED = new TupleTag<BigtableFailedDoc>() {};

    public static void main(String[] args){
        // [START apache_beam_create_pipeline]
        PipelineOptions pipelineOptions = PipelineOptionsFactory.fromArgs(args).withValidation().as(PipelineOptions.class);
        Pipeline pipeline = Pipeline.create(pipelineOptions);
        // [END apache_beam_create_pipeline]

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
         * Step #2: Group Pubsub messages into 30s fixed windows
         */
        PCollection<PubsubMessage> fixedWindowedMessages = messages.apply("WindowPubsubMessage",
                Window.<PubsubMessage>into(FixedWindows.of(Duration.standardSeconds(30)))
        );



        /*
         * Step #3: Convert Pubsub messages into  KV<rowKey, KV<timestamp, String>>
         */
        PCollectionTuple keyedPubsubMessage =
                fixedWindowedMessages.apply("ParsePubsubMessage", ParDo
                        .of(new GroupIntoKV())
                        .withOutputTags(TRANSFORM_OUT, TupleTagList.of(TRANSFORM_FAILED))
                );


        /*
         * Step #4
         * Group KV<rowKey, KV<timestamp, String>> according to rowKey and sort according to timestamp
         */
        PCollection<KV<String, Iterable<KV<String, BigtableInventory>>>> groupedAndSorted =
                keyedPubsubMessage
                        .get(TRANSFORM_OUT)
                        .apply("GroupAndSortPubsubMessage", new GroupAndSortPubsubMessage());



        /*
         * Step #5: Write the successful records out to Bigtable
         */
        PCollectionTuple insertedDocumentMessage =
                groupedAndSorted.apply("WritePubsubToBigtable", ParDo
                        .of(new PubsubToBigtable())
                        .withOutputTags(PUBSUB_OUT, TupleTagList.of(PUBSUB_OUT_FAILED))
                );

        /*
         * Step #4 Contd.
         * Write records that failed conversion out to Bigtable error rows.
         */
        keyedPubsubMessage
                .get(TRANSFORM_FAILED)
                .apply("WriteFailedRecords", new WriteFailedPubsubMessage());

        /*
         * Step #5: Publish inserted documents as message to Pubsub
         */
        insertedDocumentMessage
                .get(PUBSUB_OUT)
                .apply("WritePubSubEvents", PubsubIO.writeStrings().to(outputTopic));
        /*
         * Step #5 Contd.
         * Write inserted documents that failed conversion into Pubsub message out to Bigtable error rows
         */
        insertedDocumentMessage
                .get(PUBSUB_OUT_FAILED)
                .apply("WritePubSubFailedEventsToBigtable", new WriteFailedPubsubMessage());

        pipeline.run();
    }

    public static class GroupAndSortPubsubMessage extends PTransform<PCollection<KV<String, KV<String, BigtableInventory>>>, PCollection<KV<String, Iterable<KV<String, BigtableInventory>>>>> {
        @Override
        public PCollection<KV<String, Iterable<KV<String, BigtableInventory>>>> expand(PCollection<KV<String, KV<String, BigtableInventory>>> input) {
            return input
                    .apply(GroupByKey.<String, KV<String, BigtableInventory>>create())
                    .apply(SortValues.<String, String, BigtableInventory>create(BufferedExternalSorter.options()));
        }
    }


        public static class WriteFailedPubsubMessage extends PTransform<PCollection<BigtableFailedDoc>, PDone> {


        @Override
        public PDone expand(PCollection<BigtableFailedDoc> failedRecords) {
           failedRecords
                    .apply("FailedRecordToBigTableRow", ParDo.of(new FailedPubsubMessageToBigTableRowFn()))
                    .apply("WriteTableRow", ParDo.of(new WriteTableRowFn()));
           return PDone.in(failedRecords.getPipeline());

        }
    }


    static class FailedPubsubMessageToBigTableRowFn extends DoFn<BigtableFailedDoc, RowMutation> {
        private static String tableId = "pi-dataflow-inventory";

        @ProcessElement
        public void processElement(@Element BigtableFailedDoc failedDoc,  OutputReceiver<RowMutation> out) {

            RowMutation rowMutation = RowMutation.create(tableId, failedDoc.getRowKey());
            rowMutation
                    .setCell("cf-meta", "payload", failedDoc.getPayload())
                    .setCell("cf-meta", "errorMessage", failedDoc.getErrorMessage())
                    .setCell("cf-meta", "stacktrace", failedDoc.getStacktrace());
            if (failedDoc.getErroredRowKey() != null){
                rowMutation.setCell("cf-meta", "erroredRowKey", failedDoc.getErroredRowKey());
            }
            out.output(rowMutation);
        }
    }

    static class WriteTableRowFn extends DoFn<RowMutation, Void> {
        private static BigtableDataClient dataClient;
        private static BigtableTableAdminClient adminClient;
        private static String tableId = "pi-dataflow-inventory";
        private static String bigtableProjectId = "ret-shasta-cug01-dev";
        private static String bigtableInstanceId = "pi-bigtable";

        @Setup
        public void initializeBigtableConnection() throws IOException {
            // Creates the settings to configure a bigtable data client.
            BigtableDataSettings settings =
                    BigtableDataSettings.newBuilder().setProjectId(bigtableProjectId)
                            .setInstanceId(bigtableInstanceId).build();
            // Creates a bigtable data client.
            dataClient = BigtableDataClient.create(settings);
            // Creates the settings to configure a bigtable table admin client.
            BigtableTableAdminSettings adminSettings =
                    BigtableTableAdminSettings.newBuilder()
                            .setProjectId(bigtableProjectId)
                            .setInstanceId(bigtableInstanceId)
                            .build();

            // Creates a bigtable table admin client.
            adminClient = BigtableTableAdminClient.create(adminSettings);
        }


        @ProcessElement
        public void processElement(@Element RowMutation rowMutation){
            dataClient.mutateRow(rowMutation);
        }
    }

    static class PubsubToBigtable extends DoFn<KV<String, Iterable<KV<String, BigtableInventory>>>, String> {
        private static BigtableDataClient dataClient;
        private static BigtableTableAdminClient adminClient;
        private static String tableId = "pi-dataflow-inventory";
        private static String bigtableProjectId = "ret-shasta-cug01-dev";
        private static String bigtableInstanceId = "pi-bigtable";

        @Setup
        public void initializeBigtableConnection() throws IOException {
            // Creates the settings to configure a bigtable data client.
            BigtableDataSettings settings =
                    BigtableDataSettings.newBuilder().setProjectId(bigtableProjectId)
                            .setInstanceId(bigtableInstanceId).build();
            // Creates a bigtable data client.
            dataClient = BigtableDataClient.create(settings);
            // Creates the settings to configure a bigtable table admin client.
            BigtableTableAdminSettings adminSettings =
                    BigtableTableAdminSettings.newBuilder()
                            .setProjectId(bigtableProjectId)
                            .setInstanceId(bigtableInstanceId)
                            .build();

            // Creates a bigtable table admin client.
            adminClient = BigtableTableAdminClient.create(adminSettings);
        }

        @ProcessElement
        public void processElement(@Element KV<String, Iterable<KV<String, BigtableInventory>>> kv, MultiOutputReceiver out){
            String rowKey = kv.getKey();
            Iterable<KV<String, BigtableInventory>> messages = kv.getValue();
            int totalCount = 0;

            for (KV<String, BigtableInventory> message : messages) {
                BigtableInventory bigtableInventory = message.getValue();
                Inventory inventory = bigtableInventory.getInventory();
                long reversedTimeStamp = Long.MAX_VALUE - Long.parseLong(message.getKey());
                String rowkeyStamped = rowKey + "#" + reversedTimeStamp;
                Filters.Filter filter = Filters.FILTERS.limit().cellsPerColumn(1);
                RowMutation rowMutation = RowMutation.create(tableId, rowkeyStamped);
                if (inventory.countOverride != null) {
                    totalCount = Integer.parseInt(inventory.countOverride);
                    rowMutation.setCell("cf-meta", "count#override", String.valueOf(totalCount));
                } else {
                    Query query = Query.create(tableId).prefix(rowKey);
                    ServerStream<Row> rows = dataClient.readRows(query);
                    for (Row row : rows) {
                        List<RowCell> cell = row.getCells("cf-meta", "BOH");
                        if (cell.size() != 0) {
                            totalCount = Integer.parseInt(cell.get(0).getValue().toStringUtf8());
                            System.out.println("timeStamp ******************** " + cell.get(0).getTimestamp());
                        }
                        break;
                    }
                }

                if (inventory.adjustment != null) {
                    totalCount += Integer.parseInt(inventory.adjustment);
                    rowMutation.setCell("cf-meta", "adjustment", String.valueOf(inventory.adjustment));
                }
                rowMutation
                        .setCell("cf-meta", "BOH", String.valueOf(totalCount))
                        .setCell("cf-meta", "payload", bigtableInventory.getPayload())
                        .setCell("cf-meta", "createdDate", inventory.effectiveDate);

                dataClient.mutateRow(rowMutation);
                try {
                    // Construct pubsub message
                    ObjectMapper mapper = new ObjectMapper();
                    InventorySum inventorySum = new InventorySum(inventory.version, inventory.retail, inventory.store, inventory.itemCode, inventory.UPC, inventory.documentId, String.valueOf(totalCount));
                    String json = mapper.writeValueAsString(inventorySum);
                    out.get(PUBSUB_OUT).output(json);
                } catch (JsonProcessingException e) {
                    BigtableFailedDoc failedDocuments = new BigtableFailedDoc();
                    failedDocuments.setRowKey("PI#dataflow#ERROR#ApplicationError#" + reversedTimeStamp);
                    failedDocuments.setErroredRowKey(rowkeyStamped);
                    failedDocuments.setPayload(bigtableInventory.getPayload());
                    failedDocuments.setErrorMessage(e.getMessage());
                    failedDocuments.setStacktrace(Throwables.getStackTraceAsString(e));
                    out.get(PUBSUB_OUT_FAILED).output(failedDocuments);
                    //PI#dataflow#ERROR#ApplicationError# + columns: erroredKey, payload
                }
            }
        }
    }

    static class GroupIntoKV extends DoFn<PubsubMessage, KV<String, KV<String, BigtableInventory>>> {
        @ProcessElement
        public void processElement(@Element PubsubMessage message, @Timestamp Instant timestamp, MultiOutputReceiver out) {
            String payload = new String(message.getPayload(), StandardCharsets.UTF_8);
            ObjectMapper mapper = new ObjectMapper();
            Inventory inventory = null;
            try {
                inventory = mapper.readValue(payload, Inventory.class);
                String rowKey = "dataflow#count#retail#" + inventory.retail + "#store#" + inventory.store + "#upc#" + inventory.UPC + "#ItemCode#" + inventory.itemCode;
                BigtableInventory bigtableInventory = new BigtableInventory();
                bigtableInventory.setPayload(payload);
                bigtableInventory.setInventory(inventory);
                System.out.println("rowKey ******************** " + rowKey);
                out.get(TRANSFORM_OUT).output(KV.of(rowKey, KV.of(String.valueOf(timestamp.getMillis()), bigtableInventory)));
            } catch (JsonProcessingException e) {
                long reversedTimeStamp = Long.MAX_VALUE - timestamp.getMillis();
                BigtableFailedDoc failedDocuments = new BigtableFailedDoc();
                failedDocuments.setRowKey("PI#dataflow#ERROR#InputRequestError#" + reversedTimeStamp);
                failedDocuments.setPayload(payload);
                failedDocuments.setErrorMessage(e.getMessage());
                failedDocuments.setStacktrace(Throwables.getStackTraceAsString(e));
                System.out.println("Document failed parsing");
                out.get(TRANSFORM_FAILED).output(failedDocuments);
            }
        }
    }

}
