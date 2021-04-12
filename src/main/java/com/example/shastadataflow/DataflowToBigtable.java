package com.example.shastadataflow;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.bigtable.repackaged.com.google.api.gax.rpc.ServerStream;
import com.google.bigtable.repackaged.com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.bigtable.repackaged.com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.bigtable.repackaged.com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.bigtable.repackaged.com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.bigtable.repackaged.com.google.cloud.bigtable.data.v2.models.*;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sorter.BufferedExternalSorter;
import org.apache.beam.sdk.extensions.sorter.SortValues;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class DataflowToBigtable {

    private static String inputSubscription = "projects/ret-shasta-cug01-dev/subscriptions/Shasta-PI-Inbound-Test-sub";
    private static String outputTopic = "projects/ret-shasta-cug01-dev/topics/Shasta-PI-outbound";
    private static BigtableDataClient dataClient;
    private static BigtableTableAdminClient adminClient;
    private static String tableId = "pi-dataflow-inventory";


    public static void main(String[] args) throws IOException {
        // [START apache_beam_create_pipeline]
//        BigtableOptions bigtableOptions = PipelineOptionsFactory.create().as(BigtableOptions.class);
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
         * Step #2: Group Pubsub messages into 5s fixed windows
         */
        PCollection<PubsubMessage> fixedWindowedMessages = messages.apply("WindowPubsubMessage",
                Window.<PubsubMessage>into(FixedWindows.of(Duration.standardSeconds(30)))
        );

        /*
         * Step #3: Convert Pubsub messages into 5s KV<rowKey, KV<timestamp, String>>
         */
        PCollection<KV<String, KV<String, String>>> keyedPubsubMessage =
                fixedWindowedMessages.apply("WritePubsubToBigtable", ParDo.of(new GroupIntoKV()));


        /*
         * Step #4: Group  KV<rowKey, KV<timestamp, String>> according to rowKey
         */
        PCollection<KV<String, Iterable<KV<String, String>>>> groupedPubsubMessages =
                keyedPubsubMessage.apply(GroupByKey.<String, KV<String, String>>create());

        /*
         * Step #4: Sort  <KV<String, Iterable<KV<Long, String>>>> according to long timestamp
         */
        PCollection<KV<String, Iterable<KV<String, String>>>> groupedAndSorted =
                groupedPubsubMessages.apply(SortValues.<String, String, String>create(BufferedExternalSorter.options()));


        /*
         * Step #2: Write Pubsub messages and total count into bigtable
         */
        PCollection<String> inventorySum =
                groupedAndSorted.apply("WritePubsubToBigtable", ParDo.of(new PubsubToBigtable()));

        /*
         * Step #4: Publish the message to Pubsub
         */
        inventorySum.apply("Write PubSub Events", PubsubIO.writeStrings().to(outputTopic));

        pipeline.run();
    }

    static class PubsubToBigtable extends DoFn<KV<String, Iterable<KV<String, String>>>, String> {
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
        public void processElement(@Element KV<String, Iterable<KV<String, String>>> kv, OutputReceiver<String> out){
            String rowKey = kv.getKey();
            Iterable<KV<String, String>> messages = kv.getValue();
            int totalCount = 0;

            if (rowKey.equals("PI#dataflow#ERROR")){
                for (KV<String, String> message : messages){
                    String payload = message.getValue();
                    long reversedTimeStamp = Long.MAX_VALUE - Long.parseLong(message.getKey());
                    String rowkeyStamped = rowKey + "#" + reversedTimeStamp;
                    RowMutation rowMutation = RowMutation.create(tableId, rowkeyStamped);
                    rowMutation.setCell("cf-meta", "payload", payload);
                    dataClient.mutateRow(rowMutation);
                }
                out.output("failure");
            }
            else{
                for (KV<String, String> message : messages){
                    String payload = message.getValue();
                    ObjectMapper mapper = new ObjectMapper();
                    Inventory inventory = null;
                    long reversedTimeStamp = Long.MAX_VALUE - Long.parseLong(message.getKey());
                    String rowkeyStamped = rowKey + "#" + reversedTimeStamp;

                    try {
                        inventory = mapper.readValue(payload, Inventory.class);
                        Filters.Filter filter = Filters.FILTERS.limit().cellsPerColumn(1);
                        System.out.println("rowKey ******************** " + rowKey);
                        RowMutation rowMutation = RowMutation.create(tableId, rowkeyStamped);
                        if (inventory.countOverride != null) {
                            totalCount = Integer.parseInt(inventory.countOverride);
                            rowMutation.setCell("cf-meta", "count#override", String.valueOf(totalCount));
                        } else {
                            Query query =  Query.create(tableId).prefix(rowKey);
                            ServerStream<Row> rows = dataClient.readRows(query);
                            for (Row row: rows){
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
                                .setCell("cf-meta", "payload", payload)
                                .setCell("cf-meta", "createdDate", inventory.effectiveDate);

                        dataClient.mutateRow(rowMutation);

                        // Construct pubsub message
                        InventorySum inventorySum = new InventorySum(inventory.itemCode, inventory.UPC, inventory.documentId, String.valueOf(totalCount));
                        String json = null;
                        json = mapper.writeValueAsString(inventorySum);
                        out.output(json);
                    } catch (JsonProcessingException e) {
                        System.out.print("failed when parsing json object: " + payload);
                        out.output("failure");
                    }
                }
            }
        }
    }

    static class GroupIntoKV extends DoFn<PubsubMessage, KV<String, KV<String, String>>> {
        @ProcessElement
        public void processElement(@Element PubsubMessage message, @Timestamp Instant timestamp, OutputReceiver<KV<String, KV<String, String>>> out){
            String payload = new String(message.getPayload(), StandardCharsets.UTF_8);
            ObjectMapper mapper = new ObjectMapper();
            Inventory inventory = null;
            try {
                inventory = mapper.readValue(payload, Inventory.class);
                String rowKey = "dataflow#count#retail#" + inventory.retail + "#store#" + inventory.store + "#upc#" + inventory.UPC + "#ItemCode#" + inventory.itemCode;
                System.out.println("rowKey ******************** " + rowKey);
                out.output(KV.of(rowKey, KV.of(String.valueOf(timestamp.getMillis()), payload)));
            } catch (JsonProcessingException e) {
                System.out.print("failed when parsing json object: " + payload);
                out.output(KV.of("PI#dataflow#ERROR", KV.of(String.valueOf(timestamp.getMillis()), payload)));
            }
        }
    }

    static class ReadSumFromBigtable extends DoFn<String, String> {
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
        public void processElement(@Element String rowKey, OutputReceiver<String> out) throws JsonProcessingException {
            Row btRow = dataClient.readRow(tableId, rowKey);
            System.out.println("rowKey ******************** " + rowKey);

            String[] itemKeys = rowKey.split("#");
            String documentId = itemKeys[3];
            String UPC = itemKeys[5];
            String itemCode = itemKeys[7];
            InventorySum inventorySum = new InventorySum(itemCode,UPC,documentId,String.valueOf(1));
            ObjectMapper mapper = new ObjectMapper();
            String json = mapper.writeValueAsString(inventorySum);
            out.output(json);
        }
    }

}
