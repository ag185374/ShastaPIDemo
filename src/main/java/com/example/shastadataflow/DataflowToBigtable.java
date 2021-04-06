package com.example.shastadataflow;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.bigtable.repackaged.com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.bigtable.repackaged.com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.bigtable.repackaged.com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.bigtable.repackaged.com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.bigtable.repackaged.com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.bigtable.repackaged.com.google.cloud.bigtable.data.v2.models.Row;
import com.google.bigtable.repackaged.com.google.cloud.bigtable.data.v2.models.RowCell;
import com.google.bigtable.repackaged.com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.cloud.bigtable.beam.CloudBigtableIO;
import com.google.cloud.bigtable.beam.CloudBigtableScanConfiguration;
import com.google.cloud.bigtable.beam.CloudBigtableTableConfiguration;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class DataflowToBigtable {

    private static String inputSubscription = "projects/ret-shasta-cug01-dev/subscriptions/Shasta-PI-Inbound-sub";
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


//        // Creates the settings to configure a bigtable data client.
//        BigtableDataSettings settings =
//                BigtableDataSettings.newBuilder().setProjectId(bigtableOptions.getBigtableProjectId())
//                        .setInstanceId(bigtableOptions.getBigtableInstanceId()).build();
//        // Creates a bigtable data client.
//        dataClient = BigtableDataClient.create(settings);
//        // Creates the settings to configure a bigtable table admin client.
//                BigtableTableAdminSettings adminSettings =
//                        BigtableTableAdminSettings.newBuilder()
//                                .setProjectId(bigtableOptions.getBigtableProjectId())
//                                .setInstanceId(bigtableOptions.getBigtableInstanceId())
//                                .build();
//
//        // Creates a bigtable table admin client.
//        adminClient = BigtableTableAdminClient.create(adminSettings);
//
//        // [START bigtable_beam_helloworld_write_config]
//        CloudBigtableTableConfiguration bigtableTableConfig =
//                new CloudBigtableTableConfiguration.Builder()
//                        .withProjectId(bigtableOptions.getBigtableProjectId())
//                        .withInstanceId(bigtableOptions.getBigtableInstanceId())
//                        .withTableId(bigtableOptions.getBigtableTableId())
//                        .build();
//

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
         * Step #2: Write Pubsub messages and total count into bigtable
         */
        PCollection<String> inventorySum =
                messages.apply("WritePubsubToBigtable", ParDo.of(new PubsubToBigtable()));

//        /*
//         * Step #3: Calculate the count sum for written rows
//         */
//        PCollection<String>  inventorySum=
//                rowKeys.apply("CalculateSumFromBigtable", ParDo.of(new CalculateSumFromBigtable()));

        /*
         * Step #4: Publish the message to Pubsub
         */
        inventorySum.apply("Write PubSub Events", PubsubIO.writeStrings().to(outputTopic));

        pipeline.run();
    }

    public static void pullDataFromBigTable(Pipeline pipeline, CloudBigtableScanConfiguration bigtableTableConfig) {
        pipeline.apply(Read.from(CloudBigtableIO.read(bigtableTableConfig)))
                .apply(
                        ParDo.of(
                                new DoFn<Result, Void>() {
                                    @ProcessElement
                                    public void processElement(@Element Result row, OutputReceiver<Void> out) {
                                        System.out.println(">>>>>>>>>>>" + Bytes.toString(row.getRow()));
                                        String s = Bytes.toString(row.getRow());
                                    }
                                }));

    }

//    public interface BigtableOptions extends DataflowPipelineOptions {
//        @Description("The Bigtable project ID, this can be different than your Dataflow project")
//        @Default.String("ret-shasta-cug01-dev")
//        String getBigtableProjectId();
//
//        void setBigtableProjectId(String bigtableProjectId);
//
//        @Description("The Bigtable instance ID")
//        @Default.String("pi-bigtable")
//        String getBigtableInstanceId();
//
//        void setBigtableInstanceId(String bigtableInstanceId);
//
//        @Description("The Bigtable table ID in the instance.")
//        @Default.String("pi-dataflow-inventory")
//        String getBigtableTableId();
//
//        void setBigtableTableId(String bigtableTableId);
//    }

    static class PubsubToBigtable extends DoFn<PubsubMessage, String> {
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
        public void processElement(@Element PubsubMessage message, OutputReceiver<String> out) throws JsonProcessingException {
            String payload = new String(message.getPayload(), StandardCharsets.UTF_8);
            ObjectMapper mapper = new ObjectMapper();
            Inventory inventory = mapper.readValue(payload, Inventory.class);
            String rowKey = "dataflow#count#retail#"+inventory.retail+"#store#"+inventory.store+"#upc#"+inventory.UPC+"#ItemCode#"+inventory.itemCode;

            Filters.Filter filter = Filters.FILTERS.limit().cellsPerColumn(1);
            System.out.println("rowKey ******************** " + rowKey);
            int totalCount = 0;
            RowMutation rowMutation = RowMutation.create(tableId, rowKey);
            if (inventory.countOverride != null){
                totalCount = Integer.parseInt(inventory.countOverride);
                rowMutation.setCell("cf-meta", "count#override", String.valueOf(totalCount));
            }
            else{
                Row btRow = dataClient.readRow(tableId, rowKey,filter);
                if (btRow != null){
                    List<RowCell> cell  = btRow.getCells("cf-meta","BOH");
                    if (cell.size() != 0){
                        totalCount = Integer.parseInt(cell.get(0).getValue().toStringUtf8());
                        System.out.println("timeStamp ******************** " + cell.get(0).getTimestamp());
                    }
                }
            }

            if (inventory.adjustment != null){
                totalCount += Integer.parseInt(inventory.adjustment);
                rowMutation.setCell("cf-meta", "adjustment",String.valueOf(inventory.adjustment));
            }
            rowMutation
                .setCell("cf-meta","BOH",String.valueOf(totalCount))
                .setCell("cf-meta", "payload", payload)
                .setCell("cf-meta", "createdDate", inventory.effectiveDate);
            dataClient.mutateRow(rowMutation);

            // Construct pubsub message
            InventorySum inventorySum = new InventorySum(inventory.itemCode,inventory.UPC,inventory.documentId,String.valueOf(totalCount));
            String json = mapper.writeValueAsString(inventorySum);
            out.output(json);
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
