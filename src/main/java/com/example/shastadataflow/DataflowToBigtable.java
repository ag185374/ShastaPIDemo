package com.example.shastadataflow;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.bigtable.repackaged.com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.bigtable.repackaged.com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.bigtable.repackaged.com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.bigtable.repackaged.com.google.cloud.bigtable.data.v2.BigtableDataSettings;
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
    private static String tableId = "dataflow";


    public static void main(String[] args) throws IOException {
        // [START apache_beam_create_pipeline]
        BigtableOptions bigtableOptions = PipelineOptionsFactory.fromArgs(args).withValidation().as(BigtableOptions.class);
        PipelineOptions pipelineOptions = PipelineOptionsFactory.fromArgs(args).withValidation().as(PipelineOptions.class);
        Pipeline pipeline = Pipeline.create(pipelineOptions);
        // [END apache_beam_create_pipeline]


        // Creates the settings to configure a bigtable data client.
        BigtableDataSettings settings =
                BigtableDataSettings.newBuilder().setProjectId(bigtableOptions.getBigtableProjectId())
                        .setInstanceId(bigtableOptions.getBigtableInstanceId()).build();
        // Creates a bigtable data client.
        dataClient = BigtableDataClient.create(settings);
        // Creates the settings to configure a bigtable table admin client.
                BigtableTableAdminSettings adminSettings =
                        BigtableTableAdminSettings.newBuilder()
                                .setProjectId(bigtableOptions.getBigtableProjectId())
                                .setInstanceId(bigtableOptions.getBigtableInstanceId())
                                .build();

        // Creates a bigtable table admin client.
        adminClient = BigtableTableAdminClient.create(adminSettings);

        // [START bigtable_beam_helloworld_write_config]
        CloudBigtableTableConfiguration bigtableTableConfig =
                new CloudBigtableTableConfiguration.Builder()
                        .withProjectId(bigtableOptions.getBigtableProjectId())
                        .withInstanceId(bigtableOptions.getBigtableInstanceId())
                        .withTableId(bigtableOptions.getBigtableTableId())
                        .build();


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
         * Step #2: Write Pubsub messages into bigtable
         */
        PCollection<String> rowKeys =
                messages.apply("WritePubsubToBigtable", ParDo.of(new PubsubToBigtable()));

        /*
         * Step #3: Calculate the count sum for written rows
         */
        PCollection<String>  inventorySum=
                rowKeys.apply("WriteTableRowsToBigTable", ParDo.of(new CalculateSumFromBigtable()));

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

    public interface BigtableOptions extends DataflowPipelineOptions {
        @Description("The Bigtable project ID, this can be different than your Dataflow project")
        @Default.String("ret-shasta-cug01-dev")
        String getBigtableProjectId();

        void setBigtableProjectId(String bigtableProjectId);

        @Description("The Bigtable instance ID")
        @Default.String("pi-bigtable")
        String getBigtableInstanceId();

        void setBigtableInstanceId(String bigtableInstanceId);

        @Description("The Bigtable table ID in the instance.")
        @Default.String("pi-dataflow-inventory")
        String getBigtableTableId();

        void setBigtableTableId(String bigtableTableId);
    }

    static class PubsubToBigtable extends DoFn<PubsubMessage, String> {
        @ProcessElement
        public void processElement(@Element PubsubMessage message, OutputReceiver<String> out) throws JsonProcessingException {
            String payload = new String(message.getPayload(), StandardCharsets.UTF_8);
            ObjectMapper mapper = new ObjectMapper();
            Inventory inventory = mapper.readValue(payload, Inventory.class);
            String rowKey = "Dataflow#Count#Dept#"+inventory.documentId+"#UPC#"+inventory.UPC+"#ItemCode#"+inventory.itemCode;

            RowMutation rowMutation =
                    RowMutation.create(tableId, rowKey)
                            .setCell("cf-meta", "count",inventory.count)
                            .setCell("cf-meta", "payload", payload)
                            .setCell("cf-meta", "createdDate", inventory.effectiveDate);
            dataClient.mutateRow(rowMutation);
            out.output(rowKey);

            // Use Bigtable IO connector
//            Put row = new Put(Bytes.toBytes(rowKey));
//            row.addColumn(Bytes.toBytes("cf-meta"),Bytes.toBytes("payload"), Bytes.toBytes(payload));
//            row.addColumn(Bytes.toBytes("cf-meta"),Bytes.toBytes("count"), Bytes.toBytes(inventory.count));
//            row.addColumn(Bytes.toBytes("cf-meta"),Bytes.toBytes("createdDate"), Bytes.toBytes(inventory.effectiveDate));
        }
    }


    static class CalculateSumFromBigtable extends DoFn<String, String> {
        @ProcessElement
        public void processElement(@Element String rowKey, OutputReceiver<String> out) throws JsonProcessingException {
            Row btRow = dataClient.readRow(tableId, rowKey);
            System.out.println("rowKey ******************** " + rowKey);
            int totalCount = 0;
            if (btRow != null){
                List<RowCell> cells  = btRow.getCells("cf-meta","count");
                for (RowCell cell : cells) {
                    int curCount = Integer.parseInt(cell.getValue().toStringUtf8());
                    System.out.println("curCount ******************** " + curCount);
                    System.out.println("timeStamp ******************** " + cell.getTimestamp());
                    totalCount += curCount;
                }
            }

            String[] itemKeys = rowKey.split("#");
            String documentId = itemKeys[3];
            String UPC = itemKeys[5];
            String itemCode = itemKeys[7];
            InventorySum inventorySum = new InventorySum(itemCode,UPC,documentId,String.valueOf(totalCount));
            ObjectMapper mapper = new ObjectMapper();
            String json = mapper.writeValueAsString(inventorySum);
            out.output(json);
        }
    }

}
