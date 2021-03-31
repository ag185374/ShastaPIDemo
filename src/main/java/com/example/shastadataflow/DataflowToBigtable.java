package com.example.shastadataflow;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.bigtable.beam.CloudBigtableIO;
import com.google.cloud.bigtable.beam.CloudBigtableTableConfiguration;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

public class DataflowToBigtable {
    private static String inputSubscription = "projects/ret-shasta-cug01-dev/subscriptions/Shasta-PI-Inbound-sub";

    public static void main(String[] args) {
        // [START bigtable_beam_create_pipeline]
        BigtableOptions bigtableOptions = PipelineOptionsFactory.create().as(BigtableOptions.class);

        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(PipelineOptions.class);
        Pipeline pipeline = Pipeline.create(options);

        // [END bigtable_beam_create_pipeline]

        /*
         * Step #1: Read messages in from Pub/Sub
         * Either from a Subscription or Topic
         */
        PCollection<PubsubMessage> messages = null;
        messages =
                pipeline.apply(
                        "ReadPubSubSubscription",
                        PubsubIO.readMessagesWithAttributes()
                                .fromSubscription(inputSubscription));


        PCollection<Mutation> convertedTableRows =
                messages
                        /*
                         * Step #2: Transform the PubsubMessages into TableRows
                         */
                        .apply("ConvertMessageToBigtableRow", ParDo.of(new PubsubMessageToTableRow()));


        // [START bigtable_beam_helloworld_write_config]
        CloudBigtableTableConfiguration bigtableTableConfig =
                new CloudBigtableTableConfiguration.Builder()
                        .withProjectId(bigtableOptions.getBigtableProjectId())
                        .withInstanceId(bigtableOptions.getBigtableInstanceId())
                        .withTableId(bigtableOptions.getBigtableTableId())
                        .build();
        // [END bigtable_beam_helloworld_write_config]

        convertedTableRows.apply(CloudBigtableIO.writeToTable(bigtableTableConfig)); //step 5: writing to bt

        //step 6: push to topic

        //

        pipeline.run().waitUntilFinish();
    }

    // [START bigtable_beam_helloworld_options]
    public interface BigtableOptions extends DataflowPipelineOptions {
        @Description("The Bigtable project ID, this can be different than your Dataflow project")
        @Default.String("ret-shasta-cug01-dev")
        String getBigtableProjectId();

        void setBigtableProjectId(String bigtableProjectId);

        @Description("The Bigtable instance ID")
        @Default.String("bigtable-dataflow")
        String getBigtableInstanceId();

        void setBigtableInstanceId(String bigtableInstanceId);

        @Description("The Bigtable table ID in the instance.")
        @Default.String("dataflow")
        String getBigtableTableId();

        void setBigtableTableId(String bigtableTableId);
    }

    static class PubsubMessageToTableRow extends DoFn<PubsubMessage, Mutation> {


        @ProcessElement
        public void processElement(@Element PubsubMessage payload, OutputReceiver<Mutation> out) throws JsonMappingException, JsonProcessingException {
            String rowKey = "Dataflow#Count#Dept#6666#UPC#000001#ItemCode#8400";
            Put row = new Put(Bytes.toBytes(rowKey));
            row.addColumn(Bytes.toBytes("cf-meta"),Bytes.toBytes("count"), Bytes.toBytes("20"));

            //pull from topic
            final Logger LOG = LoggerFactory.getLogger(PubsubMessageToTableRow.class);
            String json = new String(payload.getPayload(), StandardCharsets.UTF_8);
            ObjectMapper mapper = new ObjectMapper();
            Inventory inventory = mapper.readValue(json, Inventory.class);
            row.addColumn(Bytes.toBytes("cf-meta"),Bytes.toBytes("count"), Bytes.toBytes(json));
            out.output(row);
        }

    }
}
