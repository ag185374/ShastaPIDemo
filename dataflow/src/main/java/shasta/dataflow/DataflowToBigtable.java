package shasta.dataflow;


import com.google.cloud.bigtable.beam.CloudBigtableIO;
import com.google.cloud.bigtable.beam.CloudBigtableTableConfiguration;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.services.bigquery.model.TableRow;
import com.google.common.collect.ImmutableList;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Map;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryInsertError;
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageWithAttributesCoder;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    
    convertedTableRows.apply(CloudBigtableIO.writeToTable(bigtableTableConfig));


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
//		String data = payload.getPayload().toString();
//	    ObjectMapper mapper = new ObjectMapper();
//	    JsonNode dataObj;
//		dataObj = mapper.readTree(data);
//	    String UPC = dataObj.get("UPC").asText();
//	    String itemCode = dataObj.get("itemCode").asText();
//	    String count = dataObj.get("count").asText();
//	    String rowKey = "Dataflow#Count#Dept#6666#UPC#"+UPC+"#ItemCode#"+itemCode;
	    String rowKey = "Dataflow#Count#Dept#6666#UPC#000001#ItemCode#8400";

//	    Iterator<Map.Entry<String,JsonNode>> it = dataObj.fields();
	    Put row = new Put(Bytes.toBytes(rowKey));
//	    while (it.hasNext()) {
//	    	Map.Entry<String,JsonNode> cur = it.next();
//	    	row.addColumn(Bytes.toBytes("cf-meta"),Bytes.toBytes(cur.getKey()), Bytes.toBytes(cur.getValue().asText()));
//	    }
    	row.addColumn(Bytes.toBytes("cf-meta"),Bytes.toBytes("count"), Bytes.toBytes("20"));

	    // Use OutputReceiver.output to emit the output element.
	    out.output(row);
	  }
    
  }
  
  
}
