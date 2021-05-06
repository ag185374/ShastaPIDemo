package com.example.shastadataflow;

import com.example.shastadataflow.POJO.BigtableFailedDoc;
import com.example.shastadataflow.POJO.BigtableInventory;
import com.example.shastadataflow.common.BigtableConverter;
import com.example.shastadataflow.common.BigtableConverter.FailedPubsubMessageToBigTableRowFn;
import com.example.shastadataflow.common.BigtableConverter.ParseDocumentFn;
import com.example.shastadataflow.common.BigtableConverter.WriteTableRowFn;
import com.example.shastadataflow.common.BigtableOperation.PubsubToBigtable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sorter.BufferedExternalSorter;
import org.apache.beam.sdk.extensions.sorter.SortValues;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.*;
import org.joda.time.Duration;

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
         * Step #3: Convert Pubsub messages into  KV<rowKey, KV<timestamp, BigtableInventory>>
         */
        PCollectionTuple keyedPubsubMessage =
                fixedWindowedMessages.apply("ParsePubsubMessage", ParDo
                        .of(new ParseDocumentFn().setSuccessTag(TRANSFORM_OUT).setFailureTag(TRANSFORM_FAILED))
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
                        .of(new PubsubToBigtable().setSuccessTag(PUBSUB_OUT).setFailureTag(PUBSUB_OUT_FAILED))
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
                    .apply(GroupByKey.<String, KV<String, BigtableInventory>>create())  // <String, Iterable<KV<String, BigtableInventory>>>
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

}
