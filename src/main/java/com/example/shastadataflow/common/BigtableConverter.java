package com.example.shastadataflow.common;

import avro.shaded.com.google.common.base.Throwables;
import com.example.shastadataflow.POJO.BigtableFailedDoc;
import com.example.shastadataflow.POJO.BigtableInventory;
import com.example.shastadataflow.POJO.Inventory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class BigtableConverter {

    private static final Logger LOG = LoggerFactory.getLogger(BigtableConverter.class);

    public static class ParseDocumentFn extends DoFn<PubsubMessage, KV<String, KV<String, BigtableInventory>>> {
        /** The tag for the main output of the json transformation. */
        private TupleTag<KV<String, KV<String, BigtableInventory>>> successTag;

        /** The tag for the dead-letter output of the json to table row transform. */
        private TupleTag<BigtableFailedDoc> failureTag;

        public ParseDocumentFn setSuccessTag(TupleTag<KV<String, KV<String, BigtableInventory>>> successTag) {
            this.successTag = successTag;
            return this;
        }

        public ParseDocumentFn setFailureTag(TupleTag<BigtableFailedDoc> failureTag) {
            this.failureTag = failureTag;
            return this;
        }

        @ProcessElement
        public void processElement(@Element PubsubMessage message, @Timestamp Instant timestamp, MultiOutputReceiver out) {
            String payload = new String(message.getPayload(), StandardCharsets.UTF_8);
            ObjectMapper mapper = new ObjectMapper();
            Inventory inventory = null;
            try {
                inventory = mapper.readValue(payload, Inventory.class);
                long effectiveDateTs = inventory.getEffectiveDateMillis();
                String rowKey = inventory.getRowKey();
                BigtableInventory bigtableInventory = new BigtableInventory();
                bigtableInventory.setMessageTimestamp(timestamp.getMillis());
                bigtableInventory.setEffectiveDateTs(effectiveDateTs);
                bigtableInventory.setPayload(payload);
                bigtableInventory.setInventory(inventory);
                if (inventory.adjustment != null || inventory.countOverride != null || inventory.packageLevel != null && inventory.packageCase != null){
                    out.get(successTag).output(KV.of(rowKey, KV.of(String.valueOf(timestamp.getMillis()), bigtableInventory)));
                    LOG.info("Got Document with rowKey ******************** " + rowKey + "#" + inventory.getEffectiveDate());
                }
                else{
                    throw new Exception("Field countOverride, adjustment, or packageLevel and packageSize missing");
                }
            } catch (Exception e) {
                BigtableFailedDoc failedDocuments = new BigtableFailedDoc();
                failedDocuments.setTimestamp(timestamp.getMillis());
                failedDocuments.setErrorType("InputRequestError");
                failedDocuments.setPayload(payload);
                if (inventory!= null){
                    failedDocuments.setItemKey(inventory.getRowKey());
                }
                failedDocuments.setErrorMessage(e.getMessage());
                failedDocuments.setStacktrace(Throwables.getStackTraceAsString(e));
                out.get(failureTag).output(failedDocuments);
            }
        }
    }


    public static class FailedPubsubMessageToBigTableRowFn extends DoFn<BigtableFailedDoc, RowMutation> {
        private static String tableId = "pi-dataflow-error";

        @ProcessElement
        public void processElement(@Element BigtableFailedDoc failedDoc,  OutputReceiver<RowMutation> out) {
            RowMutation rowMutation = RowMutation.create(tableId, failedDoc.getRowKey());
            rowMutation.setCell("cf-meta", "payload", failedDoc.getPayload());
            if(failedDoc.getErrorMessage()!= null){
                rowMutation.setCell("cf-meta", "errorMessage", failedDoc.getErrorMessage());
            }
            if(failedDoc.getStacktrace()!=null){
                rowMutation.setCell("cf-meta", "stacktrace", failedDoc.getStacktrace());
            }
            if (failedDoc.getErroredRowKey() != null){
                rowMutation.setCell("cf-meta", "erroredRowKey", failedDoc.getErroredRowKey());
            }
            out.output(rowMutation);
            String logMessage = "Document failed processing, rowKey: " + failedDoc.getRowKey() + ", error message: " + failedDoc.getErrorMessage();
            if (failedDoc.getItemKey() != null){
                logMessage += " item key: " + failedDoc.getItemKey();
            }
            LOG.info(logMessage);
        }
    }

    public static class WriteTableRowFn extends DoFn<RowMutation, Void> {
        private static BigtableDataClient dataClient;
        private static BigtableTableAdminClient adminClient;
        private static String bigtableProjectId = "ret-shasta-cug01-dev";
        private static String bigtableInstanceId = "shasta-inventory-test";

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

        @Teardown
        public void shutdownClients(){
            dataClient.close();
            adminClient.close();
        }

        @ProcessElement
        public void processElement(@Element RowMutation rowMutation){
            dataClient.mutateRow(rowMutation);
        }
    }
}
