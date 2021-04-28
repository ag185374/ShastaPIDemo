package com.example.shastadataflow.common;

import avro.shaded.com.google.common.base.Throwables;
import com.example.shastadataflow.POJO.BigtableFailedDoc;
import com.example.shastadataflow.POJO.BigtableInventory;
import com.example.shastadataflow.POJO.Inventory;
import com.example.shastadataflow.POJO.InventorySum;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class BigtableOperation {
    public static class PubsubToBigtable extends DoFn<KV<String, Iterable<KV<String, BigtableInventory>>>, String> {
        private static BigtableDataClient dataClient;
        private static BigtableTableAdminClient adminClient;
        private static String tableId = "pi-dataflow-inventory";
        private static String bigtableProjectId = "ret-shasta-cug01-dev";
        private static String bigtableInstanceId = "pi-bigtable";

        /** The tag for the main output of InventorySum to json transform. */
        private TupleTag<String> successTag;

        /** The tag for the dead-letter output of the InventorySum to json transform. */
        private TupleTag<BigtableFailedDoc> failureTag;

        public PubsubToBigtable setSuccessTag(TupleTag<String> successTag) {
            this.successTag = successTag;
            return this;
        }

        public PubsubToBigtable setFailureTag(TupleTag<BigtableFailedDoc> failureTag) {
            this.failureTag = failureTag;
            return this;
        }

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
            Filters.Filter filter = Filters.FILTERS.chain()
                    .filter(Filters.FILTERS.limit().cellsPerColumn(1))
                    .filter(Filters.FILTERS.family().exactMatch("cf-meta"));

            //TODO: records with duplicate effectiveDate, but different timestamp needs to be considered(which BOH to use, which records to update)
            for (KV<String, BigtableInventory> message : messages) {
                int totalCount = 0;
                BigtableInventory bigtableInventory = message.getValue();
                Inventory inventory = bigtableInventory.getInventory();
                String rowkeyStamped = bigtableInventory.getRowKeyStamped();
                String rowKeyStart = bigtableInventory.getRowKeyStart();
                String rowKeyEnd = bigtableInventory.getRowKeyEnd();


                // Insert current record into Bigtable
                RowMutation rowMutation = RowMutation.create(tableId, rowkeyStamped);
                if (inventory.countOverride != null) {
                    totalCount = Integer.parseInt(inventory.countOverride);
                    rowMutation.setCell("cf-meta", "count#override", String.valueOf(totalCount));
                } else {
                    Query query = Query.create(tableId).range(rowkeyStamped, rowKeyEnd).filter(filter);
                    ServerStream<Row> rows = dataClient.readRows(query); // All rows in range [currentRow, end)
                    for (Row row : rows) {
                        List<RowCell> cell = row.getCells("cf-meta", "BOH");
                        if (cell.size() != 0) {
                            totalCount = Integer.parseInt(cell.get(0).getValue().toStringUtf8());
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
                        .setCell("cf-meta", "effectiveDate", inventory.effectiveDate);
                dataClient.mutateRow(rowMutation);

                // Update all previous records until we see a countOverride
                Query query = Query.create(tableId).range(rowKeyStart, rowkeyStamped).filter(filter);
                ServerStream<Row> rowsToUpdate = dataClient.readRows(query);
                List<Row> rowList = StreamSupport
                        .stream(rowsToUpdate.spliterator(), false)
                        .collect(Collectors.toList());
                Collections.reverse(rowList);
                boolean sendPubsubMessage = true;
                for (Row row: rowList){
                    List<RowCell> countOverrideCells = row.getCells("cf-meta", "count#override");
                    if (countOverrideCells.size() != 0) {
                        sendPubsubMessage = false;
                        break;
                    }
                    else{
                        List<RowCell> adjustmentCell = row.getCells("cf-meta", "adjustment");
                        if (adjustmentCell.size() != 0) {
                            int adjustment = Integer.parseInt(adjustmentCell.get(0).getValue().toStringUtf8());
                            totalCount += adjustment;
                            RowMutation rowToUpdate = RowMutation.create(tableId, row.getKey())
                                    .setCell("cf-meta", "BOH", String.valueOf(totalCount));
                            dataClient.mutateRow(rowToUpdate);
                        }
                    }
                }

                if (sendPubsubMessage){
                    // Construct pubsub message if the latest BOH is updated with the payload came
                    try {
                        String rowKeyStampedInclusive = bigtableInventory.getRowKeyStampedInclusive();
                        Query queryFirstRow = Query.create(tableId).range(rowKeyStart, rowKeyStampedInclusive).filter(filter);
                        ServerStream<Row> firstRows = dataClient.readRows(queryFirstRow);
                        for (Row row: firstRows){
                            List<RowCell> firstRowpayloadCells = row.getCells("cf-meta", "payload");
                            List<RowCell> firstRowpayBOHCells = row.getCells("cf-meta", "BOH");
                            if (firstRowpayloadCells.size()!=0 && firstRowpayBOHCells.size()!=0){
                                String firstRowPayload = firstRowpayloadCells.get(0).getValue().toStringUtf8();
                                String firstRowBOH = firstRowpayBOHCells.get(0).getValue().toStringUtf8();
                                ObjectMapper mapper = new ObjectMapper();
                                SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSS");
                                Date date = new Date();
                                Inventory firstInventory = mapper.readValue(firstRowPayload, Inventory.class);
                                InventorySum inventorySum = new InventorySum(formatter.format(date), firstInventory.version, firstInventory.org, firstInventory.enterpriseUnit, firstInventory.itemCode, firstInventory.upc, firstInventory.documentId, firstRowBOH);
                                String json = mapper.writeValueAsString(inventorySum);
                                out.get(successTag).output(json);
                            }
                            break;
                        }
                    } catch (JsonProcessingException e) {
                        BigtableFailedDoc failedDocuments = new BigtableFailedDoc();
                        failedDocuments.setTimestamp(bigtableInventory.getMessageTimestamp());
                        failedDocuments.setErrorType("ApplicationError");
                        failedDocuments.setErroredRowKey(rowkeyStamped);
                        failedDocuments.setPayload(bigtableInventory.getPayload());
                        failedDocuments.setErrorMessage(e.getMessage());
                        failedDocuments.setStacktrace(Throwables.getStackTraceAsString(e));
                        out.get(failureTag).output(failedDocuments);
                    }
                }
            }
        }
    }
}