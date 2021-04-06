package com.example.shastadataflow;

import com.google.bigtable.repackaged.com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.bigtable.repackaged.com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.bigtable.repackaged.com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.bigtable.repackaged.com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.bigtable.repackaged.com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.bigtable.repackaged.com.google.cloud.bigtable.data.v2.models.Row;
import com.google.bigtable.repackaged.com.google.cloud.bigtable.data.v2.models.RowCell;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.List;

public class FilterTest {
    private static BigtableDataClient dataClient;
    private static BigtableTableAdminClient adminClient;
    private static String tableId = "pi-dataflow-inventory";
    private static String bigtableProjectId = "ret-shasta-cug01-dev";
    private static String bigtableInstanceId = "pi-bigtable";

    public static void main(String[] args) throws IOException, ParseException {
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

        // Get date epoch
        String start = "2021/04/06";
        String end = "2021/04/07";
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd");
        SimpleDateFormat outputSdf = new SimpleDateFormat("yyyy/MM/dd:HH:mm:ss:SSS");
        Date startDate = sdf.parse(start);
        Date endDate = sdf.parse(end);
        long startEpoch = startDate.getTime() * 1000L;
        long endEpoch = endDate.getTime() * 1000L;
        long timestamp = Instant.now().minus(1, ChronoUnit.HOURS).toEpochMilli() * 1000;

        Filters.Filter timeFilter = Filters.FILTERS.chain()
                                            .filter(Filters.FILTERS.timestamp().range().startClosed(startEpoch).endClosed(endEpoch))
                                            .filter(Filters.FILTERS.limit().cellsPerColumn(1));

        Filters.Filter filter2 = Filters.FILTERS.timestamp().range().startClosed(0L).endClosed(timestamp);


        String rowKey = "dataflow#count#retail#9898#store#9898#upc#9898#ItemCode#9898";
        Row btRow = dataClient.readRow(tableId, rowKey,timeFilter);
        if (btRow != null){
            List<RowCell> cell  = btRow.getCells("cf-meta","BOH");
            if (cell.size()!= 0){
                System.out.println("timeStamp -----------> " + outputSdf.format(new Date(cell.get(0).getTimestamp()/1000)));
                System.out.println("BOH -----------------> " + cell.get(0).getValue().toStringUtf8());
            }
        }
    }
}
