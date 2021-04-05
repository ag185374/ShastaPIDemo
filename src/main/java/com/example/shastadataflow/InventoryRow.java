package com.example.shastadataflow;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

@DefaultCoder(AvroCoder.class)
public class InventoryRow {
    public InventoryRow(String rowKey, int totalCount, Inventory inventory, String payload){
        this.rowKey = rowKey;
        this.totalCount = totalCount;
        this.inventory = inventory;
        this.payload = payload;
    }
    public String rowKey;
    public String payload;
    public Inventory inventory;
    public int totalCount;
}
