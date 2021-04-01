package com.example.shastadataflow;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

import java.io.Serializable;

@DefaultCoder(AvroCoder.class)
public class BigtableInventory implements Serializable {
    public BigtableInventory(Inventory inventory, String payload){
        this.payload = payload;
        this.inventory = inventory;
        this.rowKey = "Dataflow#Count#Dept#"+inventory.documentId+"#UPC#"+inventory.UPC+"#ItemCode#"+inventory.itemCode;
    }

    public String rowKey;
    public String payload;
    public Inventory inventory;

}