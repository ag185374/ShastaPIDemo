package com.example.shastadataflow.POJO;

import com.example.shastadataflow.POJO.Inventory;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

@DefaultCoder(AvroCoder.class)
public class BigtableInventory {
    private long messageTimestamp;
    private long effectiveDateTs;
    private String payload;
    private Inventory inventory;

    public String getPayload() {
        return payload;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }

    public Inventory getInventory() {
        return inventory;
    }

    public void setInventory(Inventory inventory) {
        this.inventory = inventory;
    }

    public long getMessageTimestamp() {
        return messageTimestamp;
    }

    public void setMessageTimestamp(long messageTimestamp) {
        this.messageTimestamp = messageTimestamp;
    }

    public long getEffectiveDateTs() {
        return effectiveDateTs;
    }

    public void setEffectiveDateTs(long effectiveDateTs) {
        this.effectiveDateTs = effectiveDateTs;
    }

    public String getRowKeyStamped(){
        long reversedEffectiveDateTs = Long.MAX_VALUE - this.effectiveDateTs;
        long reversedTimestamp = Long.MAX_VALUE - this.messageTimestamp;
        return this.inventory.getRowKey() + "#effectiveDate#" + reversedEffectiveDateTs + "#timestamp#" + reversedTimestamp;
    }

    public String getRowKeyStampedInclusive(){
        long reversedEffectiveDateTs = Long.MAX_VALUE - this.effectiveDateTs + 1;
        long reversedTimestamp = Long.MAX_VALUE - this.messageTimestamp;
        return this.inventory.getRowKey() + "#effectiveDate#" + reversedEffectiveDateTs + "#timestamp#" + reversedTimestamp;
    }

    public String getRowKeyWithEffectiveDate(){
        long reversedEffectiveDateTs = Long.MAX_VALUE - this.effectiveDateTs;
        return this.inventory.getRowKey() + "#effectiveDate#" + reversedEffectiveDateTs;
    }

    public String getFamilyRowKeyStamped(){
        long reversedTimestamp = Long.MAX_VALUE - this.messageTimestamp;
        return this.inventory.getFamilyRowKey() + "#timestamp#" + reversedTimestamp;
    }

    public String getRowKeyStart(){
        return this.inventory.getRowKey() + "#effectiveDate#0";
    }

    public String getRowKeyEnd(){
        return this.inventory.getRowKey() + "#effectiveDate#" + Long.MAX_VALUE;
    }
}
