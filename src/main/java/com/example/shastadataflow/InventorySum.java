package com.example.shastadataflow;

import com.fasterxml.jackson.annotation.JsonGetter;
import lombok.Data;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.annotate.JsonSetter;

@Data
public class InventorySum {
    public InventorySum(String version, String retail, String store, String itemCode, String upc, String documentId, String totalCount){
        this.version = version;
        this.retail = retail;
        this.store = store;
        this.itemCode = itemCode;
        this.upc = upc;
        this.documentId = documentId;
        this.BOH = totalCount;
    }
    public String version;
    public String retail;
    public String store;
    public String itemCode;
    @JsonProperty("BOH")
    public String BOH;
    public String upc;
    public String documentId;

    @JsonGetter("BOH")
    public String getBOH() {
        return BOH;
    }

    @JsonSetter("BOH")
    public void setBOH(String BOH) {
        this.BOH = BOH;
    }
}
