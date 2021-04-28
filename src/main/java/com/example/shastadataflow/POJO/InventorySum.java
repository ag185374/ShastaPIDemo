package com.example.shastadataflow.POJO;

import com.fasterxml.jackson.annotation.JsonGetter;
import lombok.Data;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.annotate.JsonSetter;

@Data
public class InventorySum {
    public InventorySum(String timestamp, String version, String org, String enterpriseUnit, String itemCode, String upc, String documentId, String totalCount){
        this.version = version;
        this.org = org;
        this.enterpriseUnit = enterpriseUnit;
        this.itemCode = itemCode;
        this.upc = upc;
        this.documentId = documentId;
        this.BOH = totalCount;
        this.timestamp = timestamp;
    }
    public String timestamp;
    public String version;
    public String org;
    public String enterpriseUnit;
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
