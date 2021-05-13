package com.example.shastadataflow.POJO;

import com.fasterxml.jackson.annotation.JsonGetter;
import lombok.Data;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.annotate.JsonSetter;

@Data
public class InventorySum {
    public InventorySum(String timestamp, String version, String org, String enterpriseUnit, String itemCode, String upc, String documentId, int totalCount, String familyId, String effectiveDate){
        this.version = version;
        this.org = org;
        this.enterpriseUnit = enterpriseUnit;
        this.itemCode = itemCode;
        this.upc = upc;
        this.documentId = documentId;
        this.BOH = totalCount;
        this.timestamp = timestamp;
        this.familyId = familyId;
        this.effectiveDate = effectiveDate;

    }
    public String timestamp;
    public String version;
    public String org;
    public String enterpriseUnit;
    public String itemCode;
    public String upc;
    @JsonProperty("BOH")
    public Integer BOH;
    public String documentId;
    public String familyId;
    public Integer adjustment;
    public Integer countOverride;
    public Integer packageOverride;
    public String effectiveDate;


    @JsonGetter("BOH")
    public Integer getBOH() {
        return BOH;
    }

    @JsonSetter("BOH")
    public void setBOH(int BOH) {
        this.BOH = BOH;
    }
}
