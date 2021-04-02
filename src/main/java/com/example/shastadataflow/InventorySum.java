package com.example.shastadataflow;

import lombok.Data;

@Data
public class InventorySum {
    public InventorySum(String itemCode, String upc, String documentId, String totalCount){
        this.documentId = documentId;
        this.upc = upc;
        this.totalCount = totalCount;
        this.itemCode = itemCode;
    }
    public String itemCode;
    public String totalCount;
    public String upc;
    public String documentId;

}
