package com.example.shastadataflow;

import lombok.Data;

@Data
public class Inventory {
    public String version;
    public String itemCode;
    public String UPC;
    public String documentName;
    public String documentId;
    public String documentType;
    public String documentStatus;
    public int count;
    public String effectiveDate;
    public String endDate;
    public double basePrice;
}
