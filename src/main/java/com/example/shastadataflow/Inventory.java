package com.example.shastadataflow;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class Inventory {
    public String version;
    public String retail;
    public String store;
    public String itemCode;
    public String UPC;
    public String documentName;
    public String documentId;
    public String documentType;
    public String documentStatus;
    public String countOverride;
    public String adjustment;
    public String effectiveDate;
    public String endDate;
    public double basePrice;
}
