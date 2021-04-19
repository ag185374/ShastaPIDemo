package com.example.shastadataflow;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;
import org.apache.avro.reflect.Nullable;

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
    @Nullable public String countOverride;
    @Nullable public String adjustment;
    public String effectiveDate;
    public String endDate;
    public double basePrice;
}
