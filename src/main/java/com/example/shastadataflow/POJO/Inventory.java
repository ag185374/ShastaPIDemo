package com.example.shastadataflow.POJO;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;
import org.apache.avro.reflect.Nullable;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class Inventory {
    public String version;
    public String org;
    public String enterpriseUnit;
    public String itemCode;
    public String upc;
    public String familyId;
    @Nullable public Integer packageLevel;
    @Nullable public Integer packageCase;
    @Nullable public Integer countOverride;
    @Nullable public Integer adjustment;
    public String documentName;
    public String documentId;
    public String documentType;
    public String documentStatus;

    public String effectiveDate;
    public String endDate;
    public double basePrice;
    public double cost;

    public String getRowKey(){
        return "dataflow#count#org#" + this.org + "#enterpriseUnit#" + this.enterpriseUnit + "#upc#" + this.upc + "#itemCode#" + this.itemCode;
    }

    public String getFamilyRowKey(){
        return "dataflow#count#org#" + this.org + "#enterpriseUnit#" + this.enterpriseUnit + "#familyId#" + this.familyId + "#upc#" + this.upc + "#itemCode#" + this.itemCode;
    }

    public long getEffectiveDateMillis() throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        Date date = sdf.parse(this.effectiveDate);
        return date.getTime();
    }
}
