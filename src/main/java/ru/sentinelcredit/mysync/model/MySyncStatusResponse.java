package ru.sentinelcredit.mysync.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class MySyncStatusResponse {
    private Integer errorCode;
    private String errorMessage;
    private String campaignStatus;

    public MySyncStatusResponse() {

    }
}
