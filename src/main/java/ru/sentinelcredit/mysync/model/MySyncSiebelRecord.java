package ru.sentinelcredit.mysync.model;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class MySyncSiebelRecord {
    private String addr;
    private Integer contactInfoType;
    private Integer conPhoneOrdId;
    private Integer xChainSq;
    private Integer mAttempts;
}
