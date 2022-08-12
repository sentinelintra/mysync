package ru.sentinelcredit.mysync.model;

import com.cck.genesys4j.model.BaseApiResponse;

public class MySyncStatResponse extends BaseApiResponse {
    private final Integer campConCount;
    private final Integer pc;

    protected MySyncStatResponse(Integer campConCount, Integer pc, Integer errorCode, String errorMessage) {
        super(errorCode, errorMessage);
        this.campConCount = campConCount;
        this.pc = pc;
    }

    public Integer getCampConCount() {
        return this.campConCount;
    }
    public Integer getPc() {
        return this.pc;
    }

    public static MySyncStatResponse success(Integer campConCount, Integer pc) {
        return new MySyncStatResponse(campConCount, pc, Errors.SUCCESS, "");
    }

    public static MySyncStatResponse failure(Integer errorCode, String errorMessage) {
        return new MySyncStatResponse((Integer)null, (Integer)null, errorCode, errorMessage);
    }
}
