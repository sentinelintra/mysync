package ru.sentinelcredit.mysync.controller;

import com.cck.genesys4j.model.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.*;
import ru.sentinelcredit.mysync.service.MySyncService;

import javax.servlet.http.HttpSession;
import java.time.Duration;
import java.time.LocalDate;
import java.util.Collection;
import java.util.Date;

@Slf4j
@RestController
public class MySyncController {

    @Autowired
    private MySyncService mySyncService;

    @GetMapping("/mysync/syncClient")
    public BaseApiResponse syncClient (@RequestParam String guid, @RequestParam String perId, @RequestParam String tableName, @RequestParam Integer dailyFrom,
                                       @RequestParam Integer dailyTill, @RequestParam Integer tzDbId, @RequestParam Integer chainId,
                                       @RequestParam String crmCampConId, @RequestParam Integer tzDbId2, @RequestParam String crmCampaignId,
                                       @RequestParam Integer xBatchId, @RequestParam @DateTimeFormat(pattern="dd/MM/yyyy HH:mm:ss") Date xCcUpdId, HttpSession session) {
        Date startDate = new Date();

        log.trace("syncClient start with guid => '{}' perId => '{}' tableName => '{}' dailyFrom => '{}' dailyTill => '{}' tzDbId => '{}' chainId => '{}' " +
                        "crmCampConId => '{}' tzDbId2 => '{}' crmCampaignId => '{}' xBatchId => '{}' xCcUpdId  => '{}'", guid, perId, tableName, dailyFrom,
                dailyTill, tzDbId, chainId, crmCampConId, tzDbId2, crmCampaignId, xBatchId, xCcUpdId, session);
        BaseApiResponse baseApiResponse = mySyncService.syncClient(perId, tableName, dailyFrom,
                dailyTill, tzDbId, chainId, crmCampConId, tzDbId2, crmCampaignId, xBatchId, xCcUpdId, session);

        Date endDate = new Date();
        long diff = endDate.getTime() - startDate.getTime();

        log.trace("syncClient end with code => '{}' message => '{}' duration {} seconds", baseApiResponse.getErrorCode(), baseApiResponse.getErrorMessage(), diff/1000);
        return baseApiResponse;
    }

    @GetMapping("/mysync/truncateTable")
    public BaseApiResponse truncateTable (@RequestParam String guid, @RequestParam String tableName, @RequestParam String campaignId, @RequestParam String groupId, HttpSession session) {
        log.trace("truncateTable start with guid => '{}' tableName => '{}' campaignId => '{}' groupId => '{}' ", guid, tableName, campaignId, groupId);
        BaseApiResponse baseApiResponse = mySyncService.truncateTable(guid, tableName, campaignId, groupId, session);
        log.trace("truncateTable end with code => '{}' message => '{}'", baseApiResponse.getErrorCode(), baseApiResponse.getErrorMessage());
        return baseApiResponse;
    }

    @GetMapping("/mysync/truncateTable2")
    public BaseApiResponse truncateTable2 (@RequestParam String guid, @RequestParam String crmCampaignId, @RequestParam String tableName) {
        log.trace("truncateTable2 start with guid => '{}' crmCampaignId => '{}' tableName => '{}'", guid, crmCampaignId, tableName);
        BaseApiResponse baseApiResponse = mySyncService.truncateTable2(crmCampaignId, tableName);
        log.trace("truncateTable2 end with code => '{}' message => '{}'", baseApiResponse.getErrorCode(), baseApiResponse.getErrorMessage());
        return baseApiResponse;
    }

    @GetMapping("/mysync/getStatData")
    public BaseApiResponse getOpcData (@RequestParam String guid, @RequestParam String crmCampaignId, @RequestParam String tableName) {
        log.trace("getStatData start with guid => '{}' crmCampaignId => '{}' tableName => '{}'", guid, crmCampaignId, tableName);
        BaseApiResponse baseApiResponse = mySyncService.getStatData(crmCampaignId, tableName);
        log.trace("getStatData end with code => '{}' message => '{}'", baseApiResponse.getErrorCode(), baseApiResponse.getErrorMessage());
        return baseApiResponse;
    }
}
