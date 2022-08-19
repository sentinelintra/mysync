package ru.sentinelcredit.mysync.service;

import com.cck.genesys4j.model.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.cloud.client.discovery.DiscoveryClient;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import ru.sentinelcredit.mysync.model.MySyncSiebelRecord;
import ru.sentinelcredit.mysync.model.MySyncStatResponse;
import ru.sentinelcredit.mysync.model.MySyncStatusResponse;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.servlet.http.HttpSession;
import java.time.ZoneId;
import java.util.*;
import java.util.Date;

import java.sql.*;

@Slf4j
@Service
public class MySyncService {

    @Autowired
    private DiscoveryClient discoveryClient;

    @Value("${genesysApi.outboundJdbcUrl}")
    private String outboundJdbcUrl;
    @Value("${genesysApi.siebelSrcUrl}")
    private String siebelSrcUrl;
    @Value("${genesysApi.my.camp.service}")
    private String myCampService;

    private final static String distinctCampConSql =
            "select distinct CRM_CAMP_CON_ID from GENESYSSQL.iX_GEN_TABLE_NAME";

    private final static String queyCampConBySrcSql =
            "select ROW_ID from SIEBEL.S_CAMP_CON where SRC_ID = ?";

    private Connection conSiebel;
    private Connection conGenesys;

    private void genesysConnect() {

        try {
            //Loading and registering Oracle database thin driver			 
            Class.forName("oracle.jdbc.driver.OracleDriver");

            conSiebel = DriverManager.getConnection(siebelSrcUrl, "SIEBEL", "Gjcnjhjyybv1201");
            if (conSiebel != null)
                log.trace("Connected to the database {}", siebelSrcUrl);
            else
                log.trace("Failed to make connection {}", siebelSrcUrl);

            conGenesys = DriverManager.getConnection(outboundJdbcUrl, "genesyssql", "G3u%S02k");
            if (conGenesys != null)
                log.trace("Connected to the database {}", outboundJdbcUrl);
            else
                log.trace("Failed to make connection {}", outboundJdbcUrl);

            conGenesys.setAutoCommit(true);
            // conGenesys.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        } catch (SQLException e) {
            log.trace("genesysConnect SQL State: {} {}", e.getSQLState(), e.getMessage());
        } catch (Exception e) {
            log.trace("genesysConnect {}", e.getMessage());
        }
    }

    private void genesysDisconnect() {

        try {
            if (conSiebel != null)
                conSiebel.close();
        } catch (SQLException e) {
            log.trace("genesysDisconnect conSiebel SQL State: {} {}", e.getSQLState(), e.getMessage());
        }

        try {
            if (conGenesys != null)
                conGenesys.close();
        } catch (SQLException e) {
            log.trace("genesysDisconnect conGenesys SQL State: {} {}", e.getSQLState(), e.getMessage());
        }
    }

    @PreDestroy
    private void destroy() {
        genesysDisconnect();
    }

    @PostConstruct
    private void construct() { genesysConnect(); }

    private BaseApiResponse checkCampaignStatus (String guid, String campaigId, String groupId) {
        List<ServiceInstance> list = discoveryClient.getInstances(myCampService.toUpperCase());
        if (list == null || list.size() == 0 ) {
            return BaseApiResponse.failure(-2,"MyCamp service is not available");
        }

        RestTemplate restTemplate = new RestTemplate();
        MySyncStatusResponse mySyncStatusResponse = restTemplate.getForObject(list.get(0).getUri()+"/mycamp/getCampaignStatus"+
                "?guid="+guid+
                "&id="+campaigId+
                "&groupId="+groupId, MySyncStatusResponse.class);

        if (mySyncStatusResponse.getErrorCode() != 0)
            return BaseApiResponse.failure(-3, "MyCamp campaign status is not available");

        if (!"NotLoaded".equals(mySyncStatusResponse.getCampaignStatus()))
            return BaseApiResponse.failure(-3,"MyCamp campaign status is not NotLoaded");

        return BaseApiResponse.success();
    }

    public BaseApiResponse truncateTable (String guid, String tableName, String campaigId, String groupId, HttpSession session) {

        //session.setAttribute("guid", guid);
        //session.setAttribute("tableName", tableName);
        //session.setAttribute("campaigId", campaigId);
        //session.setAttribute("groupId", groupId);

        BaseApiResponse baseApiResponse = checkCampaignStatus(guid, campaigId, groupId);
        if (baseApiResponse.getErrorCode() != 0)
            return baseApiResponse;

        try {
            Statement st = conGenesys.createStatement();
            st.execute("truncate table GENESYSSQL." + tableName);
        } catch (SQLException e) {
            log.trace("truncateTable SQL State: {} {}", e.getSQLState(), e.getMessage());
            return BaseApiResponse.failure(1001, e.getMessage());
        } catch (Exception e) {
            log.trace("truncateTable: {}", e.getMessage());
            return BaseApiResponse.failure(-1,"truncateTable (conGenesys is " + String.valueOf(conGenesys) + ") -> " + e.toString());
        }

        return BaseApiResponse.success();
    }

    public BaseApiResponse truncateTable2 (String crmCampaignId, String tableName) {
        // get all camp_con records
        // get distinct camp_con from genTable
        try {
            Statement st = conGenesys.createStatement();
            ResultSet rs = st.executeQuery(
                    "select distinct CRM_CAMP_CON_ID from GENESYSSQL.iX_GEN_TABLE_NAME".replace("iX_GEN_TABLE_NAME", tableName));

            Set<String> genCampCon = new HashSet();
            while (rs.next()) {
                genCampCon.add(rs.getString(1));
            }

            rs.close();
            st.close();

            PreparedStatement st2 = conSiebel.prepareStatement("select ROW_ID from SIEBEL.S_CAMP_CON where SRC_ID = ?");
            st2.setString(1, crmCampaignId);
            ResultSet rs2 = st2.executeQuery();

            Set<String> crmCampCon = new HashSet();
            while (rs2.next()) {
                crmCampCon.add(rs.getString(1));
            }

            rs2.close();
            st2.close();

            genCampCon.removeAll(crmCampCon);

            if (!genCampCon.isEmpty()) {
                PreparedStatement st3 = conGenesys.prepareStatement(
                        "update GENESYSSQL.iX_GEN_TABLE_NAME set PORTFOLIO_NAME = to_char(SYSDATE)||'X' where CRM_CAMP_CON_ID = ?".
                                replace("iX_GEN_TABLE_NAME", tableName));

                Iterator<String> i = genCampCon.iterator();
                while (i.hasNext()) {
                    st3.setString(1, i.next());
                    st3.addBatch();
                }

                st3.executeBatch();
                st3.close();

                Statement st4 = conGenesys.createStatement();
                st4.execute("delete from GENESYSSQL.iX_GEN_TABLE_NAME where RECORD_STATUS <> CASE WHEN (sysdate-TO_DATE(''' || sCurUPDID || ''')) >= ' || iCountOfMinWaitRetrive || '/24/60 THEN 99 ELSE 2 END");
                st4.close();
            }
        } catch (SQLException e) {
            log.trace("truncateTable2 SQL State: {} {}", e.getSQLState(), e.getMessage());
            return BaseApiResponse.failure(1001, e.getMessage());
        }

        return BaseApiResponse.success();
    }

    public BaseApiResponse syncClient (String perId, String tableName, Integer dailyFrom, Integer dailyTill, Integer tzDbId, Integer chainId,
                                       String crmCampConId, Integer tzDbId2, String crmCampaignId, Integer xBatchId, Date xCcUpdId, HttpSession session) {
        try {
            PreparedStatement st = conSiebel.prepareStatement("select ADDR, CONTACT_INFO_TYPE, " +
                    "ROWNUM CON_PHONE_ORD_ID, X_CHAIN_SQ, M_ATTEMPTS " +
                    "from SIEBEL.CX_SOFT_PHONES_FOR_CL " +
                    "where PER_ID = ?");
            st.setString(1, perId);

            Collection<MySyncSiebelRecord> myCampCrmTable = new LinkedList();
            ResultSet rs = st.executeQuery();
            while (rs.next()) {
                myCampCrmTable.add(new MySyncSiebelRecord(rs.getString(1), rs.getInt(2), rs.getInt(3),
                        rs.getInt(4), rs.getInt(5)));

                log.trace("ADDR -> {} CONTACT_INFO_TYPE -> {} CON_PHONE_ORD_ID -> {} X_CHAIN_SQ -> {} M_ATTEMPTS -> {}",
                        rs.getString(1), rs.getInt(2), rs.getInt(3), rs.getInt(4), rs.getInt(5));
            }

            rs.close();
            st.close();

            st = conGenesys.prepareStatement("insert into GENESYSSQL." + tableName + " ( " +
                            "RECORD_ID, CRM_CONTACT_ID, CONTACT_INFO, CONTACT_INFO_TYPE, RECORD_TYPE, RECORD_STATUS, " +
                            "ATTEMPT, DAILY_FROM, DAILY_TILL, TZ_DBID, CHAIN_ID, CHAIN_N, DAILY_FROM2, DAILY_TILL2, " +
                            "CRM_CAMP_CON_ID, MSK_MUSIC_FILE, CHAIN_SEQUENCE, CONTACT_INFO_TYPE2, TZ_DBID2, " +
                            "CALL_RESULT, COL_LEAD_ID, CRM_CAMPAIGN_ID, X_BATCH_ID, X_CC_UPD_ID ) values ( " +
                            "my_sync_service_seq.nextval, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )");

            Iterator<MySyncSiebelRecord> i = myCampCrmTable.iterator();
            while (i.hasNext()) {
                MySyncSiebelRecord myCampSiebelRecord = i.next();

                st.setString(1, perId);
                st.setString(2, myCampSiebelRecord.getAddr());
                st.setInt(3, myCampSiebelRecord.getContactInfoType());
                st.setInt(4, 2);
                st.setInt(5, 1);
                st.setInt(6, myCampSiebelRecord.getMAttempts());
                st.setInt(7, dailyFrom);
                st.setInt(8, dailyTill);
                st.setInt(9, tzDbId);
                st.setInt(10, chainId);
                st.setInt(11, myCampSiebelRecord.getConPhoneOrdId());
                st.setInt(12, dailyFrom);
                st.setInt(13, dailyTill);
                st.setString(14, crmCampConId);
                st.setInt(15, 9);
                st.setInt(16, myCampSiebelRecord.getXChainSq());
                st.setInt(17, myCampSiebelRecord.getContactInfoType());
                st.setInt(18, tzDbId2);
                st.setInt(19, 28);
                st.setString(20, perId);
                st.setString(21, crmCampaignId);
                st.setInt(22, xBatchId);
                st.setObject(23, xCcUpdId.toInstant().atZone(ZoneId.of("Europe/Moscow")));
                st.addBatch();
            }

            st.executeBatch();
            st.close();

        } catch (SQLException e) {
            log.trace("genesysConnect SQL State: {} {}", e.getSQLState(), e.getMessage());
            return BaseApiResponse.failure(1001, e.getMessage());
        }

        return BaseApiResponse.success();
    }

    public BaseApiResponse getStatData (String crmCampaignId, String tableName) {

        Integer campConCount = 0;
        Integer numberSum = 0;
        Integer numberSumC = 0;
        Integer pc = 0;

        try {
            PreparedStatement st = conSiebel.prepareStatement("select count(*) from SIEBEL.S_CAMP_CON where SRC_ID = ?");
            st.setString(1, crmCampaignId);
            ResultSet rs = st.executeQuery();

            Set<String> crmCampCon = new HashSet();
            while (rs.next()) {
                campConCount = rs.getInt(1);
            }

            rs.close();
            st.close();

            Statement st2 = conGenesys.createStatement();
            ResultSet rs2 = st2.executeQuery(
                    "select count(*) from GENESYSSQL." + tableName);

            while (rs2.next()) {
                numberSum = rs2.getInt(1);
            }

            rs2.close();
            st2.close();

            Statement st3 = conGenesys.createStatement();
            ResultSet rs3 = st3.executeQuery(
                    "select count(*) from GENESYSSQL." + tableName + " where RECORD_STATUS = 3 or CALL_RESULT <> 28");

            while (rs3.next()) {
                numberSumC = rs3.getInt(1);
            }

            rs3.close();
            st3.close();

            if (numberSum == 0) pc = 0; else pc = numberSumC / numberSum;

        } catch (SQLException e) {
            log.trace("getStatData SQL State: {} {}", e.getSQLState(), e.getMessage());
            return MySyncStatResponse.failure(1001, e.getMessage());
        } catch (Exception e) {
            log.trace("getStatData: {}", e.getMessage());
            return MySyncStatResponse.failure(-1,"getStatData -> " + e.toString());
        }

        return MySyncStatResponse.success(campConCount, pc);
    }

    public MySyncService() {

    }
}
