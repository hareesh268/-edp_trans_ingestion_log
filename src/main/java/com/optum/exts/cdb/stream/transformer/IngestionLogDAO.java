package com.optum.exts.cdb.stream.transformer;

import com.google.gson.*;
import com.optum.exts.cdb.stream.config.UtilityConfig;
import com.optum.exts.cdb.stream.transformer.dataobjects.HeadersDO;
import com.optum.exts.cdb.stream.transformer.dataobjects.IngestionLogDO;
import com.optum.exts.cdb.stream.transformer.dataobjects.IngestionLogJsonStructureDO;
import com.optum.exts.cdb.stream.utility.ConsumerUtil;
import jnr.ffi.annotations.In;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.logging.log4j.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;

import java.io.InputStream;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Configuration
@ConfigurationProperties("spring.datasource")
public abstract class IngestionLogDAO {

    @Autowired
    public JdbcTemplate jdbcTemplate;
    @Autowired
    UtilityConfig utilityConfig;
    @Autowired
    ConsumerUtil consumerUtil;
    String byteBufferRowKey;

    String  jsonString = "";

    private static final Logger log = LoggerFactory.getLogger(IngestionLogDAO.class);
   public abstract int[] ingestDataToIngestionLog(String completeJson, SpecificRecordBase specificRecordBase, String salaryYear, boolean isPhysicalDelete);

    //Ingesting on the Postgres
    public int[] ingestDataToIngestionLog(String completeJson, String tableName, boolean isPhysicalDelete) {

        IngestionLogDO ingestionLogDO = getIngestionLogDO(completeJson, tableName, isPhysicalDelete);
        //return ingestData(ingestionLogDO);

        List<IngestionLogDO> ingestionLogDOList = new ArrayList<>();

        if(ingestionLogDO.isRowKeyChanged()){

            log.info("ROW_KEY Changed "+ingestionLogDO.getHst_val_row_key()  + " to "+ ingestionLogDO.getRow_key() );

            ingestionLogDOList.add(generateNewInsert(ingestionLogDO));
            ingestionLogDOList.add(generateDelete(ingestionLogDO));


        } else {

            ingestionLogDOList.add(ingestionLogDO);

        }
        return ingestData(ingestionLogDOList);
    }

    private int[] ingestData(List<IngestionLogDO> ingestionLogDO) {


        String sql = utilityConfig.getQueryLookup().get("ingestionLog").replace("<SCHEMA>", utilityConfig.getSchema());





        return jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                ps.setString(1, ingestionLogDO.get(i).getTable_name());
                ps.setString(2, ingestionLogDO.get(i).getRow_key());
                ps.setTimestamp(3, ingestionLogDO.get(i).getIngestion_ts());
                ps.setString(4, ingestionLogDO.get(i).getRow_sts_cd());
                ps.setString(5, ingestionLogDO.get(i).getCdc_flag());
                ps.setTimestamp(6, ingestionLogDO.get(i).getCdc_ts());
                ps.setString(7, ingestionLogDO.get(i).getHst_val());
                ps.setString(8, ingestionLogDO.get(i).getNew_val());
                ps.setTimestamp(9, ConsumerUtil.timeStampParser(ingestionLogDO.get(i).getRow_tmstmp()));

               // log.info( ConsumerUtil.timeStampParser(ingestionLogDO.get(i).getRow_tmstmp()) +"++++++++"+ingestionLogDO.get(i).getRow_key());

                ps.setString(10, ingestionLogDO.get(i).getRow_sts_cd());
                ps.setString(11, ingestionLogDO.get(i).getCdc_flag());
                ps.setTimestamp(12, ingestionLogDO.get(i).getCdc_ts());
                ps.setString(13, ingestionLogDO.get(i).getHst_val());
                ps.setString(14, ingestionLogDO.get(i).getNew_val());
                ps.setTimestamp(15, ConsumerUtil.timeStampParser(ingestionLogDO.get(i).getRow_tmstmp()));
                ps.setTimestamp(16, ConsumerUtil.timeStampParser(ingestionLogDO.get(i).getRow_tmstmp()));
            }

            @Override
            public int getBatchSize() {
                return ingestionLogDO.size();
            }
        });

    }

    private IngestionLogDO getIngestionLogDO(String completeJson, String tableName, boolean isPhysicalDelete) {
        return buildIngestionLogDO(completeJson, tableName, isPhysicalDelete);

    }


    private String deriveRowKey(JsonElement jsonElement, String tableName) {
        JsonObject jsonObject = jsonElement.getAsJsonObject();
        StringBuilder rowKeyBuilder = new StringBuilder();
        //InputStream ipStream = Thread.currentThread().getContextClassLoader().getResourceAsStream("primary_key_input.json");
        if ( !jsonString.isEmpty()) {

            Gson gson = new Gson();
            JsonObject json = gson.fromJson(jsonString, JsonObject.class);
            JsonArray rowKeyArray = json.getAsJsonArray(tableName);
//TODO check if require trim
            int size = rowKeyArray.size();
            for (int i = 0; i < size - 1; i++) {
                String columnName = rowKeyArray.get(i).toString().replaceAll("^\"|\"$", "");
                Pattern pattern = Pattern.compile("(.*?)_EFF_DT");
                Matcher matcher = pattern.matcher(columnName);
                if (columnName.equalsIgnoreCase("PARTN_NBR")) {
                    rowKeyBuilder.append(getMemberValue(jsonObject, "XREF_ID_PARTN_NBR")).append("~");
                } else if (columnName.equalsIgnoreCase("CNSM_ID")) {
                    rowKeyBuilder.append(getMemberValue(jsonObject, "SRC_CDB_XREF_ID")).append("~");
                } else if (matcher.find()) {
                    String effDate = getMemberValue(jsonObject, columnName);
                    rowKeyBuilder.append(consumerUtil.dateParser(effDate)).append("~");
                }

                else {
                    rowKeyBuilder.append(getMemberValue(jsonObject, columnName)).append("~");
                }
            }
            String columnName = rowKeyArray.get(size - 1).toString().replaceAll("^\"|\"$", "");
            Pattern pattern = Pattern.compile("(.*?)_EFF_DT");
            Matcher matcher = pattern.matcher(columnName);
            if (matcher.find()) {
                String effDate = getMemberValue(jsonObject, columnName);
                rowKeyBuilder.append(consumerUtil.dateParser(effDate));
            } else {
                rowKeyBuilder.append(getMemberValue(jsonObject, columnName));
            }
            return rowKeyBuilder.toString();
        }
        return null;
    }


    private String getMemberValue(JsonObject jsonObject, String memberName) {

        if (!Objects.isNull(jsonObject.get(memberName))) {
            Object value = jsonObject.get(memberName);
            if (value instanceof JsonPrimitive) {
                return jsonObject.get(memberName).getAsString().trim();

            } else {
                return byteBufferRowKey;
            }
        }
        return "";
    }


    public IngestionLogDO buildIngestionLogDO(String completeJson, String tableName, boolean isPhysicalDelete) {

        Set<Map.Entry<String, JsonElement>> jsonSet = getJsonLogEntrySet(completeJson);

        IngestionLogJsonStructureDO ingestionLogJsonStructureDO = buildIngestionLogStructureDO(jsonSet);

        IngestionLogDO ingestionLogDO = new IngestionLogDO();

        if (!isPhysicalDelete) {
            ingestionLogDO.setRow_sts_cd(extractRowStatusCD(ingestionLogJsonStructureDO));
        } else {
            ingestionLogDO.setRow_sts_cd(utilityConfig.getPhysicalDelValue());
        }
        HeadersDO headersDO = extractHeadersObject(ingestionLogJsonStructureDO);
        ingestionLogDO.setTable_name(tableName);
        ingestionLogDO.setCdc_flag(headersDO.getOperation().split("")[0]);
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
        dateFormat.setTimeZone(TimeZone.getTimeZone("CST"));
        try {
            ingestionLogDO.setCdc_ts(consumerUtil.getTimeInCST(headersDO.getTimestamp()));
        } catch (ParseException e) {
            e.printStackTrace();
        }
        ingestionLogDO.setIngestion_ts(consumerUtil.getSysTimeStamp());
        if (!isPhysicalDelete)
            ingestionLogDO.setNew_val(extractNewVal(ingestionLogJsonStructureDO).toString());
        else {
            ingestionLogDO.setNew_val(utilityConfig.getEmptyJsonValue());
        }
        if (!Objects.isNull(ingestionLogJsonStructureDO.getOldJson().get("beforeData"))
                && !ingestionLogJsonStructureDO.getOldJson().get("beforeData").toString().equalsIgnoreCase("null")) {
            ingestionLogDO.setHst_val(ingestionLogJsonStructureDO.getOldJson().get("beforeData").toString());


            ingestionLogDO.setHst_val_row_key(deriveRowKey(new Gson().fromJson(ingestionLogDO.getHst_val(), JsonObject.class), tableName));
        }
        else {
            ingestionLogDO.setHst_val(utilityConfig.getEmptyJsonValue());
        }

        ingestionLogDO.setRow_tmstmp(extractRowTimestmp(ingestionLogJsonStructureDO));
        ingestionLogDO.setRow_key(deriveRowKey(extractNewVal(ingestionLogJsonStructureDO), tableName));
        /*if(ingestionLogJsonStructureDO.getOldJson().get("beforeData") != null) {

          //  ingestionLogJsonStructureDO.getOldJson().get("beforeData").getAsJsonArray().size() != 0
            ingestionLogDO.setHst_val_row_key(deriveRowKey(extractHstVal(ingestionLogJsonStructureDO), tableName));

        }*/
        isRowKeyChange(ingestionLogDO);

        return ingestionLogDO;
    }

    private void isRowKeyChange(IngestionLogDO ingestionLogDO){

        if(ingestionLogDO.getHst_val_row_key() !=null)


        {
            ingestionLogDO.setRowKeyChanged(!(ingestionLogDO.getRow_key().equals(ingestionLogDO.getHst_val_row_key())));



        }

        else {
            ingestionLogDO.setRowKeyChanged(false)
            ;}



       // ingestionLogDO.setRowKeyChanged(ingestionLogDO.getRow_key().equals((ingestionLogDO.getHst_val_row_key() !=null)?ingestionLogDO.getHst_val_row_key():ingestionLogDO.getRow_key()));


    }

    private String extractRowStatusCD(IngestionLogJsonStructureDO ingestionLogJsonStructureDO) {
        Map<String, JsonElement> newVal = ingestionLogJsonStructureDO.getNewJson();
        JsonObject jsonObject = newVal.get(extractTableName(ingestionLogJsonStructureDO)).getAsJsonObject();
//        return jsonObject.get("ROW_STS_CD").getAsString();
        HeadersDO headersDO = extractHeadersObject(ingestionLogJsonStructureDO);
        String rowstscd = null;
        if (!Objects.isNull(jsonObject.get("ROW_STS_CD"))) {
            return jsonObject.get("ROW_STS_CD").getAsString();
        } else {
            if (!headersDO.getOperation().split("")[0].equalsIgnoreCase("D")) {
                rowstscd = "A";
            }
            return rowstscd;
        }
    }


    private long extractRowTimestmp(IngestionLogJsonStructureDO ingestionLogJsonStructureDO) {
        Map<String, JsonElement> newVal = ingestionLogJsonStructureDO.getNewJson();
        JsonObject jsonObject = newVal.get(extractTableName(ingestionLogJsonStructureDO)).getAsJsonObject();
//        return jsonObject.get("ROW_STS_CD").getAsString();
        HeadersDO headersDO = extractHeadersObject(ingestionLogJsonStructureDO);
        long rowtmstmp = 0;
        if (!Objects.isNull(jsonObject.get("ROW_TMSTMP"))) {
            return jsonObject.get("ROW_TMSTMP").getAsLong();
        }
            return rowtmstmp;

    }

    private JsonElement extractNewVal(IngestionLogJsonStructureDO ingestionLogJsonStructureDO) {
        Map<String, JsonElement> newVal = ingestionLogJsonStructureDO.getNewJson();
        return newVal.get(extractTableName(ingestionLogJsonStructureDO));
    }

    private JsonElement extractHstVal(IngestionLogJsonStructureDO ingestionLogJsonStructureDO) {
        Map<String, JsonElement> hstVal = ingestionLogJsonStructureDO.getOldJson();
        return hstVal.get(extractTableName(ingestionLogJsonStructureDO));
    }

    private Set<Map.Entry<String, JsonElement>> getJsonLogEntrySet(String completeJson) {
        JsonParser jsonParser = new JsonParser();
        JsonObject jsonObject = (JsonObject) jsonParser.parse(consumerUtil.removeAsciiNull(completeJson));

        return jsonObject.entrySet();
    }

    private HeadersDO extractHeadersObject(IngestionLogJsonStructureDO ingestionLogJsonStructureDO) {

        Map<String, JsonElement> headersJson = ingestionLogJsonStructureDO.getHeaders();
        final HeadersDO[] headers = {new HeadersDO()};
        headersJson.forEach((k, v) -> {
            JsonElement element = v;
            Gson gson = new Gson();
            headers[0] = gson.fromJson(element, HeadersDO.class);
        });

        return headers[0];
    }

    private String extractTableName(IngestionLogJsonStructureDO ingestionLogJsonStructureDO) {
        final StringBuilder tableName = new StringBuilder("");
        Map<String, JsonElement> jsonEntry = ingestionLogJsonStructureDO.getNewJson();
        jsonEntry.keySet().forEach(key -> {
            tableName.append(key);
        });
        return tableName.toString();
    }

    private IngestionLogJsonStructureDO buildIngestionLogStructureDO(Set<Map.Entry<String, JsonElement>> jsonSet) {

        IngestionLogJsonStructureDO ingestionLogJsonStructureDO = new IngestionLogJsonStructureDO();

        String stringToRemove = "_DATA";

        jsonSet.forEach(mapEntry -> {
                    if (mapEntry.getKey().equals("headers")) {
                        Map<String, JsonElement> headerMap = new HashMap<>();
                        headerMap.put(mapEntry.getKey(), mapEntry.getValue());
                        ingestionLogJsonStructureDO.setHeaders(headerMap);

                    } else if (mapEntry.getKey().equals("beforeData")) {
                        Map<String, JsonElement> beforeDataMap = new HashMap<>();
                        beforeDataMap.put(mapEntry.getKey(), mapEntry.getValue());
                        ingestionLogJsonStructureDO.setOldJson(beforeDataMap);
                    } else {
                        Map<String, JsonElement> tableDataMap = new HashMap<>();
                        tableDataMap.put(mapEntry.getKey().split(stringToRemove)[0], mapEntry.getValue());
                        ingestionLogJsonStructureDO.setNewJson(tableDataMap);
                    }
                }
        );
        return ingestionLogJsonStructureDO;
    }

    public String rowstscd(String completeJson) {
        Set<Map.Entry<String, JsonElement>> jsonSet = getJsonLogEntrySet(completeJson);
        IngestionLogJsonStructureDO ingestionLogJsonStructureDO = buildIngestionLogStructureDO(jsonSet);
        return extractRowStatusCD(ingestionLogJsonStructureDO);
    }


    public IngestionLogDO generateNewInsert(IngestionLogDO ingestionLogOrig) {

        IngestionLogDO ingestionNew = new IngestionLogDO();

        ingestionNew.setTable_name(ingestionLogOrig.getTable_name());
        ingestionNew.setRow_key(ingestionLogOrig.getRow_key());
        //ingestionNew.setHst_val_row_key(ingestionLogOrig.getHst_val_row_key());
        ingestionNew.setRow_sts_cd("A");
        ingestionNew.setCdc_flag("I");
        ingestionNew.setCdc_ts(ingestionLogOrig.getCdc_ts());
        ingestionNew.setHst_val("{}");
        ingestionNew.setNew_val(ingestionLogOrig.getNew_val());
        ingestionNew.setIngestion_ts(ingestionLogOrig.getIngestion_ts());
        ingestionNew.setRow_tmstmp(ingestionLogOrig.getRow_tmstmp());


        return ingestionNew;


    }

    public IngestionLogDO generateDelete(IngestionLogDO ingestionLogOrig) {



        IngestionLogDO ingestionLogOldObject = new IngestionLogDO();
        ingestionLogOldObject.setTable_name(ingestionLogOrig.getTable_name());
        ingestionLogOldObject.setRow_key(ingestionLogOrig.getHst_val_row_key());
        ingestionLogOldObject.setRow_sts_cd("X");
        ingestionLogOldObject.setCdc_flag("D");
        ingestionLogOldObject.setCdc_ts(ingestionLogOrig.getCdc_ts());
        ingestionLogOldObject.setHst_val("{}");
        ingestionLogOldObject.setNew_val("{}");
        ingestionLogOldObject.setIngestion_ts(ingestionLogOrig.getIngestion_ts());
        ingestionLogOldObject.setRow_tmstmp(ingestionLogOrig.getRow_tmstmp());

        return ingestionLogOldObject;

    }


    @Bean
    public String ipStream() {
    InputStream ipStream = Thread.currentThread().getContextClassLoader().getResourceAsStream("primary_key_input.json");

        if (ipStream != null) {
            Scanner scanner = new Scanner(ipStream);

            while (scanner.hasNextLine()) {
                jsonString += scanner.nextLine();
            }


        }
        return jsonString;

    }



}
