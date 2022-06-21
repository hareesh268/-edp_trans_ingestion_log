package com.optum.exts.cdb.stream.transformer;


import com.optum.exts.cdb.stream.transformer.dataobjects.IngestionLogDO;
import com.optum.exts.cdb.stream.utility.ConsumerUtil;
import org.apache.avro.specific.SpecificRecordBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

@Configuration
@ConfigurationProperties("spring.datasource")
public class IngestionLogDAOForByteBufferRowKeys extends IngestionLogDAO {

    private static final Logger log = LoggerFactory.getLogger(IngestionLogDAOForByteBufferRowKeys.class);
    public int[] ingestDataToIngestionLog(String completeJson,
                                        SpecificRecordBase specificRecordBase,
                                        String salaryYear,
                                        boolean isPhysicalDelete) {
        this.byteBufferRowKey = salaryYear;

        IngestionLogDO ingestionLogDO = buildIngestionLogDO(completeJson,
                specificRecordBase.getSchema().getName(), isPhysicalDelete);

        String sql = utilityConfig.getQueryLookup().get("ingestionLog").replace("<SCHEMA>",
                utilityConfig.getSchema());

        List<IngestionLogDO> ingestionLogDOList = new ArrayList<>();

        if (ingestionLogDO.isRowKeyChanged()) {

            log.info("ROW_KEY Changed "+ingestionLogDO.getHst_val_row_key()  + " to "+ ingestionLogDO.getRow_key() );

            ingestionLogDOList.add(generateNewInsert(ingestionLogDO));
            ingestionLogDOList.add(generateDelete(ingestionLogDO));


        } else {

            ingestionLogDOList.add(ingestionLogDO);

        }
        return ingestData(ingestionLogDOList);

    }



 /*       return jdbcTemplate.update(sql, ps -> {
            ps.setString(1, ingestionLogDO.getTable_name());
            ps.setString(2, ingestionLogDO.getRow_key());
            ps.setTimestamp(3, new Timestamp(ingestionLogDO.getIngestion_ts().getTime()));
            ps.setString(4, ingestionLogDO.getRow_sts_cd());
            ps.setString(5, ingestionLogDO.getCdc_flag());
            ps.setTimestamp(6, ingestionLogDO.getCdc_ts());
            ps.setString(7, ingestionLogDO.getHst_val());
            ps.setString(8, ingestionLogDO.getNew_val());
            ps.setTimestamp(9,consumerUtil.timeStampParser(ingestionLogDO.getRow_tmstmp()));

            ps.setString(10, ingestionLogDO.getRow_sts_cd());
            ps.setString(11, ingestionLogDO.getCdc_flag());
            ps.setTimestamp(12, ingestionLogDO.getCdc_ts());
            ps.setString(13, ingestionLogDO.getHst_val());
            ps.setString(14, ingestionLogDO.getNew_val());
            ps.setTimestamp(15,consumerUtil.timeStampParser(ingestionLogDO.getRow_tmstmp()));
            ps.setTimestamp(16, ConsumerUtil.timeStampParser(ingestionLogDO.getRow_tmstmp()));

        });*/




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
}
