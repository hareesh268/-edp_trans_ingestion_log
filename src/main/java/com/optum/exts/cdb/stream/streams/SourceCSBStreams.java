/*
package com.optum.exts.cdb.stream.streams;

import com.optum.attunity.cdb.model.*;
import com.optum.exts.cdb.stream.config.TopicConfig;
import com.optum.exts.cdb.stream.transformer.*;
import com.optum.exts.cdb.stream.utility.ConsumerUtil;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Date;

*/
/**
 * Created by amolugu1
 *//*

@Configuration
@ConfigurationProperties(prefix = "streams.member")
//@Profile({"local", "member"})
public class SourceCSBStreams {

    private static final Logger log = LoggerFactory.getLogger(SourceCSBStreams.class);

    @Autowired
    IngestionLogDAO ingestionLogDAO;

    @Autowired
    TopicConfig topicConfig;

    @Autowired
    ConsumerUtil consumerUtil;

    SimpleDateFormat sdf =    new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    @Bean
    RetryTemplate retryTemplate() {
        RetryTemplate retryTemplate = new RetryTemplate();

        */
/*FixedBackOffPolicy fixedBackOffPolicy = new FixedBackOffPolicy();
        fixedBackOffPolicy.setBackOffPeriod(1000*60);
        retryTemplate.setBackOffPolicy(fixedBackOffPolicy);
*//*

        SimpleRetryPolicy retryPolicy = new SimpleRetryPolicy();
        retryPolicy.setMaxAttempts(10);

        retryTemplate.setRetryPolicy(retryPolicy);
        retryTemplate.setThrowLastExceptionOnExhausted(true);

        return retryTemplate;
    }
    @Bean
    public KStream<L_CNSM_SRCH_KEY, com.optum.attunity.cdb.model.L_CNSM_SRCH> lCNSMSrchStream(StreamsBuilder builder) {

        KStream<L_CNSM_SRCH_KEY, com.optum.attunity.cdb.model.L_CNSM_SRCH>
                memberKStream = builder.stream(topicConfig.getLcnsmSrch());

        try {
            memberKStream.map((key, value) -> {
                log.info("L_CNSM_SRCH Consuming key  :::" + key.toString());
                log.info("L_CNSM_SRCH Consuming value  :::" + value.toString());
                final int[] retryCount = {0};
                //log.info("L_CNSM_SRCH Consuming Value  :::" + value.toString());

             try {
                 if (!value.getHeaders().getOperation().name().equals("DELETE")) { //INSERT, UPDATE
                     if (value.getLCNSMSRCHDATA().getROWUSERID().trim().equalsIgnoreCase("LINK/UNLINK")) {
                         if (!value.getLCNSMSRCHDATA().getROWSTSCD().trim().equalsIgnoreCase("D")
                                 && !value.getLCNSMSRCHDATA().getROWSTSCD().trim().equalsIgnoreCase("A")) {
                             ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "L_CNSM_SRCH", false);
                         }
                         System.out.println("The above record is a LINK/UNLINK case");
                     } else { // NOT LINK/UNLINK cases
                         ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "L_CNSM_SRCH", false);
                     }
                 } else { //PHYSICAL DELETE
                     ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "L_CNSM_SRCH", true);
                 }

                 //System.exit(1);

                 return KeyValue.pair(key, value);

             }

             catch (Exception e) {
                 try {
                     retryTemplate().execute(new RetryCallback<KeyValue<L_CNSM_SRCH_KEY, com.optum.attunity.cdb.model.L_CNSM_SRCH>, Exception>() {
                         @Override
                         public KeyValue<L_CNSM_SRCH_KEY, com.optum.attunity.cdb.model.L_CNSM_SRCH> doWithRetry(RetryContext context) throws Exception {
                             retryCount[0]++;
                             log.info("************************************ RETRY COUNT : " + retryCount[0]+ ":::::::" + key.toString());
                             //throw new SQLException("");
                             if (!value.getHeaders().getOperation().name().equals("DELETE")) { //INSERT, UPDATE
                                 if (value.getLCNSMSRCHDATA().getROWUSERID().trim().equalsIgnoreCase("LINK/UNLINK")) {
                                     if (!value.getLCNSMSRCHDATA().getROWSTSCD().trim().equalsIgnoreCase("D")
                                             && !value.getLCNSMSRCHDATA().getROWSTSCD().trim().equalsIgnoreCase("A")) {
                                         ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "L_CNSM_SRCH", false);
                                     }
                                     System.out.println("The above record is a LINK/UNLINK case");
                                 } else { // NOT LINK/UNLINK cases
                                     ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "L_CNSM_SRCH", false);
                                 }
                             } else { //PHYSICAL DELETE
                                 ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "L_CNSM_SRCH", true);
                             }

                             //System.exit(1);
                             return KeyValue.pair(key, value);
                         }
                     });
                 } catch (Throwable throwable) {

                     log.error("SQLException in Table L_CNSM_SRCH :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());

                     log.error("SQLException in Table L_CNSM_SRCH :::time::::" + sdf.format(new Date()) + ":::::::" + new MetadataTransformer());

                     log.error("Exception",throwable);
                     //throw new RuntimeException("");
                     log.error("Restarting L_CNSM_SRCH POD"); System.exit(1);
                     //log.error("Exception",throwable);
                 }
             }

                return KeyValue.pair(key,value);

            }).transform(new MetadataTransformer());
        } catch (Exception e) {
            e.printStackTrace();
        }


        return memberKStream;
    }

 */
/*  @Bean
    public KStream<L_COV_PRDT_DT_KEY, com.optum.attunity.cdb.model.L_COV_PRDT_DT> lCovPrdtDtStream(StreamsBuilder builder) {

        KStream<L_COV_PRDT_DT_KEY, com.optum.attunity.cdb.model.L_COV_PRDT_DT>
                memberKStream = builder.stream(topicConfig.getLcovPrdtdt());

        try {
            memberKStream.map((key, value) -> {
                final int[] retryCount = {0};

                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

                log.info("L_COV_PRDT_DT Consuming key +++ ::time::::" + sdf.format(new Date()) + ":::::::" + key);
                log.info("L_COV_PRDT_DT Consuming value +++ ::time::::" + sdf.format(new Date()) + ":::::::" + value.toString());
                try {


                    // TODO 1. Send the table name and the other details, so that i do not have to retrieve it in the DAO
                    // Single Responsibility Principal is debatable
                    // TODO 2. Come up with a builder pattern to set the above details so that it becomes verbosely set

                    if (!value.getHeaders().getOperation().name().equals("DELETE")) { //INSERT, UPDATE
                        if (value.getLCOVPRDTDTDATA().getROWUSERID().trim().equalsIgnoreCase("LINK/UNLINK")){
                            if(!value.getLCOVPRDTDTDATA().getROWSTSCD().trim().equalsIgnoreCase("D")
                                    && !value.getLCOVPRDTDTDATA().getROWSTSCD().trim().equalsIgnoreCase("A")){
                                ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "L_COV_PRDT_DT", false);
                            }
                            System.out.println("The above record is a LINK/UNLINK case");
                        } else { // NOT LINK/UNLINK cases
                            ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "L_COV_PRDT_DT", false);
                        }
                    } else { //PHYSICAL DELETE
                        ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "L_COV_PRDT_DT", true);
                    }
                } catch (Exception e) {
                    log.error("Exception in Table L_COV_PRDT_DT ::::time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                    e.printStackTrace();

                }
                return KeyValue.pair(key, value);


            });
        } catch (Exception e) {
            e.printStackTrace();
        }


        return memberKStream;
    }

    @Bean
    public KStream<L_HLT_SRV_DT_KEY, com.optum.attunity.cdb.model.L_HLT_SRV_DT> lHltSrvDtStream(StreamsBuilder builder) {

        KStream<L_HLT_SRV_DT_KEY, com.optum.attunity.cdb.model.L_HLT_SRV_DT>
                memberKStream = builder.stream(topicConfig.getlHltSrvDt());

        try {
            memberKStream.map((key, value) -> {
                final int[] retryCount = {0};

                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

                log.info("L_HLT_SRV_DT Consuming key  ::time::::" + sdf.format(new Date()) + ":::::::" + key);
                log.info("L_HLT_SRV_DT Consuming value +++ ::time::::" + sdf.format(new Date()) + ":::::::" + value.toString());

                try {
                    if (!value.getHeaders().getOperation().name().equals("DELETE")) { //INSERT, UPDATE
                        if (value.getLHLTSRVDTDATA().getROWUSERID().trim().equalsIgnoreCase("LINK/UNLINK")) {
                            if (!value.getLHLTSRVDTDATA().getROWSTSCD().trim().equalsIgnoreCase("D")
                                    && !value.getLHLTSRVDTDATA().getROWSTSCD().trim().equalsIgnoreCase("A")) {
                                ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "L_HLT_SRV_DT", false);
                            }
                            System.out.println("The above record is a LINK/UNLINK case");
                        } else { // NOT LINK/UNLINK cases
                            ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "L_HLT_SRV_DT", false);
                        }
                    } else { //PHYSICAL DELETE
                        ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "L_HLT_SRV_DT", true);
                    }

                } catch (Exception e) {
                    log.error("Exception in Table L_HLT_SRV_DT ::::time::::" + sdf.format(new Date()) + ":::::::" + key.toString());
                    e.printStackTrace();

                }
                return KeyValue.pair(key, value);


            });
        } catch (Exception e) {
            e.printStackTrace();
        }


        return memberKStream;
    }

    @Bean
    public KStream<CNSM_STS_KEY, com.optum.attunity.cdb.model.CNSM_STS> cnsmStsStream(StreamsBuilder builder) {


        KStream<CNSM_STS_KEY, com.optum.attunity.cdb.model.CNSM_STS>
                memberKStream = builder.stream(topicConfig.getCnsmSts());

        try {
            memberKStream.map((key, value) -> {
                final int[] retryCount = {0};
                log.info("CNSM_STS Consuming key  :::" + key.toString());
                log.info("CNSM_STS Consuming value  :::" + value.toString());

                try {
                    if (!value.getHeaders().getOperation().name().equals("DELETE")) { //INSERT, UPDATE
                        if (value.getCNSMSTSDATA().getROWUSERID().trim().equalsIgnoreCase("LINK/UNLINK")) {
                            if (!value.getCNSMSTSDATA().getROWSTSCD().trim().equalsIgnoreCase("D")
                                    && !value.getCNSMSTSDATA().getROWSTSCD().trim().equalsIgnoreCase("A")) {
                                ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_STS", false);
                            }
                            System.out.println("The above record is a LINK/UNLINK case");
                        } else { // NOT LINK/UNLINK cases
                            ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_STS", false);
                        }
                    } else { //PHYSICAL DELETE
                        ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_STS", true);
                    }
                } catch (Exception e) {
                    log.info("Exception in Table CNSM_STS :::" + key.toString());
                    e.printStackTrace();
                }

                return KeyValue.pair(key, value);

            });
        } catch (Exception e) {
            e.printStackTrace();
        }


        return memberKStream;
    }



    @Bean
    public KStream<CNSM_MDCR_ENRL_KEY, com.optum.attunity.cdb.model.CNSM_MDCR_ENRL> cnsmMdcrEnrlStream(StreamsBuilder builder) {


        KStream<CNSM_MDCR_ENRL_KEY, com.optum.attunity.cdb.model.CNSM_MDCR_ENRL>
                memberKStream = builder.stream(topicConfig.getCnsmMdcrEnrl());

        try {
            memberKStream.map((key, value) -> {
                final int[] retryCount = {0};
                log.info("CNSM_MDCR_ENRL Consuming key  :::" + key.toString());
                log.info("CNSM_MDCR_ENRL Consuming value  :::" + value.toString());

                try {
                    if (!value.getHeaders().getOperation().name().equals("DELETE")) { //INSERT, UPDATE
                        if (value.getCNSMMDCRENRLDATA().getROWUSERID().trim().equalsIgnoreCase("LINK/UNLINK")) {
                            if (!value.getCNSMMDCRENRLDATA().getROWSTSCD().trim().equalsIgnoreCase("D")
                                    && !value.getCNSMMDCRENRLDATA().getROWSTSCD().trim().equalsIgnoreCase("A")) {
                                ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_MDCR_ENRL", false);
                            }
                            System.out.println("The above record is a LINK/UNLINK case");
                        } else { // NOT LINK/UNLINK cases
                            ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_MDCR_ENRL", false);
                        }
                    } else { //PHYSICAL DELETE
                        ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_MDCR_ENRL", true);
                    }
                } catch (Exception e) {
                    log.info("Exception in Table CNSM_MDCR_ENRL :::" + key.toString());
                    e.printStackTrace();
                }
                return KeyValue.pair(key, value);

            });
        } catch (Exception e) {
            e.printStackTrace();
        }

        return memberKStream;
    }

    @Bean
    public KStream<L_COV_PRDT_PCP_KEY, com.optum.attunity.cdb.model.L_COV_PRDT_PCP> lCovPrdtPcpStream(StreamsBuilder builder) {


        KStream<L_COV_PRDT_PCP_KEY, com.optum.attunity.cdb.model.L_COV_PRDT_PCP>
                memberKStream = builder.stream(topicConfig.getlCovPrdtPcp());

        try {
            memberKStream.map((key, value) -> {
                final int[] retryCount = {0};
                log.info("L_COV_PRDT_PCP Consuming key  :::" + key.toString());
                log.info("L_COV_PRDT_PCP Consuming value  :::" + value.toString());

                try {
                    if (!value.getHeaders().getOperation().name().equals("DELETE")) { //INSERT, UPDATE
                        if (value.getLCOVPRDTPCPDATA().getROWUSERID().trim().equalsIgnoreCase("LINK/UNLINK")) {
                            if (!value.getLCOVPRDTPCPDATA().getROWSTSCD().trim().equalsIgnoreCase("D")
                                    && !value.getLCOVPRDTPCPDATA().getROWSTSCD().trim().equalsIgnoreCase("A")) {
                                ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "L_COV_PRDT_PCP", false);
                            }
                            System.out.println("The above record is a LINK/UNLINK case");
                        } else { // NOT LINK/UNLINK cases
                            ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "L_COV_PRDT_PCP", false);
                        }
                    } else { //PHYSICAL DELETE
                        ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "L_COV_PRDT_PCP", true);
                    }
                } catch (Exception e) {
                    log.info("Exception in Table L_COV_PRDT_PCP :::" + key.toString());
                    e.printStackTrace();
                }

                return KeyValue.pair(key, value);

            });
        } catch (Exception e) {
            e.printStackTrace();
        }


        return memberKStream;
    }

    @Bean
    public KStream<L_LF_DIS_PRDT_DT_KEY, com.optum.attunity.cdb.model.L_LF_DIS_PRDT_DT> lLfDisPrdtDtStream(StreamsBuilder builder) {


        KStream<L_LF_DIS_PRDT_DT_KEY, com.optum.attunity.cdb.model.L_LF_DIS_PRDT_DT>
                memberKStream = builder.stream(topicConfig.getlLfDisPrdtDt());

        try {
            memberKStream.map((key, value) -> {
                final int[] retryCount = {0};
                log.info("L_LF_DIS_PRDT_DT Consuming key  :::" + key.toString());
                log.info("L_LF_DIS_PRDT_DT Consuming value  :::" + value.toString());
                try {
                    if (!value.getHeaders().getOperation().name().equals("DELETE")) { //INSERT, UPDATE
                        if (value.getLLFDISPRDTDTDATA().getROWUSERID().trim().equalsIgnoreCase("LINK/UNLINK")) {
                            if (!value.getLLFDISPRDTDTDATA().getROWSTSCD().trim().equalsIgnoreCase("D")
                                    && !value.getLLFDISPRDTDTDATA().getROWSTSCD().trim().equalsIgnoreCase("A")) {
                                ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "L_LF_DIS_PRDT_DT", false);
                            }
                            System.out.println("The above record is a LINK/UNLINK case");
                        } else { // NOT LINK/UNLINK cases
                            ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "L_LF_DIS_PRDT_DT", false);
                        }
                    } else { //PHYSICAL DELETE
                        ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "L_LF_DIS_PRDT_DT", true);
                    }
                } catch (Exception e) {
                    log.info("Exception in Table L_LF_DIS_PRDT_DT :::" + key.toString());
                    e.printStackTrace();
                }

                return KeyValue.pair(key, value);

            });
        } catch (Exception e) {
            e.printStackTrace();
        }


        return memberKStream;
    }

    @Bean
    public KStream<CNSM_DTL_KEY, com.optum.attunity.cdb.model.CNSM_DTL> cnsmDtlStream(StreamsBuilder builder) {


        KStream<CNSM_DTL_KEY, com.optum.attunity.cdb.model.CNSM_DTL>
                memberKStream = builder.stream(topicConfig.getCnsmDtl());

        try {
            memberKStream.map((key, value) -> {
                final int[] retryCount = {0};
                log.info("CNSM_DTL Consuming key  :::" + key.toString());
                log.info("CNSM_DTL Consuming value  :::" + value.toString());

                try {
                    if (!value.getHeaders().getOperation().name().equals("DELETE")) { //INSERT, UPDATE
                        if (value.getCNSMDTLDATA().getROWUSERID().trim().equalsIgnoreCase("LINK/UNLINK")) {
                            if (!value.getCNSMDTLDATA().getROWSTSCD().trim().equalsIgnoreCase("D")
                                    && !value.getCNSMDTLDATA().getROWSTSCD().trim().equalsIgnoreCase("A")) {
                                ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_DTL", false);
                            }
                            System.out.println("The above record is a LINK/UNLINK case");
                        } else { // NOT LINK/UNLINK cases
                            ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_DTL", false);
                        }
                    } else { //PHYSICAL DELETE
                        ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_DTL", true);
                    }
                } catch (Exception e) {
                    log.info("Exception in Table CNSM_DTL :::" + key.toString());
                    e.printStackTrace();
                }
                return KeyValue.pair(key, value);

            });
        } catch (Exception e) {
            e.printStackTrace();
        }


        return memberKStream;
    }

    @Bean
    public KStream<ML_CNSM_TEL_KEY, com.optum.attunity.cdb.model.ML_CNSM_TEL> mlCnsmTelStream(StreamsBuilder builder) {


        KStream<ML_CNSM_TEL_KEY, com.optum.attunity.cdb.model.ML_CNSM_TEL>
                memberKStream = builder.stream(topicConfig.getMlCnsmTel());

        try {
            memberKStream.map((key, value) -> {
                final int[] retryCount = {0};
                log.info("ML_CNSM_TEL Consuming key  :::" + key.toString());
                log.info("ML_CNSM_TEL Consuming value  :::" + value.toString());

                try {
                    if (!value.getHeaders().getOperation().name().equals("DELETE")) { //INSERT, UPDATE
                        if (value.getMLCNSMTELDATA().getROWUSERID().trim().equalsIgnoreCase("LINK/UNLINK")) {
                            if (!value.getMLCNSMTELDATA().getROWSTSCD().trim().equalsIgnoreCase("D")
                                    && !value.getMLCNSMTELDATA().getROWSTSCD().trim().equalsIgnoreCase("A")) {
                                ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "ML_CNSM_TEL", false);
                            }
                            System.out.println("The above record is a LINK/UNLINK case");
                        } else { // NOT LINK/UNLINK cases
                            ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "ML_CNSM_TEL", false);
                        }
                    } else { //PHYSICAL DELETE
                        ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "ML_CNSM_TEL", true);
                    }
                } catch (Exception e) {
                    log.info("Exception in Table ML_CNSM_TEL :::" + key.toString());
                    e.printStackTrace();
                }
                return KeyValue.pair(key, value);

            });
        } catch (Exception e) {
            e.printStackTrace();
        }


        return memberKStream;
    }

    @Bean
    public KStream<ML_CNSM_ADR_KEY, com.optum.attunity.cdb.model.ML_CNSM_ADR> mlCnsmAdrStream(StreamsBuilder builder) {


        KStream<ML_CNSM_ADR_KEY, com.optum.attunity.cdb.model.ML_CNSM_ADR>
                memberKStream = builder.stream(topicConfig.getMlCnsmAdr());

        try {
            memberKStream.map((key, value) -> {
                final int[] retryCount = {0};
                log.info("ML_CNSM_ADR Consuming key  :::" + key.toString());
                log.info("ML_CNSM_ADR Consuming value  :::" + value.toString());
                try {
                    if (!value.getHeaders().getOperation().name().equals("DELETE")) { //INSERT, UPDATE
                        if (value.getMLCNSMADRDATA().getROWUSERID().trim().equalsIgnoreCase("LINK/UNLINK")) {
                            if (!value.getMLCNSMADRDATA().getROWSTSCD().trim().equalsIgnoreCase("D")
                                    && !value.getMLCNSMADRDATA().getROWSTSCD().trim().equalsIgnoreCase("A")) {
                                ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "ML_CNSM_ADR", false);
                            }
                            System.out.println("The above record is a LINK/UNLINK case");
                        } else { // NOT LINK/UNLINK cases
                            ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "ML_CNSM_ADR", false);
                        }
                    } else { //PHYSICAL DELETE
                        ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "ML_CNSM_ADR", true);
                    }
                } catch (Exception e) {
                    log.info("Exception in Table ML_CNSM_ADR :::" + key.toString());
                    e.printStackTrace();
                }
                return KeyValue.pair(key, value);

            });
        } catch (Exception e) {
            e.printStackTrace();
        }


        return memberKStream;
    }


    // The getROWSTSCD is not present in getCNSMMDCRPRISECDATA class
    @Bean
    public KStream<CNSM_MDCR_PRISEC_KEY, com.optum.attunity.cdb.model.CNSM_MDCR_PRISEC> cnsmMdcrPrisecStream(StreamsBuilder builder) {


        KStream<CNSM_MDCR_PRISEC_KEY, com.optum.attunity.cdb.model.CNSM_MDCR_PRISEC>
                memberKStream = builder.stream(topicConfig.getCnsmMdcrPrisec());

        try {
            memberKStream.map((key, value) -> {
                final int[] retryCount = {0};
                log.info("CNSM_MDCR_PRISEC Consuming key  :::" + key.toString());
                log.info("CNSM_MDCR_PRISEC Consuming value  :::" + value.toString());
                try {
                    if (!value.getHeaders().getOperation().name().equals("DELETE")) { //INSERT, UPDATE
                        if (value.getCNSMMDCRPRISECDATA().getROWUSERID().trim().equalsIgnoreCase("LINK/UNLINK")){
                           String rowstscd=ingestionLogDAO.rowstscd(value.toString());
                            if(!rowstscd.trim().equalsIgnoreCase("D")
                                    && !rowstscd.trim().equalsIgnoreCase("A")){
                            ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_MDCR_PRISEC", false);
                            }
                            System.out.println("The above record is a LINK/UNLINK case");
                        } else { // NOT LINK/UNLINK cases
                            ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_MDCR_PRISEC", false);
                        }
                    } else { //PHYSICAL DELETE
                        ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_MDCR_PRISEC", true);
                    }
                } catch (Exception e) {
                    log.info("Exception in Table CNSM_MDCR_PRISEC :::" + key.toString());
                    e.printStackTrace();
                }
                return KeyValue.pair(key, value);

            });
        } catch (Exception e) {
            e.printStackTrace();
        }


        return memberKStream;
    }



    @Bean
    public KStream<ML_CNSM_ELCTR_ADR_KEY, com.optum.attunity.cdb.model.ML_CNSM_ELCTR_ADR> mlCnsmElctrAdrStream(StreamsBuilder builder) {


        KStream<ML_CNSM_ELCTR_ADR_KEY, com.optum.attunity.cdb.model.ML_CNSM_ELCTR_ADR>
                memberKStream = builder.stream(topicConfig.getMlCnsmElctrAdr());

        try {
            memberKStream.map((key, value) -> {
                final int[] retryCount = {0};
                log.info("ML_CNSM_ELCTR_ADR Consuming key  :::" + key.toString());
                log.info("ML_CNSM_ELCTR_ADR Consuming value  :::" + value.toString());
                try {
                    if (!value.getHeaders().getOperation().name().equals("DELETE")) { //INSERT, UPDATE
                        if (value.getMLCNSMELCTRADRDATA().getROWUSERID().trim().equalsIgnoreCase("LINK/UNLINK")) {
                            if (!value.getMLCNSMELCTRADRDATA().getROWSTSCD().trim().equalsIgnoreCase("D")
                                    && !value.getMLCNSMELCTRADRDATA().getROWSTSCD().trim().equalsIgnoreCase("A")) {
                                ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "ML_CNSM_ELCTR_ADR", false);
                            }
                            System.out.println("The above record is a LINK/UNLINK case");
                        } else { // NOT LINK/UNLINK cases
                            ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "ML_CNSM_ELCTR_ADR", false);
                        }
                    } else { //PHYSICAL DELETE
                        ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "ML_CNSM_ELCTR_ADR", true);
                    }

                } catch (Exception e) {
                    log.info("Exception in Table ML_CNSM_ELCTR_ADR :::" + key.toString());
                    e.printStackTrace();
                }
                return KeyValue.pair(key, value);

            });
        } catch (Exception e) {
            e.printStackTrace();
        }


        return memberKStream;
    }


    @Bean
    public KStream<CNSM_MDCR_ENTL_KEY, com.optum.attunity.cdb.model.CNSM_MDCR_ENTL> CnsmMdcrEntlStream(StreamsBuilder builder) {

        KStream<CNSM_MDCR_ENTL_KEY, com.optum.attunity.cdb.model.CNSM_MDCR_ENTL>
                memberKStream = builder.stream(topicConfig.getCnsmMdcrEntl());

        try {
            memberKStream.map((key, value) -> {
                final int[] retryCount = {0};
                log.info("CNSM_MDCR_ENTL Consuming key  :::" + key.toString());
                log.info("CNSM_MDCR_ENTL Consuming value  :::" + value.toString());

                try {
                    if (!value.getHeaders().getOperation().name().equals("DELETE")) { //INSERT, UPDATE
                        if (value.getCNSMMDCRENTLDATA().getROWUSERID().trim().equalsIgnoreCase("LINK/UNLINK")) {
                            if (!value.getCNSMMDCRENTLDATA().getROWSTSCD().trim().equalsIgnoreCase("D")
                                    && !value.getCNSMMDCRENTLDATA().getROWSTSCD().trim().equalsIgnoreCase("A")) {
                                ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_MDCR_ENTL", false);
                            }
                            System.out.println("The above record is a LINK/UNLINK case");
                        } else { // NOT LINK/UNLINK cases
                            ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_MDCR_ENTL", false);
                        }
                    } else { //PHYSICAL DELETE
                        ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_MDCR_ENTL", true);
                    }
                } catch (Exception e) {
                    log.info("Exception in Table CNSM_MDCR_ENTL :::" + key.toString());
                    e.printStackTrace();
                }
                return KeyValue.pair(key, value);

            });
        } catch (Exception e) {
            e.printStackTrace();
        }


        return memberKStream;
    }


    @Bean
    public KStream<CNSM_OTHR_INS_KEY, com.optum.attunity.cdb.model.CNSM_OTHR_INS> CnsmOthrInsStream(StreamsBuilder builder) {

        KStream<CNSM_OTHR_INS_KEY, com.optum.attunity.cdb.model.CNSM_OTHR_INS>
                memberKStream = builder.stream(topicConfig.getCnsmOthrIns());

        try {
            memberKStream.map((key, value) -> {
                final int[] retryCount = {0};
                log.info("CNSM_OTHR_INS Consuming key  :::" + key.toString());
                log.info("CNSM_OTHR_INS Consuming value  :::" + value.toString());

                try {
                    if (!value.getHeaders().getOperation().name().equals("DELETE")) { //INSERT, UPDATE
                        if (value.getCNSMOTHRINSDATA().getROWUSERID().trim().equalsIgnoreCase("LINK/UNLINK")) {
                            if (!value.getCNSMOTHRINSDATA().getROWSTSCD().trim().equalsIgnoreCase("D")
                                    && !value.getCNSMOTHRINSDATA().getROWSTSCD().trim().equalsIgnoreCase("A")) {
                                ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_OTHR_INS", false);
                            }
                            System.out.println("The above record is a LINK/UNLINK case");
                        } else { // NOT LINK/UNLINK cases
                            ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_OTHR_INS", false);
                        }
                    } else { //PHYSICAL DELETE
                        ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_OTHR_INS", true);
                    }
                } catch (Exception e) {
                    log.info("Exception in Table CNSM_OTHR_INS :::" + key.toString());
                    e.printStackTrace();
                }

                return KeyValue.pair(key, value);
            });
        } catch (Exception e) {
            e.printStackTrace();
        }


        return memberKStream;
    }

    @Bean
    public KStream<ML_CNSM_XREF_KEY, com.optum.attunity.cdb.model.ML_CNSM_XREF> mlCnsmXrefStream(StreamsBuilder builder) {


        KStream<ML_CNSM_XREF_KEY, com.optum.attunity.cdb.model.ML_CNSM_XREF>
                memberKStream = builder.stream(topicConfig.getMlCnsmXref());

        try {
            memberKStream.map((key, value) -> {
                final int[] retryCount = {0};
                log.info("ML_CNSM_XREF Consuming key  :::" + key.toString());
                log.info("ML_CNSM_XREF Consuming value  :::" + value.toString());

                try {
                    if (!value.getHeaders().getOperation().name().equals("DELETE")) { //INSERT, UPDATE
                        if (value.getMLCNSMXREFDATA().getROWUSERID().trim().equalsIgnoreCase("LINK/UNLINK")) {
                            if (!value.getMLCNSMXREFDATA().getROWSTSCD().trim().equalsIgnoreCase("D")
                                    && !value.getMLCNSMXREFDATA().getROWSTSCD().trim().equalsIgnoreCase("A")) {
                                ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "ML_CNSM_XREF", false);
                            }
                            System.out.println("The above record is a LINK/UNLINK case");
                        } else { // NOT LINK/UNLINK cases
                            ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "ML_CNSM_XREF", false);
                        }
                    } else { //PHYSICAL DELETE
                        ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "ML_CNSM_XREF", true);
                    }
                } catch (Exception e) {
                    log.info("Exception in Table ML_CNSM_XREF :::" + key.toString());
                    e.printStackTrace();
                }
                return KeyValue.pair(key, value);

            });
        } catch (Exception e) {
            e.printStackTrace();
        }


        return memberKStream;
    }


    @Bean
    public KStream<CNSM_AUTH_REP_KEY, com.optum.attunity.cdb.model.CNSM_AUTH_REP> cnsmAuthRepStream(StreamsBuilder builder) {


        KStream<CNSM_AUTH_REP_KEY, com.optum.attunity.cdb.model.CNSM_AUTH_REP>
                memberKStream = builder.stream(topicConfig.getCnsmAuthRep());

        try {
            memberKStream.map((key, value) -> {
                final int[] retryCount = {0};
                log.info("CNSM_AUTH_REP Consuming key  :::" + key.toString());
                log.info("CNSM_AUTH_REP Consuming value  :::" + value.toString());

                try {
                    if (!value.getHeaders().getOperation().name().equals("DELETE")) { //INSERT, UPDATE
                        if (value.getCNSMAUTHREPDATA().getROWUSERID().trim().equalsIgnoreCase("LINK/UNLINK")) {
                            if (!value.getCNSMAUTHREPDATA().getROWSTSCD().trim().equalsIgnoreCase("D")
                                    && !value.getCNSMAUTHREPDATA().getROWSTSCD().trim().equalsIgnoreCase("A")) {
                                ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_AUTH_REP", false);
                            }
                                 System.out.println("The above record is a LINK/UNLINK case");
                        } else { // NOT LINK/UNLINK cases
                            ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_AUTH_REP", false);
                        }
                    } else { //PHYSICAL DELETE
                        ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_AUTH_REP", true);
                    }
                } catch (Exception e) {
                    log.info("Exception in Table CNSM_AUTH_REP :::" + key.toString());
                    e.printStackTrace();
                }
                return KeyValue.pair(key, value);
            });
        } catch (Exception e) {
            e.printStackTrace();
        }

        return memberKStream;
    }


    @Bean
    public KStream<CNSM_CAL_KEY, com.optum.attunity.cdb.model.CNSM_CAL> cnsmCalStream(StreamsBuilder builder) {


        KStream<CNSM_CAL_KEY, com.optum.attunity.cdb.model.CNSM_CAL>
                memberKStream = builder.stream(topicConfig.getCnsmCal());

        try {
            memberKStream.map((key, value) -> {
                final int[] retryCount = {0};
                log.info("CNSM_CAL Consuming key  :::" + key.toString());
                log.info("CNSM_CAL Consuming value  :::" + value.toString());

                try {
                    if (!value.getHeaders().getOperation().name().equals("DELETE")) { //INSERT, UPDATE
                        if (value.getCNSMCALDATA().getROWUSERID().trim().equalsIgnoreCase("LINK/UNLINK")) {
                            if (!value.getCNSMCALDATA().getROWSTSCD().trim().equalsIgnoreCase("D")
                                    && !value.getCNSMCALDATA().getROWSTSCD().trim().equalsIgnoreCase("A")) {
                                ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_CAL", false);
                            }
                                 System.out.println("The above record is a LINK/UNLINK case");
                        } else { // NOT LINK/UNLINK cases
                            ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_CAL", false);
                        }
                    } else { //PHYSICAL DELETE
                        ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_CAL", true);
                    }
                } catch (Exception e) {
                    log.info("Exception in Table CNSM_CAL :::" + key.toString());
                    e.printStackTrace();
                }
                return KeyValue.pair(key, value);

            });
        } catch (Exception e) {
            e.printStackTrace();
        }

        return memberKStream;
    }


    @Bean
    public KStream<CNSM_COB_PRIMACY_KEY, com.optum.attunity.cdb.model.CNSM_COB_PRIMACY> cnsmCobPrimacy(StreamsBuilder builder) {


        KStream<CNSM_COB_PRIMACY_KEY, com.optum.attunity.cdb.model.CNSM_COB_PRIMACY>
                memberKStream = builder.stream(topicConfig.getCnsmCobPrimacy());

        try {
            memberKStream.map((key, value) -> {
                final int[] retryCount = {0};
                log.info("CNSM_COB_PRIMACY Consuming key  :::" + key.toString());
                log.info("CNSM_COB_PRIMACY Consuming value  :::" + value.toString());

                try {
                    if (!value.getHeaders().getOperation().name().equals("DELETE")) { //INSERT, UPDATE
                        if (value.getCNSMCOBPRIMACYDATA().getROWUSERID().trim().equalsIgnoreCase("LINK/UNLINK")) {
                            if (!value.getCNSMCOBPRIMACYDATA().getROWSTSCD().trim().equalsIgnoreCase("D")
                                    && !value.getCNSMCOBPRIMACYDATA().getROWSTSCD().trim().equalsIgnoreCase("A")) {
                                ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_COB_PRIMACY", false);
                            }
                                 System.out.println("The above record is a LINK/UNLINK case");
                        } else { // NOT LINK/UNLINK cases
                            ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_COB_PRIMACY", false);
                        }
                    } else { //PHYSICAL DELETE
                        ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_COB_PRIMACY", true);
                    }
                } catch (Exception e) {
                    log.info("Exception in Table CNSM_COB_PRIMACY :::" + key.toString());
                    e.printStackTrace();
                }
                return KeyValue.pair(key, value);

            });
        } catch (Exception e) {
            e.printStackTrace();
        }


        return memberKStream;
    }


    //The getROWSTSCD is not present in the getCNSMCOBPRISECDATA class
    @Bean
    public KStream<CNSM_COB_PRISEC_KEY, com.optum.attunity.cdb.model.CNSM_COB_PRISEC> cnsmCobPrisec(StreamsBuilder builder) {


        KStream<CNSM_COB_PRISEC_KEY, com.optum.attunity.cdb.model.CNSM_COB_PRISEC>
                memberKStream = builder.stream(topicConfig.getCnsmCobPrisec());

        try {
            memberKStream.map((key, value) -> {
                final int[] retryCount = {0};
                log.info("CNSM_COB_PRISEC Consuming key  :::" + key.toString());
                log.info("CNSM_COB_PRISEC Consuming value  :::" + value.toString());

                try {
                    if (!value.getHeaders().getOperation().name().equals("DELETE")) { //INSERT, UPDATE
                        if (value.getCNSMCOBPRISECDATA().getROWUSERID().trim().equalsIgnoreCase("LINK/UNLINK")){
                             String rowstscd=ingestionLogDAO.rowstscd(value.toString());
                            if(!rowstscd.trim().equalsIgnoreCase("D")
                                    && !rowstscd.trim().equalsIgnoreCase("A")){
                                ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_COB_PRISEC", false);
                            }
                                 System.out.println("The above record is a LINK/UNLINK case");
                        } else { // NOT LINK/UNLINK cases
                            ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_COB_PRISEC", false);
                        }
                    } else { //PHYSICAL DELETE
                        ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_COB_PRISEC", true);
                    }
                }
                catch (Exception e) {
                    log.info("Exception in Table CNSM_COB_PRISEC :::" + key.toString());
                    e.printStackTrace();
                }
                return KeyValue.pair(key, value);

            });
        } catch (Exception e) {
            e.printStackTrace();
        }


        return memberKStream;
    }




    @Bean
    public KStream<CNSM_CUST_DEFN_FLD_KEY, com.optum.attunity.cdb.model.CNSM_CUST_DEFN_FLD> cnsmCustDefnFldStream(StreamsBuilder builder) {


        KStream<CNSM_CUST_DEFN_FLD_KEY, com.optum.attunity.cdb.model.CNSM_CUST_DEFN_FLD>
                memberKStream = builder.stream(topicConfig.getCnsmCustDefnFld());

        try {
            memberKStream.map((key, value) -> {
                final int[] retryCount = {0};
                log.info("CNSM_CUST_DEFN_FLD Consuming key  :::" + key.toString());
                log.info("CNSM_CUST_DEFN_FLD Consuming value  :::" + value.toString());

                try {
                    if (!value.getHeaders().getOperation().name().equals("DELETE")) { //INSERT, UPDATE
                        if (value.getCNSMCUSTDEFNFLDDATA().getROWUSERID().trim().equalsIgnoreCase("LINK/UNLINK")) {
                            if (!value.getCNSMCUSTDEFNFLDDATA().getROWSTSCD().trim().equalsIgnoreCase("D")
                                    && !value.getCNSMCUSTDEFNFLDDATA().getROWSTSCD().trim().equalsIgnoreCase("A")) {
                                ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_CUST_DEFN_FLD", false);
                            }
                                 System.out.println("The above record is a LINK/UNLINK case");
                        } else { // NOT LINK/UNLINK cases
                            ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_CUST_DEFN_FLD", false);
                        }
                    } else { //PHYSICAL DELETE
                        ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_CUST_DEFN_FLD", true);
                    }
                } catch (Exception e) {
                    log.info("Exception in Table CNSM_CUST_DEFN_FLD :::" + key.toString());
                    e.printStackTrace();
                }
                return KeyValue.pair(key, value);

            });
        } catch (Exception e) {
            e.printStackTrace();
        }


        return memberKStream;
    }

    @Bean
    public KStream<CNSM_COV_CUST_DEFN_FLD_KEY, com.optum.attunity.cdb.model.CNSM_COV_CUST_DEFN_FLD> cnsmCovCustDefnFldStream(StreamsBuilder builder) {


        KStream<CNSM_COV_CUST_DEFN_FLD_KEY, com.optum.attunity.cdb.model.CNSM_COV_CUST_DEFN_FLD>
                memberKStream = builder.stream(topicConfig.getCnsmCovCustDefnFld());

        try {
            memberKStream.map((key, value) -> {
                final int[] retryCount = {0};
                log.info("CNSM_COV_CUST_DEFN_FLD Consuming key  :::" + key.toString());
                log.info("CNSM_COV_CUST_DEFN_FLD Consuming value  :::" + value.toString());

                try {
                    if (!value.getHeaders().getOperation().name().equals("DELETE")) { //INSERT, UPDATE
                        if (value.getCNSMCOVCUSTDEFNFLDDATA().getROWUSERID().trim().equalsIgnoreCase("LINK/UNLINK")) {
                            if (!value.getCNSMCOVCUSTDEFNFLDDATA().getROWSTSCD().trim().equalsIgnoreCase("D")
                                    && !value.getCNSMCOVCUSTDEFNFLDDATA().getROWSTSCD().trim().equalsIgnoreCase("A")) {
                                ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_COV_CUST_DEFN_FLD", false);
                            }
                             System.out.println("The above record is a LINK/UNLINK case");
                        } else { // NOT LINK/UNLINK cases
                            ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_COV_CUST_DEFN_FLD", false);
                        }
                    } else { //PHYSICAL DELETE
                        ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_COV_CUST_DEFN_FLD", true);
                    }
                } catch (Exception e) {
                    log.info("Exception in Table CNSM_COV_CUST_DEFN_FLD :::" + key.toString());
                    e.printStackTrace();
                }
                return KeyValue.pair(key, value);

            });
        } catch (Exception e) {
            e.printStackTrace();
        }

        return memberKStream;
    }


    @Bean
    public KStream<CNSM_EFT_KEY, com.optum.attunity.cdb.model.CNSM_EFT> cnsmEftStream(StreamsBuilder builder) {


        KStream<CNSM_EFT_KEY, com.optum.attunity.cdb.model.CNSM_EFT>
                memberKStream = builder.stream(topicConfig.getCnsmEft());

        try {
            memberKStream.map((key, value) -> {
                final int[] retryCount = {0};
                log.info("CNSM_EFT Consuming key  :::" + key.toString());
                log.info("CNSM_EFT Consuming value  :::" + value.toString());

                try {
                    if (!value.getHeaders().getOperation().name().equals("DELETE")) { //INSERT, UPDATE
                        if (value.getCNSMEFTDATA().getROWUSERID().trim().equalsIgnoreCase("LINK/UNLINK")) {
                            if (!value.getCNSMEFTDATA().getROWSTSCD().trim().equalsIgnoreCase("D")
                                    && !value.getCNSMEFTDATA().getROWSTSCD().trim().equalsIgnoreCase("A")) {
                                ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_EFT", false);
                            }
                             System.out.println("The above record is a LINK/UNLINK case");
                        } else { // NOT LINK/UNLINK cases
                            ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_EFT", false);
                        }
                    } else { //PHYSICAL DELETE
                        ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_EFT", true);
                    }
                } catch (Exception e) {
                    log.info("Exception in Table CNSM_EFT :::" + key.toString());
                    e.printStackTrace();
                }
                return KeyValue.pair(key, value);

            });
        } catch (Exception e) {
            e.printStackTrace();
        }


        return memberKStream;
    }

    @Bean
    public KStream<CNSM_MDCR_ELIG_KEY, com.optum.attunity.cdb.model.CNSM_MDCR_ELIG> cnsmMdcrEligStream(StreamsBuilder builder) {


        KStream<CNSM_MDCR_ELIG_KEY, com.optum.attunity.cdb.model.CNSM_MDCR_ELIG>
                memberKStream = builder.stream(topicConfig.getCnsmMdcrElig());

        try {
            memberKStream.map((key, value) -> {
                final int[] retryCount = {0};
                log.info("CNSM_MDCR_ELIG Consuming key  :::" + key.toString());
                log.info("CNSM_MDCR_ELIG Consuming value  :::" + value.toString());

                try {
                    if (!value.getHeaders().getOperation().name().equals("DELETE")) { //INSERT, UPDATE
                        if (value.getCNSMMDCRELIGDATA().getROWUSERID().trim().equalsIgnoreCase("LINK/UNLINK")) {
                            if (!value.getCNSMMDCRELIGDATA().getROWSTSCD().trim().equalsIgnoreCase("D")
                                    && !value.getCNSMMDCRELIGDATA().getROWSTSCD().trim().equalsIgnoreCase("A")) {
                                ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_MDCR_ELIG", false);
                            }
                            System.out.println("The above record is a LINK/UNLINK case");
                        } else { // NOT LINK/UNLINK cases
                            ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_MDCR_ELIG", false);
                        }
                    } else { //PHYSICAL DELETE
                        ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_MDCR_ELIG", true);
                    }
                } catch (Exception e) {
                    log.info("Exception in Table CNSM_MDCR_ELIG :::" + key.toString());
                    e.printStackTrace();
                }
                return KeyValue.pair(key, value);

            });
        } catch (Exception e) {
            e.printStackTrace();
        }


        return memberKStream;
    }


    @Bean
    public KStream<CNSM_MDCR_PRIMACY_KEY, com.optum.attunity.cdb.model.CNSM_MDCR_PRIMACY> cnsmMdcrPrimacyStream(StreamsBuilder builder) {


        KStream<CNSM_MDCR_PRIMACY_KEY, com.optum.attunity.cdb.model.CNSM_MDCR_PRIMACY>
                memberKStream = builder.stream(topicConfig.getCnsmMdcrPrimacy());

        try {
            memberKStream.map((key, value) -> {
                final int[] retryCount = {0};
                log.info("CNSM_MDCR_PRIMACY Consuming key  :::" + key.toString());
                log.info("CNSM_MDCR_PRIMACY Consuming value  :::" + value.toString());

                try {
                    if (!value.getHeaders().getOperation().name().equals("DELETE")) { //INSERT, UPDATE
                        if (value.getCNSMMDCRPRIMACYDATA().getROWUSERID().trim().equalsIgnoreCase("LINK/UNLINK")) {
                            if (!value.getCNSMMDCRPRIMACYDATA().getROWSTSCD().trim().equalsIgnoreCase("D")
                                    && !value.getCNSMMDCRPRIMACYDATA().getROWSTSCD().trim().equalsIgnoreCase("A")) {
                                ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_MDCR_PRIMACY", false);
                            }
                            System.out.println("The above record is a LINK/UNLINK case");
                        } else { // NOT LINK/UNLINK cases
                            ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_MDCR_PRIMACY", false);
                        }
                    } else { //PHYSICAL DELETE
                        ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_MDCR_PRIMACY", true);
                    }
                } catch (Exception e) {
                    log.info("Exception in Table CNSM_MDCR_PRIMACY :::" + key.toString());
                    e.printStackTrace();
                }
                return KeyValue.pair(key, value);

            });
        } catch (Exception e) {
            e.printStackTrace();
        }


        return memberKStream;
    }


    @Bean
    public KStream<CNSM_PRXST_COND_KEY, com.optum.attunity.cdb.model.CNSM_PRXST_COND> cnsmPrxstCondStream(StreamsBuilder builder) {


        KStream<CNSM_PRXST_COND_KEY, com.optum.attunity.cdb.model.CNSM_PRXST_COND>
                memberKStream = builder.stream(topicConfig.getCnsmPrxstCond());

        try {
            memberKStream.map((key, value) -> {
                final int[] retryCount = {0};
                log.info("CNSM_PRXST_COND Consuming key  :::" + key.toString());
                log.info("CNSM_PRXST_COND Consuming value  :::" + value.toString());

                try {
                    if (!value.getHeaders().getOperation().name().equals("DELETE")) { //INSERT, UPDATE
                        if (value.getCNSMPRXSTCONDDATA().getROWUSERID().trim().equalsIgnoreCase("LINK/UNLINK")) {
                            if (!value.getCNSMPRXSTCONDDATA().getROWSTSCD().trim().equalsIgnoreCase("D")
                                    && !value.getCNSMPRXSTCONDDATA().getROWSTSCD().trim().equalsIgnoreCase("A")) {
                                ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_PRXST_COND", false);
                            }
                            System.out.println("The above record is a LINK/UNLINK case");
                        } else { // NOT LINK/UNLINK cases
                            ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_PRXST_COND", false);
                        }
                    } else { //PHYSICAL DELETE
                        ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_PRXST_COND", true);
                    }
                } catch (Exception e) {
                    log.info("Exception in Table CNSM_PRXST_COND :::" + key.toString());
                    e.printStackTrace();
                }
                return KeyValue.pair(key, value);

            });
        } catch (Exception e) {
            e.printStackTrace();
        }


        return memberKStream;
    }


    @Bean
    public KStream<CNSM_SLRY_BAS_DED_OOP_KEY, com.optum.attunity.cdb.model.CNSM_SLRY_BAS_DED_OOP> cnsmSlryBasDedOopStream(StreamsBuilder builder) {


        KStream<CNSM_SLRY_BAS_DED_OOP_KEY, com.optum.attunity.cdb.model.CNSM_SLRY_BAS_DED_OOP>
                memberKStream = builder.stream(topicConfig.getCnsmSlryBasDedOop());

        try {
            memberKStream.map((key, value) -> {
                final int[] retryCount = {0};
                log.info("CNSM_SLRY_BAS_DED_OOP Consuming key  :::" + key.toString());
                log.info("CNSM_SLRY_BAS_DED_OOP Consuming value  :::" + value.toString());

                BigDecimal salaryYear = consumerUtil.convertByteToDecimal(value, 4, 4);
                try {
                    if (!value.getHeaders().getOperation().name().equals("DELETE")) { //INSERT, UPDATE
                        if (value.getCNSMSLRYBASDEDOOPDATA().getROWUSERID().trim().equalsIgnoreCase("LINK/UNLINK")) {
                            if (!value.getCNSMSLRYBASDEDOOPDATA().getROWSTSCD().trim().equalsIgnoreCase("D")
                                    && !value.getCNSMSLRYBASDEDOOPDATA().getROWSTSCD().trim().equalsIgnoreCase("A")) {
                                ingestionLogDAO.ingestDataToIngestionLog(value.toString(), value, salaryYear.toString(), false);
                            }
                        } else { // NOT LINK/UNLINK cases
                            ingestionLogDAO.ingestDataToIngestionLog(value.toString(), value, salaryYear.toString(), false);
                        }
                    } else { //PHYSICAL DELETE
                        ingestionLogDAO.ingestDataToIngestionLog(value.toString(), value, salaryYear.toString(), true);
                    }
                } catch (Exception e) {
                    log.info("Exception in Table CNSM_SLRY_BAS_DED_OOP :::" + key.toString());
                    e.printStackTrace();
                }
                return KeyValue.pair(key, value);

            });
        } catch (Exception e) {
            e.printStackTrace();
        }


        return memberKStream;
    }


    @Bean
    public KStream<COV_INFO_KEY, com.optum.attunity.cdb.model.COV_INFO> covInfoStream(StreamsBuilder builder) {


        KStream<COV_INFO_KEY, com.optum.attunity.cdb.model.COV_INFO>
                memberKStream = builder.stream(topicConfig.getCovInfo());

        try {
            memberKStream.map((key, value) -> {
                final int[] retryCount = {0};
                log.info("COV_INFO Consuming key  :::" + key.toString());
                log.info("COV_INFO Consuming value  :::" + value.toString());

                try {
                    if (!value.getHeaders().getOperation().name().equals("DELETE")) { //INSERT, UPDATE
                        if (value.getCOVINFODATA().getROWUSERID().trim().equalsIgnoreCase("LINK/UNLINK")) {
                            if (!value.getCOVINFODATA().getROWSTSCD().trim().equalsIgnoreCase("D")
                                    && !value.getCOVINFODATA().getROWSTSCD().trim().equalsIgnoreCase("A")) {
                                ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "COV_INFO", false);
                            }
                            System.out.println("The above record is a LINK/UNLINK case");
                        } else { // NOT LINK/UNLINK cases
                            ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "COV_INFO", false);
                        }
                    } else { //PHYSICAL DELETE
                        ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "COV_INFO", true);
                    }
                } catch (Exception e) {
                    log.info("Exception in Table COV_INFO :::" + key.toString());
                    e.printStackTrace();
                }
                return KeyValue.pair(key, value);

            });
        } catch (Exception e) {
            e.printStackTrace();
        }


        return memberKStream;
    }

    @Bean
    public KStream<CUST_INFO_KEY, com.optum.attunity.cdb.model.CUST_INFO> custInfoStream(StreamsBuilder builder) {


        KStream<CUST_INFO_KEY, com.optum.attunity.cdb.model.CUST_INFO>
                memberKStream = builder.stream(topicConfig.getCustInfo());

        try {
            memberKStream.map((key, value) -> {
                final int[] retryCount = {0};
                log.info("CUST_INFO Consuming key  :::" + key.toString());
                log.info("CUST_INFO Consuming value  :::" + value.toString());

                try {
                    if (!value.getHeaders().getOperation().name().equals("DELETE")) { //INSERT, UPDATE
                        if (value.getCUSTINFODATA().getROWUSERID().trim().equalsIgnoreCase("LINK/UNLINK")) {
                            if (!value.getCUSTINFODATA().getROWSTSCD().trim().equalsIgnoreCase("D")
                                    && !value.getCUSTINFODATA().getROWSTSCD().trim().equalsIgnoreCase("A")) {
                                ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CUST_INFO", false);
                            }
                             System.out.println("The above record is a LINK/UNLINK case");
                        } else { // NOT LINK/UNLINK cases
                            ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CUST_INFO", false);
                        }
                    } else { //PHYSICAL DELETE
                        ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CUST_INFO", true);
                    }
                } catch (Exception e) {
                    log.info("Exception in Table CUST_INFO :::" + key.toString());
                    e.printStackTrace();
                }
                return KeyValue.pair(key, value);

            });
        } catch (Exception e) {
            e.printStackTrace();
        }


        return memberKStream;
    }

    @Bean
    public KStream<PLN_BEN_SET_KEY, com.optum.attunity.cdb.model.PLN_BEN_SET> plnBenSet(StreamsBuilder builder) {


        KStream<PLN_BEN_SET_KEY, com.optum.attunity.cdb.model.PLN_BEN_SET>
                memberKStream = builder.stream(topicConfig.getPlnBenSet());

        try {
            memberKStream.map((key, value) -> {
                final int[] retryCount = {0};
                log.info("PLN_BEN_SET Consuming key  :::" + key.toString());
                log.info("PLN_BEN_SET Consuming value  :::" + value.toString());

                try {
                    if (!value.getHeaders().getOperation().name().equals("DELETE")) { //INSERT, UPDATE
                        if (value.getPLNBENSETDATA().getROWUSERID().trim().equalsIgnoreCase("LINK/UNLINK")) {
                            if (!value.getPLNBENSETDATA().getROWSTSCD().trim().equalsIgnoreCase("D")
                                    && !value.getPLNBENSETDATA().getROWSTSCD().trim().equalsIgnoreCase("A")) {
                                ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "PLN_BEN_SET", false);
                            }
                             System.out.println("The above record is a LINK/UNLINK case");
                        } else { // NOT LINK/UNLINK cases
                            ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "PLN_BEN_SET", false);
                        }
                    } else { //PHYSICAL DELETE
                        ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "PLN_BEN_SET", true);
                    }
                } catch (Exception e) {
                    log.info("Exception in Table PLN_BEN_SET :::" + key.toString());
                    e.printStackTrace();
                }
                return KeyValue.pair(key, value);

            });
        } catch (Exception e) {
            e.printStackTrace();
        }


        return memberKStream;
    }

    @Bean
    public KStream<PLN_BEN_SET_DET_KEY, com.optum.attunity.cdb.model.PLN_BEN_SET_DET> plnBenSetDetStream(StreamsBuilder builder) {


        KStream<PLN_BEN_SET_DET_KEY, com.optum.attunity.cdb.model.PLN_BEN_SET_DET>
                memberKStream = builder.stream(topicConfig.getPlnBenSetDet());

        try {
            memberKStream.map((key, value) -> {
                final int[] retryCount = {0};
                log.info("PLN_BEN_SET_DET Consuming key  :::" + key.toString());
                log.info("PLN_BEN_SET_DET Consuming value  :::" + value.toString());

                try {
                    if (!value.getHeaders().getOperation().name().equals("DELETE")) { //INSERT, UPDATE
                        if (value.getPLNBENSETDETDATA().getROWUSERID().trim().equalsIgnoreCase("LINK/UNLINK")) {
                            if (!value.getPLNBENSETDETDATA().getROWSTSCD().trim().equalsIgnoreCase("D")
                                    && !value.getPLNBENSETDETDATA().getROWSTSCD().trim().equalsIgnoreCase("A")) {
                                ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "PLN_BEN_SET_DET", false);
                            }
                             System.out.println("The above record is a LINK/UNLINK case");
                        } else { // NOT LINK/UNLINK cases
                            ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "PLN_BEN_SET_DET", false);
                        }
                    } else { //PHYSICAL DELETE
                        ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "PLN_BEN_SET_DET", true);
                    }
                } catch (Exception e) {
                    log.info("Exception in Table PLN_BEN_SET_DET :::" + key.toString());
                    e.printStackTrace();
                }
                return KeyValue.pair(key, value);

            });
        } catch (Exception e) {
            e.printStackTrace();
        }


        return memberKStream;
    }


    @Bean
    public KStream<POL_INFO_KEY, com.optum.attunity.cdb.model.POL_INFO> polInfoStream(StreamsBuilder builder) {


        KStream<POL_INFO_KEY, com.optum.attunity.cdb.model.POL_INFO>
                memberKStream = builder.stream(topicConfig.getPolInfo());

        try {
            memberKStream.map((key, value) -> {
                final int[] retryCount = {0};
                log.info("POL_INFO Consuming key  :::" + key.toString());
                log.info("POL_INFO Consuming value  :::" + value.toString());

                try {
                    if (!value.getHeaders().getOperation().name().equals("DELETE")) { //INSERT, UPDATE
                        if (value.getPOLINFODATA().getROWUSERID().trim().equalsIgnoreCase("LINK/UNLINK")) {
                            if (!value.getPOLINFODATA().getROWSTSCD().trim().equalsIgnoreCase("D")
                                    && !value.getPOLINFODATA().getROWSTSCD().trim().equalsIgnoreCase("A")) {
                                ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "POL_INFO", false);
                            }
                             System.out.println("The above record is a LINK/UNLINK case");
                        } else { // NOT LINK/UNLINK cases
                            ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "POL_INFO", false);
                        }
                    } else { //PHYSICAL DELETE
                        ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "POL_INFO", true);
                    }
                } catch (Exception e) {
                    log.info("Exception in Table POL_INFO :::" + key.toString());
                    e.printStackTrace();
                }
                return KeyValue.pair(key, value);

            });
        } catch (Exception e) {
            e.printStackTrace();
        }


        return memberKStream;
    }


    @Bean
    public KStream<RX_BEN_SET_DET_KEY, com.optum.attunity.cdb.model.RX_BEN_SET_DET> rxBenSetDetStream(StreamsBuilder builder) {


        KStream<RX_BEN_SET_DET_KEY, com.optum.attunity.cdb.model.RX_BEN_SET_DET>
                memberKStream = builder.stream(topicConfig.getRxBenSetDet());

        try {
            KStream stream = memberKStream.map((key, value) -> {
                final int[] retryCount = {0};
                log.info("RX_BEN_SET_DET Consuming key  :::" + key.toString());
                log.info("RX_BEN_SET_DET Consuming value  :::" + value.toString());

                try {
                    if (!value.getHeaders().getOperation().name().equals("DELETE")) { //INSERT, UPDATE
                        if (value.getRXBENSETDETDATA().getROWUSERID().trim().equalsIgnoreCase("LINK/UNLINK")) {
                            if (!value.getRXBENSETDETDATA().getROWSTSCD().trim().equalsIgnoreCase("D")
                                    && !value.getRXBENSETDETDATA().getROWSTSCD().trim().equalsIgnoreCase("A")) {
                                ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "RX_BEN_SET_DET", false);
                            }
                             System.out.println("The above record is a LINK/UNLINK case");
                        } else { // NOT LINK/UNLINK cases
                            ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "RX_BEN_SET_DET", false);
                        }
                    } else { //PHYSICAL DELETE
                        ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "RX_BEN_SET_DET", true);
                    }
                } catch (Exception e) {
                    log.info("Exception in Table RX_BEN_SET_DET :::" + key.toString());
                    e.printStackTrace();
                }
                return KeyValue.pair(key, value);

            });
        } catch (Exception e) {
            e.printStackTrace();
        }


        return memberKStream;
    }
*//*

}


*/
