package com.optum.exts.cdb.stream.streams;

import com.optum.attunity.cdb.model.*;
import com.optum.exts.cdb.stream.config.TopicConfig;
import com.optum.exts.cdb.stream.transformer.IngestionLogDAO;
import com.optum.exts.cdb.stream.transformer.MetadataTransformer;
import com.optum.exts.cdb.stream.utility.ConsumerUtil;
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
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by amolugu1
 */
@Configuration
@ConfigurationProperties(prefix = "streams.member")
//@Profile({"local", "member"})
public class SourceCSBStreams_new {

    private static final Logger log = LoggerFactory.getLogger(SourceCSBStreams_new.class);

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

        /*FixedBackOffPolicy fixedBackOffPolicy = new FixedBackOffPolicy();
        fixedBackOffPolicy.setBackOffPeriod(1000*60);
        retryTemplate.setBackOffPolicy(fixedBackOffPolicy);
*/
        SimpleRetryPolicy retryPolicy = new SimpleRetryPolicy();
        retryPolicy.setMaxAttempts(10);

        retryTemplate.setRetryPolicy(retryPolicy);
        retryTemplate.setThrowLastExceptionOnExhausted(true);

        return retryTemplate;
    }

   @Bean
    public KStream<L_CNSM_SRCH_KEY, L_CNSM_SRCH> lCNSMSrchStream(StreamsBuilder builder) {

        KStream<L_CNSM_SRCH_KEY, L_CNSM_SRCH>
                memberKStream = builder.stream(topicConfig.getLcnsmSrch());

        try {
            memberKStream.map((key, value) -> {
                log.info("L_CNSM_SRCH Consuming key  :::" + key.toString());
                //log.info("L_CNSM_SRCH Consuming value  :::" + value.toString());
                final int[] retryCount = {0};
                //log.info("L_CNSM_SRCH Consuming Value  :::" + value.toString());

             try {
                 if (!value.getHeaders().getOperation().name().equals("REFRESH")) {
                     if (!value.getHeaders().getOperation().name().equals("DELETE")) { //INSERT, UPDATE
                         if (value.getLCNSMSRCHDATA().getROWUSERID().trim().equalsIgnoreCase("LINK/UNLINK")) {
                             if (!value.getLCNSMSRCHDATA().getROWSTSCD().trim().equalsIgnoreCase("D")
                                     && !value.getLCNSMSRCHDATA().getROWSTSCD().trim().equalsIgnoreCase("A")) {
                                 ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "L_CNSM_SRCH", false);
                             }

                             log.info("L_CNSM_SRCH LINK/UNLINK case key  :::" + key.toString());
                         } else { // NOT LINK/UNLINK cases
                             ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "L_CNSM_SRCH", false);
                             log.info("L_CNSM_SRCH written key  :::" + key.toString());
                         }
                     } else { //PHYSICAL DELETE
                         ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "L_CNSM_SRCH", true);
                         log.info("L_CNSM_SRCH physical delete key  :::" + key.toString());

                     }
                 }
                 return KeyValue.pair(key, value);

             }

             catch (Exception e) {
                 try {
                     retryTemplate().execute(new RetryCallback<KeyValue<L_CNSM_SRCH_KEY, L_CNSM_SRCH>, Exception>() {
                         @Override
                         public KeyValue<L_CNSM_SRCH_KEY, L_CNSM_SRCH> doWithRetry(RetryContext context) throws Exception {
                             retryCount[0]++;
                             log.info("************************************ RETRY COUNT : " + retryCount[0]+ ":::::::" + key.toString());
                             //throw new SQLException("");
                             if (!value.getHeaders().getOperation().name().equals("REFRESH")) {
                                 if (!value.getHeaders().getOperation().name().equals("DELETE")) { //INSERT, UPDATE
                                     if (value.getLCNSMSRCHDATA().getROWUSERID().trim().equalsIgnoreCase("LINK/UNLINK")) {
                                         if (!value.getLCNSMSRCHDATA().getROWSTSCD().trim().equalsIgnoreCase("D")
                                                 && !value.getLCNSMSRCHDATA().getROWSTSCD().trim().equalsIgnoreCase("A")) {
                                             ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "L_CNSM_SRCH", false);
                                         }
                                         log.info("L_CNSM_SRCH LINK/UNLINK case key  :::" + key.toString());
                                     } else { // NOT LINK/UNLINK cases
                                         ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "L_CNSM_SRCH", false);
                                         log.info("L_CNSM_SRCH written key  :::" + key.toString());
                                     }
                                 } else { //PHYSICAL DELETE
                                     ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "L_CNSM_SRCH", true);
                                     log.info("L_CNSM_SRCH physical delete key  :::" + key.toString());
                                 }
                             }
                             return KeyValue.pair(key, value);
                         }
                     });
                 } catch (Throwable throwable) {

                     log.error("SQLException in Table L_CNSM_SRCH :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());

                    // log.error("SQLException in Table L_CNSM_SRCH :::time::::" + sdf.format(new Date()) + ":::::::" + new MetadataTransformer());

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

   @Bean
    public KStream<L_COV_PRDT_DT_KEY, L_COV_PRDT_DT> lCovPrdtDtStream(StreamsBuilder builder) {

        KStream<L_COV_PRDT_DT_KEY, L_COV_PRDT_DT>
                memberKStream = builder.stream(topicConfig.getLcovPrdtdt());

        try {
            memberKStream.map((key, value) -> {
                final int[] retryCount = {0};

                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

                log.info("L_COV_PRDT_DT Consuming key +++ ::time::::" + sdf.format(new Date()) + ":::::::" + key);
                //log.info("L_COV_PRDT_DT Consuming value +++ ::time::::" + sdf.format(new Date()) + ":::::::" + value.toString());
                try {


                    if (!value.getHeaders().getOperation().name().equals("REFRESH")) {
                        if (!value.getHeaders().getOperation().name().equals("DELETE")) { //INSERT, UPDATE
                            if (value.getLCOVPRDTDTDATA().getROWUSERID().trim().equalsIgnoreCase("LINK/UNLINK")) {
                                if (!value.getLCOVPRDTDTDATA().getROWSTSCD().trim().equalsIgnoreCase("D")
                                        && !value.getLCOVPRDTDTDATA().getROWSTSCD().trim().equalsIgnoreCase("A")) {
                                    ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "L_COV_PRDT_DT", false);
                                }
                                log.info("L_COV_PRDT_DT LINK/UNLINK case key  :::" + key.toString());
                            } else { // NOT LINK/UNLINK cases
                                ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "L_COV_PRDT_DT", false);
                                log.info("L_COV_PRDT_DT written key  :::" + key.toString());
                            }
                        } else { //PHYSICAL DELETE
                            ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "L_COV_PRDT_DT", true);
                            log.info("L_COV_PRDT_DT physical delete key  :::" + key.toString());
                        }
                    }
                }      catch (Exception e) {
                    try {
                        retryTemplate().execute(new RetryCallback<KeyValue<L_COV_PRDT_DT_KEY, L_COV_PRDT_DT>, Exception>() {
                            @Override
                            public KeyValue<L_COV_PRDT_DT_KEY, L_COV_PRDT_DT> doWithRetry(RetryContext context) throws Exception {
                                retryCount[0]++;
                                log.info("************************************ RETRY COUNT : " + retryCount[0]+ ":::::::" + key.toString());
                                //throw new SQLException("");
                                if (!value.getHeaders().getOperation().name().equals("REFRESH")) {
                                    if (!value.getHeaders().getOperation().name().equals("DELETE")) { //INSERT, UPDATE
                                        if (value.getLCOVPRDTDTDATA().getROWUSERID().trim().equalsIgnoreCase("LINK/UNLINK")) {
                                            if (!value.getLCOVPRDTDTDATA().getROWSTSCD().trim().equalsIgnoreCase("D")
                                                    && !value.getLCOVPRDTDTDATA().getROWSTSCD().trim().equalsIgnoreCase("A")) {
                                                ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "L_COV_PRDT_DT", false);
                                            }
                                            log.info("L_COV_PRDT_DT LINK/UNLINK case key  :::" + key.toString());
                                        } else { // NOT LINK/UNLINK cases
                                            ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "L_COV_PRDT_DT", false);
                                            log.info("L_COV_PRDT_DT written key  :::" + key.toString());
                                        }
                                    } else { //PHYSICAL DELETE
                                        ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "L_COV_PRDT_DT", true);
                                        log.info("L_COV_PRDT_DT physical delete key  :::" + key.toString());
                                    }
                                }
                                return KeyValue.pair(key, value);
                            }
                        });
                    } catch (Throwable throwable) {

                        log.error("SQLException in Table L_COV_PRDT_DT :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());

                        //log.error("SQLException in Table L_COV_PRDT_DT :::time::::" + sdf.format(new Date()) + ":::::::" + new MetadataTransformer());

                        log.error("Exception",throwable);
                        //throw new RuntimeException("");
                        log.error("Restarting L_COV_PRDT_DT POD"); System.exit(1);
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

    @Bean
    public KStream<L_HLT_SRV_DT_KEY, L_HLT_SRV_DT> lHltSrvDtStream(StreamsBuilder builder) {

        KStream<L_HLT_SRV_DT_KEY, L_HLT_SRV_DT>
                memberKStream = builder.stream(topicConfig.getlHltSrvDt());

        try {
            memberKStream.map((key, value) -> {
                final int[] retryCount = {0};

                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

                log.info("L_HLT_SRV_DT Consuming key  ::time::::" + sdf.format(new Date()) + ":::::::" + key);
               // log.info("L_HLT_SRV_DT Consuming value +++ ::time::::" + sdf.format(new Date()) + ":::::::" + value.toString());

                try {
                    if (!value.getHeaders().getOperation().name().equals("REFRESH")) {
                        if (!value.getHeaders().getOperation().name().equals("DELETE")) { //INSERT, UPDATE
                            if (value.getLHLTSRVDTDATA().getROWUSERID().trim().equalsIgnoreCase("LINK/UNLINK")) {
                                if (!value.getLHLTSRVDTDATA().getROWSTSCD().trim().equalsIgnoreCase("D")
                                        && !value.getLHLTSRVDTDATA().getROWSTSCD().trim().equalsIgnoreCase("A")) {
                                    ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "L_HLT_SRV_DT", false);
                                }
                                log.info("L_HLT_SRV_DT LINK/UNLINK case key  :::" + key.toString());
                            } else { // NOT LINK/UNLINK cases
                                ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "L_HLT_SRV_DT", false);
                                log.info("L_HLT_SRV_DT written key  :::" + key.toString());
                            }
                        } else { //PHYSICAL DELETE
                            ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "L_HLT_SRV_DT", true);
                            log.info("L_HLT_SRV_DT physical delete key  :::" + key.toString());
                        }
                    }

                }     catch (Exception e) {
                    try {
                        retryTemplate().execute(new RetryCallback<KeyValue<L_HLT_SRV_DT_KEY, L_HLT_SRV_DT>, Exception>() {
                            @Override
                            public KeyValue<L_HLT_SRV_DT_KEY, L_HLT_SRV_DT> doWithRetry(RetryContext context) throws Exception {
                                retryCount[0]++;
                                log.info("************************************ RETRY COUNT : " + retryCount[0]+ ":::::::" + key.toString());
                                //throw new SQLException("");
                                if (!value.getHeaders().getOperation().name().equals("REFRESH")) {
                                    if (!value.getHeaders().getOperation().name().equals("DELETE")) { //INSERT, UPDATE
                                        if (value.getLHLTSRVDTDATA().getROWUSERID().trim().equalsIgnoreCase("LINK/UNLINK")) {
                                            if (!value.getLHLTSRVDTDATA().getROWSTSCD().trim().equalsIgnoreCase("D")
                                                    && !value.getLHLTSRVDTDATA().getROWSTSCD().trim().equalsIgnoreCase("A")) {
                                                ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "L_HLT_SRV_DT", false);
                                            }
                                            log.info("L_HLT_SRV_DT LINK/UNLINK case key  :::" + key.toString());
                                        } else { // NOT LINK/UNLINK cases
                                            ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "L_HLT_SRV_DT", false);
                                            log.info("L_HLT_SRV_DT written key  :::" + key.toString());
                                        }
                                    } else { //PHYSICAL DELETE
                                        ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "L_HLT_SRV_DT", true);
                                        log.info("L_HLT_SRV_DT physical delete key  :::" + key.toString());
                                    }
                                }

                                return KeyValue.pair(key, value);
                            }
                        });
                    } catch (Throwable throwable) {

                        log.error("SQLException in Table L_HLT_SRV_DT :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());

                       // log.error("SQLException in Table L_HLT_SRV_DT :::time::::" + sdf.format(new Date()) + ":::::::" + new MetadataTransformer());

                        log.error("Exception",throwable);
                        //throw new RuntimeException("");
                        log.error("Restarting L_HLT_SRV_DT POD"); System.exit(1);
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

    @Bean
    public KStream<CNSM_STS_KEY, CNSM_STS> cnsmStsStream(StreamsBuilder builder) {


        KStream<CNSM_STS_KEY, CNSM_STS>
                memberKStream = builder.stream(topicConfig.getCnsmSts());

        try {
            memberKStream.map((key, value) -> {
                final int[] retryCount = {0};
                log.info("CNSM_STS Consuming key  :::" + key.toString());
               // log.info("CNSM_STS Consuming value  :::" + value.toString());

                try {
                    if (!value.getHeaders().getOperation().name().equals("REFRESH")) {
                        if (!value.getHeaders().getOperation().name().equals("DELETE")) { //INSERT, UPDATE
                            if (value.getCNSMSTSDATA().getROWUSERID().trim().equalsIgnoreCase("LINK/UNLINK")) {
                                if (!value.getCNSMSTSDATA().getROWSTSCD().trim().equalsIgnoreCase("D")
                                        && !value.getCNSMSTSDATA().getROWSTSCD().trim().equalsIgnoreCase("A")) {
                                    ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_STS", false);
                                }
                                log.info("CNSM_STS LINK/UNLINK case key  :::" + key.toString());
                            } else { // NOT LINK/UNLINK cases
                                ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_STS", false);
                                log.info("CNSM_STS written key  :::" + key.toString());

                            }
                        } else { //PHYSICAL DELETE
                            ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_STS", true);
                            log.info("CNSM_STS physical delete key  :::" + key.toString());
                        }
                    }
                }      catch (Exception e) {
                    try {
                        retryTemplate().execute(new RetryCallback<KeyValue<CNSM_STS_KEY, CNSM_STS>, Exception>() {
                            @Override
                            public KeyValue<CNSM_STS_KEY, CNSM_STS> doWithRetry(RetryContext context) throws Exception {
                                retryCount[0]++;
                                log.info("************************************ RETRY COUNT : " + retryCount[0]+ ":::::::" + key.toString());
                                //throw new SQLException("");
                                if (!value.getHeaders().getOperation().name().equals("REFRESH")) {
                                    if (!value.getHeaders().getOperation().name().equals("DELETE")) { //INSERT, UPDATE
                                        if (value.getCNSMSTSDATA().getROWUSERID().trim().equalsIgnoreCase("LINK/UNLINK")) {
                                            if (!value.getCNSMSTSDATA().getROWSTSCD().trim().equalsIgnoreCase("D")
                                                    && !value.getCNSMSTSDATA().getROWSTSCD().trim().equalsIgnoreCase("A")) {
                                                ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_STS", false);
                                            }
                                            log.info("CNSM_STS LINK/UNLINK case key  :::" + key.toString());
                                        } else { // NOT LINK/UNLINK cases
                                            ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_STS", false);
                                            log.info("CNSM_STS written key  :::" + key.toString());

                                        }
                                    } else { //PHYSICAL DELETE
                                        ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_STS", true);
                                        log.info("CNSM_STS physical delete key  :::" + key.toString());
                                    }
                                }

                                return KeyValue.pair(key, value);
                            }
                        });
                    } catch (Throwable throwable) {

                        log.error("SQLException in Table CNSM_STS :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());

                       // log.error("SQLException in Table CNSM_STS :::time::::" + sdf.format(new Date()) + ":::::::" + new MetadataTransformer());

                        log.error("Exception",throwable);
                        //throw new RuntimeException("");
                        log.error("Restarting CNSM_STS POD"); System.exit(1);
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



    @Bean
    public KStream<CNSM_MDCR_ENRL_KEY, CNSM_MDCR_ENRL> cnsmMdcrEnrlStream(StreamsBuilder builder) {


        KStream<CNSM_MDCR_ENRL_KEY, CNSM_MDCR_ENRL>
                memberKStream = builder.stream(topicConfig.getCnsmMdcrEnrl());

        try {
            memberKStream.map((key, value) -> {
                final int[] retryCount = {0};
                log.info("CNSM_MDCR_ENRL Consuming key  :::" + key.toString());
               // log.info("CNSM_MDCR_ENRL Consuming value  :::" + value.toString());

                try {
                    if (!value.getHeaders().getOperation().name().equals("REFRESH")) {
                        if (!value.getHeaders().getOperation().name().equals("DELETE")) { //INSERT, UPDATE
                            if (value.getCNSMMDCRENRLDATA().getROWUSERID().trim().equalsIgnoreCase("LINK/UNLINK")) {
                                if (!value.getCNSMMDCRENRLDATA().getROWSTSCD().trim().equalsIgnoreCase("D")
                                        && !value.getCNSMMDCRENRLDATA().getROWSTSCD().trim().equalsIgnoreCase("A")) {
                                    ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_MDCR_ENRL", false);
                                }
                                log.info("CNSM_MDCR_ENRL LINK/UNLINK case key  :::" + key.toString());
                            } else { // NOT LINK/UNLINK cases
                                ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_MDCR_ENRL", false);
                                log.info("CNSM_MDCR_ENRL written key  :::" + key.toString());
                            }
                        } else { //PHYSICAL DELETE
                            ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_MDCR_ENRL", true);
                            log.info("CNSM_MDCR_ENRL physical delete key  :::" + key.toString());
                        }
                    }
                }      catch (Exception e) {
                    try {
                        retryTemplate().execute(new RetryCallback<KeyValue<CNSM_MDCR_ENRL_KEY, CNSM_MDCR_ENRL>, Exception>() {
                            @Override
                            public KeyValue<CNSM_MDCR_ENRL_KEY, CNSM_MDCR_ENRL> doWithRetry(RetryContext context) throws Exception {
                                retryCount[0]++;
                                log.info("************************************ RETRY COUNT : " + retryCount[0]+ ":::::::" + key.toString());
                                //throw new SQLException("");
                                if (!value.getHeaders().getOperation().name().equals("REFRESH")) {
                                    if (!value.getHeaders().getOperation().name().equals("DELETE")) { //INSERT, UPDATE
                                        if (value.getCNSMMDCRENRLDATA().getROWUSERID().trim().equalsIgnoreCase("LINK/UNLINK")) {
                                            if (!value.getCNSMMDCRENRLDATA().getROWSTSCD().trim().equalsIgnoreCase("D")
                                                    && !value.getCNSMMDCRENRLDATA().getROWSTSCD().trim().equalsIgnoreCase("A")) {
                                                ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_MDCR_ENRL", false);
                                            }
                                            log.info("CNSM_MDCR_ENRL LINK/UNLINK case key  :::" + key.toString());
                                        } else { // NOT LINK/UNLINK cases
                                            ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_MDCR_ENRL", false);
                                            log.info("CNSM_MDCR_ENRL written key  :::" + key.toString());
                                        }
                                    } else { //PHYSICAL DELETE
                                        ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_MDCR_ENRL", true);
                                        log.info("CNSM_MDCR_ENRL physical delete key  :::" + key.toString());
                                    }
                                }

                                return KeyValue.pair(key, value);
                            }
                        });
                    } catch (Throwable throwable) {

                        log.error("SQLException in Table CNSM_MDCR_ENRL :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());

                       // log.error("SQLException in Table CNSM_MDCR_ENRL :::time::::" + sdf.format(new Date()) + ":::::::" + new MetadataTransformer());

                        log.error("Exception",throwable);
                        //throw new RuntimeException("");
                        log.error("Restarting CNSM_MDCR_ENRL POD"); System.exit(1);
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

    @Bean
    public KStream<L_COV_PRDT_PCP_KEY, L_COV_PRDT_PCP> lCovPrdtPcpStream(StreamsBuilder builder) {


        KStream<L_COV_PRDT_PCP_KEY, L_COV_PRDT_PCP>
                memberKStream = builder.stream(topicConfig.getlCovPrdtPcp());

        try {
            memberKStream.map((key, value) -> {
                final int[] retryCount = {0};
                log.info("L_COV_PRDT_PCP Consuming key  :::" + key.toString());
              //  log.info("L_COV_PRDT_PCP Consuming value  :::" + value.toString());

                try {
                    if (!value.getHeaders().getOperation().name().equals("REFRESH")) {
                        if (!value.getHeaders().getOperation().name().equals("DELETE")) { //INSERT, UPDATE
                            if (value.getLCOVPRDTPCPDATA().getROWUSERID().trim().equalsIgnoreCase("LINK/UNLINK")) {
                                if (!value.getLCOVPRDTPCPDATA().getROWSTSCD().trim().equalsIgnoreCase("D")
                                        && !value.getLCOVPRDTPCPDATA().getROWSTSCD().trim().equalsIgnoreCase("A")) {
                                    ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "L_COV_PRDT_PCP", false);
                                }
                                log.info("L_COV_PRDT_PCP LINK/UNLINK case key  :::" + key.toString());
                            } else { // NOT LINK/UNLINK cases
                                ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "L_COV_PRDT_PCP", false);
                                log.info("L_COV_PRDT_PCP written key  :::" + key.toString());
                            }
                        } else { //PHYSICAL DELETE
                            ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "L_COV_PRDT_PCP", true);
                            log.info("L_COV_PRDT_PCP physical delete key  :::" + key.toString());
                        }
                    }
                }      catch (Exception e) {
                    try {
                        retryTemplate().execute(new RetryCallback<KeyValue<L_COV_PRDT_PCP_KEY, L_COV_PRDT_PCP>, Exception>() {
                            @Override
                            public KeyValue<L_COV_PRDT_PCP_KEY, L_COV_PRDT_PCP> doWithRetry(RetryContext context) throws Exception {
                                retryCount[0]++;
                                log.info("************************************ RETRY COUNT : " + retryCount[0]+ ":::::::" + key.toString());
                                //throw new SQLException("");
                                if (!value.getHeaders().getOperation().name().equals("REFRESH")) {
                                    if (!value.getHeaders().getOperation().name().equals("DELETE")) { //INSERT, UPDATE
                                        if (value.getLCOVPRDTPCPDATA().getROWUSERID().trim().equalsIgnoreCase("LINK/UNLINK")) {
                                            if (!value.getLCOVPRDTPCPDATA().getROWSTSCD().trim().equalsIgnoreCase("D")
                                                    && !value.getLCOVPRDTPCPDATA().getROWSTSCD().trim().equalsIgnoreCase("A")) {
                                                ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "L_COV_PRDT_PCP", false);
                                            }
                                            log.info("L_COV_PRDT_PCP LINK/UNLINK case key  :::" + key.toString());
                                        } else { // NOT LINK/UNLINK cases
                                            ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "L_COV_PRDT_PCP", false);
                                            log.info("L_COV_PRDT_PCP written key  :::" + key.toString());
                                        }
                                    } else { //PHYSICAL DELETE
                                        ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "L_COV_PRDT_PCP", true);
                                        log.info("L_COV_PRDT_PCP physical delete key  :::" + key.toString());
                                    }
                                }
                                return KeyValue.pair(key, value);
                            }
                        });
                    } catch (Throwable throwable) {

                        log.error("SQLException in Table L_COV_PRDT_PCP :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());

                       // log.error("SQLException in Table L_COV_PRDT_PCP :::time::::" + sdf.format(new Date()) + ":::::::" + new MetadataTransformer());

                        log.error("Exception",throwable);
                        //throw new RuntimeException("");
                        log.error("Restarting L_COV_PRDT_PCP POD"); System.exit(1);
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

    @Bean
    public KStream<L_LF_DIS_PRDT_DT_KEY, L_LF_DIS_PRDT_DT> lLfDisPrdtDtStream(StreamsBuilder builder) {


        KStream<L_LF_DIS_PRDT_DT_KEY, L_LF_DIS_PRDT_DT>
                memberKStream = builder.stream(topicConfig.getlLfDisPrdtDt());

        try {
            memberKStream.map((key, value) -> {
                final int[] retryCount = {0};
                log.info("L_LF_DIS_PRDT_DT Consuming key  :::" + key.toString());
                //log.info("L_LF_DIS_PRDT_DT Consuming value  :::" + value.toString());
                try {
                    if (!value.getHeaders().getOperation().name().equals("REFRESH")) {
                        if (!value.getHeaders().getOperation().name().equals("DELETE")) { //INSERT, UPDATE
                            if (value.getLLFDISPRDTDTDATA().getROWUSERID().trim().equalsIgnoreCase("LINK/UNLINK")) {
                                if (!value.getLLFDISPRDTDTDATA().getROWSTSCD().trim().equalsIgnoreCase("D")
                                        && !value.getLLFDISPRDTDTDATA().getROWSTSCD().trim().equalsIgnoreCase("A")) {
                                    ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "L_LF_DIS_PRDT_DT", false);
                                }
                                log.info("L_LF_DIS_PRDT_DT LINK/UNLINK case key  :::" + key.toString());
                            } else { // NOT LINK/UNLINK cases
                                ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "L_LF_DIS_PRDT_DT", false);
                                log.info("L_LF_DIS_PRDT_DT written key  :::" + key.toString());
                            }
                        } else { //PHYSICAL DELETE
                            ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "L_LF_DIS_PRDT_DT", true);
                            log.info("L_LF_DIS_PRDT_DT physical delete key  :::" + key.toString());
                        }
                    }
                }     catch (Exception e) {
                    try {
                        retryTemplate().execute(new RetryCallback<KeyValue<L_LF_DIS_PRDT_DT_KEY, L_LF_DIS_PRDT_DT>, Exception>() {
                            @Override
                            public KeyValue<L_LF_DIS_PRDT_DT_KEY, L_LF_DIS_PRDT_DT> doWithRetry(RetryContext context) throws Exception {
                                retryCount[0]++;
                                log.info("************************************ RETRY COUNT : " + retryCount[0]+ ":::::::" + key.toString());
                                //throw new SQLException("");
                                if (!value.getHeaders().getOperation().name().equals("REFRESH")) {
                                    if (!value.getHeaders().getOperation().name().equals("DELETE")) { //INSERT, UPDATE
                                        if (value.getLLFDISPRDTDTDATA().getROWUSERID().trim().equalsIgnoreCase("LINK/UNLINK")) {
                                            if (!value.getLLFDISPRDTDTDATA().getROWSTSCD().trim().equalsIgnoreCase("D")
                                                    && !value.getLLFDISPRDTDTDATA().getROWSTSCD().trim().equalsIgnoreCase("A")) {
                                                ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "L_LF_DIS_PRDT_DT", false);
                                            }
                                            log.info("L_LF_DIS_PRDT_DT LINK/UNLINK case key  :::" + key.toString());
                                        } else { // NOT LINK/UNLINK cases
                                            ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "L_LF_DIS_PRDT_DT", false);
                                            log.info("L_LF_DIS_PRDT_DT written key  :::" + key.toString());
                                        }
                                    } else { //PHYSICAL DELETE
                                        ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "L_LF_DIS_PRDT_DT", true);
                                        log.info("L_LF_DIS_PRDT_DT physical delete key  :::" + key.toString());
                                    }
                                }
                                return KeyValue.pair(key, value);
                            }
                        });
                    } catch (Throwable throwable) {

                        log.error("SQLException in Table L_LF_DIS_PRDT_DT :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());

                      //  log.error("SQLException in Table L_LF_DIS_PRDT_DT :::time::::" + sdf.format(new Date()) + ":::::::" + new MetadataTransformer());

                        log.error("Exception",throwable);
                        //throw new RuntimeException("");
                        log.error("Restarting L_LF_DIS_PRDT_DT POD"); System.exit(1);
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

    @Bean
    public KStream<CNSM_DTL_KEY, CNSM_DTL> cnsmDtlStream(StreamsBuilder builder) {


        KStream<CNSM_DTL_KEY, CNSM_DTL>
                memberKStream = builder.stream(topicConfig.getCnsmDtl());

        try {
            memberKStream.map((key, value) -> {
                final int[] retryCount = {0};
                log.info("CNSM_DTL Consuming key  :::" + key.toString());
               // log.info("CNSM_DTL Consuming value  :::" + value.toString());

                try {
                    if (!value.getHeaders().getOperation().name().equals("REFRESH")) {
                        if (!value.getHeaders().getOperation().name().equals("DELETE")) { //INSERT, UPDATE
                            if (value.getCNSMDTLDATA().getROWUSERID().trim().equalsIgnoreCase("LINK/UNLINK")) {
                                if (!value.getCNSMDTLDATA().getROWSTSCD().trim().equalsIgnoreCase("D")
                                        && !value.getCNSMDTLDATA().getROWSTSCD().trim().equalsIgnoreCase("A")) {
                                    ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_DTL", false);
                                }
                                log.info("CNSM_DTL LINK/UNLINK case key  :::" + key.toString());
                            } else { // NOT LINK/UNLINK cases
                                ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_DTL", false);
                                log.info("CNSM_DTL written key  :::" + key.toString());
                            }
                        } else { //PHYSICAL DELETE
                            ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_DTL", true);
                            log.info("CNSM_DTL physical delete key  :::" + key.toString());
                        }
                    }
                }      catch (Exception e) {
                    try {
                        retryTemplate().execute(new RetryCallback<KeyValue<CNSM_DTL_KEY, CNSM_DTL>, Exception>() {
                            @Override
                            public KeyValue<CNSM_DTL_KEY, CNSM_DTL> doWithRetry(RetryContext context) throws Exception {
                                retryCount[0]++;
                                log.info("************************************ RETRY COUNT : " + retryCount[0]+ ":::::::" + key.toString());
                                //throw new SQLException("");
                                if (!value.getHeaders().getOperation().name().equals("REFRESH")) {
                                    if (!value.getHeaders().getOperation().name().equals("DELETE")) { //INSERT, UPDATE
                                        if (value.getCNSMDTLDATA().getROWUSERID().trim().equalsIgnoreCase("LINK/UNLINK")) {
                                            if (!value.getCNSMDTLDATA().getROWSTSCD().trim().equalsIgnoreCase("D")
                                                    && !value.getCNSMDTLDATA().getROWSTSCD().trim().equalsIgnoreCase("A")) {
                                                ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_DTL", false);
                                            }
                                            log.info("CNSM_DTL LINK/UNLINK case key  :::" + key.toString());
                                        } else { // NOT LINK/UNLINK cases
                                            ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_DTL", false);
                                            log.info("CNSM_DTL written key  :::" + key.toString());
                                        }
                                    } else { //PHYSICAL DELETE
                                        ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_DTL", true);
                                        log.info("CNSM_DTL physical delete key  :::" + key.toString());
                                    }
                                }
                                return KeyValue.pair(key, value);
                            }
                        });
                    } catch (Throwable throwable) {

                        log.error("SQLException in Table CNSM_DTL :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());

                       // log.error("SQLException in Table CNSM_DTL :::time::::" + sdf.format(new Date()) + ":::::::" + new MetadataTransformer());

                        log.error("Exception",throwable);
                        //throw new RuntimeException("");
                        log.error("Restarting CNSM_DTL POD"); System.exit(1);
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

    @Bean
    public KStream<ML_CNSM_TEL_KEY, ML_CNSM_TEL> mlCnsmTelStream(StreamsBuilder builder) {


        KStream<ML_CNSM_TEL_KEY, ML_CNSM_TEL>
                memberKStream = builder.stream(topicConfig.getMlCnsmTel());

        try {
            memberKStream.map((key, value) -> {
                final int[] retryCount = {0};
                log.info("ML_CNSM_TEL Consuming key  :::" + key.toString());
               // log.info("ML_CNSM_TEL Consuming value  :::" + value.toString());

                try {
                    if (!value.getHeaders().getOperation().name().equals("REFRESH")) {
                        if (!value.getHeaders().getOperation().name().equals("DELETE")) { //INSERT, UPDATE
                            if (value.getMLCNSMTELDATA().getROWUSERID().trim().equalsIgnoreCase("LINK/UNLINK")) {
                                if (!value.getMLCNSMTELDATA().getROWSTSCD().trim().equalsIgnoreCase("D")
                                        && !value.getMLCNSMTELDATA().getROWSTSCD().trim().equalsIgnoreCase("A")) {
                                    ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "ML_CNSM_TEL", false);
                                }
                                log.info("ML_CNSM_TEL LINK/UNLINK case key  :::" + key.toString());
                            } else { // NOT LINK/UNLINK cases
                                ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "ML_CNSM_TEL", false);
                                log.info("ML_CNSM_TEL written key  :::" + key.toString());
                            }
                        } else { //PHYSICAL DELETE
                            ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "ML_CNSM_TEL", true);
                            log.info("ML_CNSM_TEL physical delete key  :::" + key.toString());
                        }
                    }
                }      catch (Exception e) {
                    try {
                        retryTemplate().execute(new RetryCallback<KeyValue<ML_CNSM_TEL_KEY, ML_CNSM_TEL>, Exception>() {
                            @Override
                            public KeyValue<ML_CNSM_TEL_KEY, ML_CNSM_TEL> doWithRetry(RetryContext context) throws Exception {
                                retryCount[0]++;
                                log.info("************************************ RETRY COUNT : " + retryCount[0]+ ":::::::" + key.toString());
                                //throw new SQLException("");
                                if (!value.getHeaders().getOperation().name().equals("REFRESH")) {
                                    if (!value.getHeaders().getOperation().name().equals("DELETE")) { //INSERT, UPDATE
                                        if (value.getMLCNSMTELDATA().getROWUSERID().trim().equalsIgnoreCase("LINK/UNLINK")) {
                                            if (!value.getMLCNSMTELDATA().getROWSTSCD().trim().equalsIgnoreCase("D")
                                                    && !value.getMLCNSMTELDATA().getROWSTSCD().trim().equalsIgnoreCase("A")) {
                                                ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "ML_CNSM_TEL", false);
                                            }
                                            log.info("ML_CNSM_TEL LINK/UNLINK case key  :::" + key.toString());
                                        } else { // NOT LINK/UNLINK cases
                                            ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "ML_CNSM_TEL", false);
                                            log.info("ML_CNSM_TEL written key  :::" + key.toString());
                                        }
                                    } else { //PHYSICAL DELETE
                                        ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "ML_CNSM_TEL", true);
                                        log.info("ML_CNSM_TEL physical delete key  :::" + key.toString());
                                    }
                                }

                                return KeyValue.pair(key, value);
                            }
                        });
                    } catch (Throwable throwable) {

                        log.error("SQLException in Table ML_CNSM_TEL :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());

                      //  log.error("SQLException in Table ML_CNSM_TEL :::time::::" + sdf.format(new Date()) + ":::::::" + new MetadataTransformer());

                        log.error("Exception",throwable);
                        //throw new RuntimeException("");
                        log.error("Restarting ML_CNSM_TEL POD"); System.exit(1);
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

    @Bean
    public KStream<ML_CNSM_ADR_KEY, ML_CNSM_ADR> mlCnsmAdrStream(StreamsBuilder builder) {


        KStream<ML_CNSM_ADR_KEY, ML_CNSM_ADR>
                memberKStream = builder.stream(topicConfig.getMlCnsmAdr());

        try {
            memberKStream.map((key, value) -> {
                final int[] retryCount = {0};
                log.info("ML_CNSM_ADR Consuming key  :::" + key.toString());
               // log.info("ML_CNSM_ADR Consuming value  :::" + value.toString());
                try {
                    if (!value.getHeaders().getOperation().name().equals("REFRESH")) {
                        if (!value.getHeaders().getOperation().name().equals("DELETE")) { //INSERT, UPDATE
                            if (value.getMLCNSMADRDATA().getROWUSERID().trim().equalsIgnoreCase("LINK/UNLINK")) {
                                if (!value.getMLCNSMADRDATA().getROWSTSCD().trim().equalsIgnoreCase("D")
                                        && !value.getMLCNSMADRDATA().getROWSTSCD().trim().equalsIgnoreCase("A")) {
                                    ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "ML_CNSM_ADR", false);
                                }
                                log.info("ML_CNSM_ADR LINK/UNLINK case key  :::" + key.toString());
                            } else { // NOT LINK/UNLINK cases
                                ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "ML_CNSM_ADR", false);
                                log.info("ML_CNSM_ADR written key  :::" + key.toString());
                            }
                        } else { //PHYSICAL DELETE
                            ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "ML_CNSM_ADR", true);
                            log.info("ML_CNSM_ADR physical delete key  :::" + key.toString());
                        }
                    }
                }      catch (Exception e) {
                    try {
                        retryTemplate().execute(new RetryCallback<KeyValue<ML_CNSM_ADR_KEY, ML_CNSM_ADR>, Exception>() {
                            @Override
                            public KeyValue<ML_CNSM_ADR_KEY, ML_CNSM_ADR> doWithRetry(RetryContext context) throws Exception {
                                retryCount[0]++;
                                log.info("************************************ RETRY COUNT : " + retryCount[0]+ ":::::::" + key.toString());
                                //throw new SQLException("");
                                if (!value.getHeaders().getOperation().name().equals("REFRESH")) {
                                    if (!value.getHeaders().getOperation().name().equals("DELETE")) { //INSERT, UPDATE
                                        if (value.getMLCNSMADRDATA().getROWUSERID().trim().equalsIgnoreCase("LINK/UNLINK")) {
                                            if (!value.getMLCNSMADRDATA().getROWSTSCD().trim().equalsIgnoreCase("D")
                                                    && !value.getMLCNSMADRDATA().getROWSTSCD().trim().equalsIgnoreCase("A")) {
                                                ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "ML_CNSM_ADR", false);
                                            }
                                            log.info("ML_CNSM_ADR LINK/UNLINK case key  :::" + key.toString());
                                        } else { // NOT LINK/UNLINK cases
                                            ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "ML_CNSM_ADR", false);
                                            log.info("ML_CNSM_ADR written key  :::" + key.toString());
                                        }
                                    } else { //PHYSICAL DELETE
                                        ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "ML_CNSM_ADR", true);
                                        log.info("ML_CNSM_ADR physical delete key  :::" + key.toString());
                                    }
                                }
                                return KeyValue.pair(key, value);
                            }
                        });
                    } catch (Throwable throwable) {

                        log.error("SQLException in Table ML_CNSM_ADR :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());

                       // log.error("SQLException in Table ML_CNSM_ADR :::time::::" + sdf.format(new Date()) + ":::::::" + new MetadataTransformer());

                        log.error("Exception",throwable);
                        //throw new RuntimeException("");
                        log.error("Restarting ML_CNSM_ADR POD"); System.exit(1);
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


    // The getROWSTSCD is not present in getCNSMMDCRPRISECDATA class
    @Bean
    public KStream<CNSM_MDCR_PRISEC_KEY, CNSM_MDCR_PRISEC> cnsmMdcrPrisecStream(StreamsBuilder builder) {


        KStream<CNSM_MDCR_PRISEC_KEY, CNSM_MDCR_PRISEC>
                memberKStream = builder.stream(topicConfig.getCnsmMdcrPrisec());

        try {
            memberKStream.map((key, value) -> {
                final int[] retryCount = {0};
                log.info("CNSM_MDCR_PRISEC Consuming key  :::" + key.toString());
              //  log.info("CNSM_MDCR_PRISEC Consuming value  :::" + value.toString());
                try {
                    if (!value.getHeaders().getOperation().name().equals("REFRESH")) {
                        if (!value.getHeaders().getOperation().name().equals("DELETE")) { //INSERT, UPDATE
                            if (value.getCNSMMDCRPRISECDATA().getROWUSERID().trim().equalsIgnoreCase("LINK/UNLINK")) {
                                String rowstscd = ingestionLogDAO.rowstscd(value.toString());
                                if (!rowstscd.trim().equalsIgnoreCase("D")
                                        && !rowstscd.trim().equalsIgnoreCase("A")) {
                                    ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_MDCR_PRISEC", false);
                                }
                                log.info("CNSM_MDCR_PRISEC LINK/UNLINK case key  :::" + key.toString());
                            } else { // NOT LINK/UNLINK cases
                                ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_MDCR_PRISEC", false);
                                log.info("CNSM_MDCR_PRISEC written key  :::" + key.toString());
                            }
                        } else { //PHYSICAL DELETE
                            ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_MDCR_PRISEC", true);
                            log.info("CNSM_MDCR_PRISEC physical delete key  :::" + key.toString());
                        }
                    }
                }      catch (Exception e) {
                    try {
                        retryTemplate().execute(new RetryCallback<KeyValue<CNSM_MDCR_PRISEC_KEY, CNSM_MDCR_PRISEC>, Exception>() {
                            @Override
                            public KeyValue<CNSM_MDCR_PRISEC_KEY, CNSM_MDCR_PRISEC> doWithRetry(RetryContext context) throws Exception {
                                retryCount[0]++;
                                log.info("************************************ RETRY COUNT : " + retryCount[0]+ ":::::::" + key.toString());
                                //throw new SQLException("");
                                if (!value.getHeaders().getOperation().name().equals("REFRESH")) {
                                    if (!value.getHeaders().getOperation().name().equals("DELETE")) { //INSERT, UPDATE
                                        if (value.getCNSMMDCRPRISECDATA().getROWUSERID().trim().equalsIgnoreCase("LINK/UNLINK")) {
                                            String rowstscd = ingestionLogDAO.rowstscd(value.toString());
                                            if (!rowstscd.trim().equalsIgnoreCase("D")
                                                    && !rowstscd.trim().equalsIgnoreCase("A")) {
                                                ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_MDCR_PRISEC", false);
                                            }
                                            log.info("CNSM_MDCR_PRISEC LINK/UNLINK case key  :::" + key.toString());
                                        } else { // NOT LINK/UNLINK cases
                                            ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_MDCR_PRISEC", false);
                                            log.info("CNSM_MDCR_PRISEC written key  :::" + key.toString());
                                        }
                                    } else { //PHYSICAL DELETE
                                        ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_MDCR_PRISEC", true);
                                        log.info("CNSM_MDCR_PRISEC physical delete key  :::" + key.toString());
                                    }
                                }
                                return KeyValue.pair(key, value);
                            }
                        });
                    } catch (Throwable throwable) {

                        log.error("SQLException in Table CNSM_MDCR_PRISEC :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());

                    //    log.error("SQLException in Table CNSM_MDCR_PRISEC :::time::::" + sdf.format(new Date()) + ":::::::" + new MetadataTransformer());

                        log.error("Exception",throwable);
                        //throw new RuntimeException("");
                        log.error("Restarting CNSM_MDCR_PRISEC POD"); System.exit(1);
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



    @Bean
    public KStream<ML_CNSM_ELCTR_ADR_KEY, ML_CNSM_ELCTR_ADR> mlCnsmElctrAdrStream(StreamsBuilder builder) {


        KStream<ML_CNSM_ELCTR_ADR_KEY, ML_CNSM_ELCTR_ADR>
                memberKStream = builder.stream(topicConfig.getMlCnsmElctrAdr());

        try {
            memberKStream.map((key, value) -> {
                final int[] retryCount = {0};
                log.info("ML_CNSM_ELCTR_ADR Consuming key  :::" + key.toString());
               // log.info("ML_CNSM_ELCTR_ADR Consuming value  :::" + value.toString());
                try {
                    if (!value.getHeaders().getOperation().name().equals("REFRESH")) {
                        if (!value.getHeaders().getOperation().name().equals("DELETE")) { //INSERT, UPDATE
                            if (value.getMLCNSMELCTRADRDATA().getROWUSERID().trim().equalsIgnoreCase("LINK/UNLINK")) {
                                if (!value.getMLCNSMELCTRADRDATA().getROWSTSCD().trim().equalsIgnoreCase("D")
                                        && !value.getMLCNSMELCTRADRDATA().getROWSTSCD().trim().equalsIgnoreCase("A")) {
                                    ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "ML_CNSM_ELCTR_ADR", false);
                                }
                                log.info("ML_CNSM_ELCTR_ADR LINK/UNLINK case key  :::" + key.toString());
                            } else { // NOT LINK/UNLINK cases
                                ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "ML_CNSM_ELCTR_ADR", false);
                                log.info("ML_CNSM_ELCTR_ADR written key  :::" + key.toString());
                            }
                        } else { //PHYSICAL DELETE
                            ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "ML_CNSM_ELCTR_ADR", true);
                            log.info("ML_CNSM_ELCTR_ADR physical delete key  :::" + key.toString());
                        }
                    }

                }      catch (Exception e) {
                    try {
                        retryTemplate().execute(new RetryCallback<KeyValue<ML_CNSM_ELCTR_ADR_KEY, ML_CNSM_ELCTR_ADR>, Exception>() {
                            @Override
                            public KeyValue<ML_CNSM_ELCTR_ADR_KEY, ML_CNSM_ELCTR_ADR> doWithRetry(RetryContext context) throws Exception {
                                retryCount[0]++;
                                log.info("************************************ RETRY COUNT : " + retryCount[0]+ ":::::::" + key.toString());
                                //throw new SQLException("");
                                if (!value.getHeaders().getOperation().name().equals("REFRESH")) {
                                    if (!value.getHeaders().getOperation().name().equals("DELETE")) { //INSERT, UPDATE
                                        if (value.getMLCNSMELCTRADRDATA().getROWUSERID().trim().equalsIgnoreCase("LINK/UNLINK")) {
                                            if (!value.getMLCNSMELCTRADRDATA().getROWSTSCD().trim().equalsIgnoreCase("D")
                                                    && !value.getMLCNSMELCTRADRDATA().getROWSTSCD().trim().equalsIgnoreCase("A")) {
                                                ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "ML_CNSM_ELCTR_ADR", false);
                                            }
                                            log.info("ML_CNSM_ELCTR_ADR LINK/UNLINK case key  :::" + key.toString());
                                        } else { // NOT LINK/UNLINK cases
                                            ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "ML_CNSM_ELCTR_ADR", false);
                                            log.info("ML_CNSM_ELCTR_ADR written key  :::" + key.toString());
                                        }
                                    } else { //PHYSICAL DELETE
                                        ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "ML_CNSM_ELCTR_ADR", true);
                                        log.info("ML_CNSM_ELCTR_ADR physical delete key  :::" + key.toString());
                                    }
                                }
                                return KeyValue.pair(key, value);
                            }
                        });
                    } catch (Throwable throwable) {

                        log.error("SQLException in Table ML_CNSM_ELCTR_ADR :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());

                       // log.error("SQLException in Table ML_CNSM_ELCTR_ADR :::time::::" + sdf.format(new Date()) + ":::::::" + new MetadataTransformer());

                        log.error("Exception",throwable);
                        //throw new RuntimeException("");
                        log.error("Restarting ML_CNSM_ELCTR_ADR POD"); System.exit(1);
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


    @Bean
    public KStream<CNSM_MDCR_ENTL_KEY, CNSM_MDCR_ENTL> CnsmMdcrEntlStream(StreamsBuilder builder) {

        KStream<CNSM_MDCR_ENTL_KEY, CNSM_MDCR_ENTL>
                memberKStream = builder.stream(topicConfig.getCnsmMdcrEntl());

        try {
            memberKStream.map((key, value) -> {
                final int[] retryCount = {0};
                log.info("CNSM_MDCR_ENTL Consuming key  :::" + key.toString());
              //  log.info("CNSM_MDCR_ENTL Consuming value  :::" + value.toString());

                try {
                    if (!value.getHeaders().getOperation().name().equals("REFRESH")) {
                        if (!value.getHeaders().getOperation().name().equals("DELETE")) { //INSERT, UPDATE
                            if (value.getCNSMMDCRENTLDATA().getROWUSERID().trim().equalsIgnoreCase("LINK/UNLINK")) {
                                if (!value.getCNSMMDCRENTLDATA().getROWSTSCD().trim().equalsIgnoreCase("D")
                                        && !value.getCNSMMDCRENTLDATA().getROWSTSCD().trim().equalsIgnoreCase("A")) {
                                    ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_MDCR_ENTL", false);
                                }
                                log.info("CNSM_MDCR_ENTL LINK/UNLINK case key  :::" + key.toString());
                            } else { // NOT LINK/UNLINK cases
                                ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_MDCR_ENTL", false);
                                log.info("CNSM_MDCR_ENTL written key  :::" + key.toString());
                            }
                        } else { //PHYSICAL DELETE
                            ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_MDCR_ENTL", true);
                            log.info("CNSM_MDCR_ENTL physical delete key  :::" + key.toString());
                        }
                    }
                }      catch (Exception e) {
                    try {
                        retryTemplate().execute(new RetryCallback<KeyValue<CNSM_MDCR_ENTL_KEY, CNSM_MDCR_ENTL>, Exception>() {
                            @Override
                            public KeyValue<CNSM_MDCR_ENTL_KEY, CNSM_MDCR_ENTL> doWithRetry(RetryContext context) throws Exception {
                                retryCount[0]++;
                                log.info("************************************ RETRY COUNT : " + retryCount[0]+ ":::::::" + key.toString());
                                //throw new SQLException("");
                                if (!value.getHeaders().getOperation().name().equals("REFRESH")) {
                                    if (!value.getHeaders().getOperation().name().equals("DELETE")) { //INSERT, UPDATE
                                        if (value.getCNSMMDCRENTLDATA().getROWUSERID().trim().equalsIgnoreCase("LINK/UNLINK")) {
                                            if (!value.getCNSMMDCRENTLDATA().getROWSTSCD().trim().equalsIgnoreCase("D")
                                                    && !value.getCNSMMDCRENTLDATA().getROWSTSCD().trim().equalsIgnoreCase("A")) {
                                                ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_MDCR_ENTL", false);
                                            }
                                            log.info("CNSM_MDCR_ENTL LINK/UNLINK case key  :::" + key.toString());
                                        } else { // NOT LINK/UNLINK cases
                                            ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_MDCR_ENTL", false);
                                            log.info("CNSM_MDCR_ENTL written key  :::" + key.toString());
                                        }
                                    } else { //PHYSICAL DELETE
                                        ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_MDCR_ENTL", true);
                                        log.info("CNSM_MDCR_ENTL physical delete key  :::" + key.toString());
                                    }
                                }
                                return KeyValue.pair(key, value);
                            }
                        });
                    } catch (Throwable throwable) {

                        log.error("SQLException in Table CNSM_MDCR_ENTL :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());

                      //  log.error("SQLException in Table CNSM_MDCR_ENTL :::time::::" + sdf.format(new Date()) + ":::::::" + new MetadataTransformer());

                        log.error("Exception",throwable);
                        //throw new RuntimeException("");
                        log.error("Restarting CNSM_MDCR_ENTL POD"); System.exit(1);
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


    @Bean
    public KStream<CNSM_OTHR_INS_KEY, CNSM_OTHR_INS> CnsmOthrInsStream(StreamsBuilder builder) {

        KStream<CNSM_OTHR_INS_KEY, CNSM_OTHR_INS>
                memberKStream = builder.stream(topicConfig.getCnsmOthrIns());

        try {
            memberKStream.map((key, value) -> {
                final int[] retryCount = {0};
                log.info("CNSM_OTHR_INS Consuming key  :::" + key.toString());
               // log.info("CNSM_OTHR_INS Consuming value  :::" + value.toString());

                try {
                    if (!value.getHeaders().getOperation().name().equals("REFRESH")) {
                        if (!value.getHeaders().getOperation().name().equals("DELETE")) { //INSERT, UPDATE
                            if (value.getCNSMOTHRINSDATA().getROWUSERID().trim().equalsIgnoreCase("LINK/UNLINK")) {
                                if (!value.getCNSMOTHRINSDATA().getROWSTSCD().trim().equalsIgnoreCase("D")
                                        && !value.getCNSMOTHRINSDATA().getROWSTSCD().trim().equalsIgnoreCase("A")) {
                                    ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_OTHR_INS", false);
                                }
                                log.info("CNSM_OTHR_INS LINK/UNLINK case key  :::" + key.toString());
                            } else { // NOT LINK/UNLINK cases
                                ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_OTHR_INS", false);
                                log.info("CNSM_OTHR_INS written key  :::" + key.toString());
                            }
                        } else { //PHYSICAL DELETE
                            ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_OTHR_INS", true);
                            log.info("CNSM_OTHR_INS physical delete key  :::" + key.toString());
                        }
                    }
                }      catch (Exception e) {
                    try {
                        retryTemplate().execute(new RetryCallback<KeyValue<CNSM_OTHR_INS_KEY, CNSM_OTHR_INS>, Exception>() {
                            @Override
                            public KeyValue<CNSM_OTHR_INS_KEY, CNSM_OTHR_INS> doWithRetry(RetryContext context) throws Exception {
                                retryCount[0]++;
                                log.info("************************************ RETRY COUNT : " + retryCount[0]+ ":::::::" + key.toString());
                                //throw new SQLException("");
                                if (!value.getHeaders().getOperation().name().equals("REFRESH")) {
                                    if (!value.getHeaders().getOperation().name().equals("DELETE")) { //INSERT, UPDATE
                                        if (value.getCNSMOTHRINSDATA().getROWUSERID().trim().equalsIgnoreCase("LINK/UNLINK")) {
                                            if (!value.getCNSMOTHRINSDATA().getROWSTSCD().trim().equalsIgnoreCase("D")
                                                    && !value.getCNSMOTHRINSDATA().getROWSTSCD().trim().equalsIgnoreCase("A")) {
                                                ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_OTHR_INS", false);
                                            }
                                            log.info("CNSM_OTHR_INS LINK/UNLINK case key  :::" + key.toString());
                                        } else { // NOT LINK/UNLINK cases
                                            ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_OTHR_INS", false);
                                            log.info("CNSM_OTHR_INS written key  :::" + key.toString());
                                        }
                                    } else { //PHYSICAL DELETE
                                        ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_OTHR_INS", true);
                                        log.info("CNSM_OTHR_INS physical delete key  :::" + key.toString());
                                    }
                                }
                                return KeyValue.pair(key, value);
                            }
                        });
                    } catch (Throwable throwable) {

                        log.error("SQLException in Table CNSM_OTHR_INS :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());

                       // log.error("SQLException in Table CNSM_OTHR_INS :::time::::" + sdf.format(new Date()) + ":::::::" + new MetadataTransformer());

                        log.error("Exception",throwable);
                        //throw new RuntimeException("");
                        log.error("Restarting CNSM_OTHR_INS POD"); System.exit(1);
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

    @Bean
    public KStream<ML_CNSM_XREF_KEY, ML_CNSM_XREF> mlCnsmXrefStream(StreamsBuilder builder) {


        KStream<ML_CNSM_XREF_KEY, ML_CNSM_XREF>
                memberKStream = builder.stream(topicConfig.getMlCnsmXref());

        try {
            memberKStream.map((key, value) -> {
                final int[] retryCount = {0};
                log.info("ML_CNSM_XREF Consuming key  :::" + key.toString());
               // log.info("ML_CNSM_XREF Consuming value  :::" + value.toString());

                try {
                    if (!value.getHeaders().getOperation().name().equals("REFRESH")) {
                        if (!value.getHeaders().getOperation().name().equals("DELETE")) { //INSERT, UPDATE
                            if (value.getMLCNSMXREFDATA().getROWUSERID().trim().equalsIgnoreCase("LINK/UNLINK")) {
                                if (!value.getMLCNSMXREFDATA().getROWSTSCD().trim().equalsIgnoreCase("D")
                                        && !value.getMLCNSMXREFDATA().getROWSTSCD().trim().equalsIgnoreCase("A")) {
                                    ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "ML_CNSM_XREF", false);
                                }
                                log.info("ML_CNSM_XREF LINK/UNLINK case key  :::" + key.toString());
                            } else { // NOT LINK/UNLINK cases
                                ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "ML_CNSM_XREF", false);
                                log.info("ML_CNSM_XREF written key  :::" + key.toString());
                            }
                        } else { //PHYSICAL DELETE
                            ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "ML_CNSM_XREF", true);
                            log.info("ML_CNSM_XREF physical delete key  :::" + key.toString());
                        }
                    }
                }      catch (Exception e) {
                    try {
                        retryTemplate().execute(new RetryCallback<KeyValue<ML_CNSM_XREF_KEY, ML_CNSM_XREF>, Exception>() {
                            @Override
                            public KeyValue<ML_CNSM_XREF_KEY, ML_CNSM_XREF> doWithRetry(RetryContext context) throws Exception {
                                retryCount[0]++;
                                log.info("************************************ RETRY COUNT : " + retryCount[0]+ ":::::::" + key.toString());
                                //throw new SQLException("");
                                if (!value.getHeaders().getOperation().name().equals("REFRESH")) {
                                    if (!value.getHeaders().getOperation().name().equals("DELETE")) { //INSERT, UPDATE
                                        if (value.getMLCNSMXREFDATA().getROWUSERID().trim().equalsIgnoreCase("LINK/UNLINK")) {
                                            if (!value.getMLCNSMXREFDATA().getROWSTSCD().trim().equalsIgnoreCase("D")
                                                    && !value.getMLCNSMXREFDATA().getROWSTSCD().trim().equalsIgnoreCase("A")) {
                                                ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "ML_CNSM_XREF", false);
                                            }
                                            log.info("ML_CNSM_XREF LINK/UNLINK case key  :::" + key.toString());
                                        } else { // NOT LINK/UNLINK cases
                                            ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "ML_CNSM_XREF", false);
                                            log.info("ML_CNSM_XREF written key  :::" + key.toString());
                                        }
                                    } else { //PHYSICAL DELETE
                                        ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "ML_CNSM_XREF", true);
                                        log.info("ML_CNSM_XREF physical delete key  :::" + key.toString());
                                    }
                                }
                                return KeyValue.pair(key, value);
                            }
                        });
                    } catch (Throwable throwable) {

                        log.error("SQLException in Table ML_CNSM_XREF :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());

                      //  log.error("SQLException in Table ML_CNSM_XREF :::time::::" + sdf.format(new Date()) + ":::::::" + new MetadataTransformer());

                        log.error("Exception",throwable);
                        //throw new RuntimeException("");
                        log.error("Restarting ML_CNSM_XREF POD"); System.exit(1);
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


    @Bean
    public KStream<CNSM_AUTH_REP_KEY, CNSM_AUTH_REP> cnsmAuthRepStream(StreamsBuilder builder) {


        KStream<CNSM_AUTH_REP_KEY, CNSM_AUTH_REP>
                memberKStream = builder.stream(topicConfig.getCnsmAuthRep());

        try {
            memberKStream.map((key, value) -> {
                final int[] retryCount = {0};
                log.info("CNSM_AUTH_REP Consuming key  :::" + key.toString());
             //   log.info("CNSM_AUTH_REP Consuming value  :::" + value.toString());

                try {
                    if (!value.getHeaders().getOperation().name().equals("REFRESH")) {
                        if (!value.getHeaders().getOperation().name().equals("DELETE")) { //INSERT, UPDATE
                            if (value.getCNSMAUTHREPDATA().getROWUSERID().trim().equalsIgnoreCase("LINK/UNLINK")) {
                                if (!value.getCNSMAUTHREPDATA().getROWSTSCD().trim().equalsIgnoreCase("D")
                                        && !value.getCNSMAUTHREPDATA().getROWSTSCD().trim().equalsIgnoreCase("A")) {
                                    ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_AUTH_REP", false);
                                }
                                log.info("CNSM_AUTH_REP LINK/UNLINK case key  :::" + key.toString());
                            } else { // NOT LINK/UNLINK cases
                                ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_AUTH_REP", false);
                                log.info("CNSM_AUTH_REP written key  :::" + key.toString());
                            }
                        } else { //PHYSICAL DELETE
                            ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_AUTH_REP", true);
                            log.info("CNSM_AUTH_REP physical delete key  :::" + key.toString());
                        }
                    }
                }      catch (Exception e) {
                    try {
                        retryTemplate().execute(new RetryCallback<KeyValue<CNSM_AUTH_REP_KEY, CNSM_AUTH_REP>, Exception>() {
                            @Override
                            public KeyValue<CNSM_AUTH_REP_KEY, CNSM_AUTH_REP> doWithRetry(RetryContext context) throws Exception {
                                retryCount[0]++;
                                log.info("************************************ RETRY COUNT : " + retryCount[0]+ ":::::::" + key.toString());
                                //throw new SQLException("");
                                if (!value.getHeaders().getOperation().name().equals("REFRESH")) {
                                    if (!value.getHeaders().getOperation().name().equals("DELETE")) { //INSERT, UPDATE
                                        if (value.getCNSMAUTHREPDATA().getROWUSERID().trim().equalsIgnoreCase("LINK/UNLINK")) {
                                            if (!value.getCNSMAUTHREPDATA().getROWSTSCD().trim().equalsIgnoreCase("D")
                                                    && !value.getCNSMAUTHREPDATA().getROWSTSCD().trim().equalsIgnoreCase("A")) {
                                                ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_AUTH_REP", false);
                                            }
                                            log.info("CNSM_AUTH_REP LINK/UNLINK case key  :::" + key.toString());
                                        } else { // NOT LINK/UNLINK cases
                                            ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_AUTH_REP", false);
                                            log.info("CNSM_AUTH_REP written key  :::" + key.toString());
                                        }
                                    } else { //PHYSICAL DELETE
                                        ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_AUTH_REP", true);
                                        log.info("CNSM_AUTH_REP physical delete key  :::" + key.toString());
                                    }
                                }
                                return KeyValue.pair(key, value);
                            }
                        });
                    } catch (Throwable throwable) {

                        log.error("SQLException in Table CNSM_AUTH_REP :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());

                      //  log.error("SQLException in Table CNSM_AUTH_REP :::time::::" + sdf.format(new Date()) + ":::::::" + new MetadataTransformer());

                        log.error("Exception",throwable);
                        //throw new RuntimeException("");
                        log.error("Restarting CNSM_AUTH_REP POD"); System.exit(1);
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


    @Bean
    public KStream<CNSM_CAL_KEY, CNSM_CAL> cnsmCalStream(StreamsBuilder builder) {


        KStream<CNSM_CAL_KEY, CNSM_CAL>
                memberKStream = builder.stream(topicConfig.getCnsmCal());

        try {
            memberKStream.map((key, value) -> {
                final int[] retryCount = {0};
                log.info("CNSM_CAL Consuming key  :::" + key.toString());
             //   log.info("CNSM_CAL Consuming value  :::" + value.toString());

                try {
                    if (!value.getHeaders().getOperation().name().equals("REFRESH")) {
                        if (!value.getHeaders().getOperation().name().equals("DELETE")) { //INSERT, UPDATE
                            if (value.getCNSMCALDATA().getROWUSERID().trim().equalsIgnoreCase("LINK/UNLINK")) {
                                if (!value.getCNSMCALDATA().getROWSTSCD().trim().equalsIgnoreCase("D")
                                        && !value.getCNSMCALDATA().getROWSTSCD().trim().equalsIgnoreCase("A")) {
                                    ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_CAL", false);
                                }
                                log.info("CNSM_CAL LINK/UNLINK case key  :::" + key.toString());
                            } else { // NOT LINK/UNLINK cases
                                ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_CAL", false);
                                log.info("CNSM_CAL written key  :::" + key.toString());
                            }
                        } else { //PHYSICAL DELETE
                            ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_CAL", true);
                            log.info("CNSM_CAL physical delete key  :::" + key.toString());
                        }
                    }
                }      catch (Exception e) {
                    try {
                        retryTemplate().execute(new RetryCallback<KeyValue<CNSM_CAL_KEY, CNSM_CAL>, Exception>() {
                            @Override
                            public KeyValue<CNSM_CAL_KEY, CNSM_CAL> doWithRetry(RetryContext context) throws Exception {
                                retryCount[0]++;
                                log.info("************************************ RETRY COUNT : " + retryCount[0]+ ":::::::" + key.toString());
                                //throw new SQLException("");
                                if (!value.getHeaders().getOperation().name().equals("REFRESH")) {
                                    if (!value.getHeaders().getOperation().name().equals("DELETE")) { //INSERT, UPDATE
                                        if (value.getCNSMCALDATA().getROWUSERID().trim().equalsIgnoreCase("LINK/UNLINK")) {
                                            if (!value.getCNSMCALDATA().getROWSTSCD().trim().equalsIgnoreCase("D")
                                                    && !value.getCNSMCALDATA().getROWSTSCD().trim().equalsIgnoreCase("A")) {
                                                ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_CAL", false);
                                            }
                                            log.info("CNSM_CAL LINK/UNLINK case key  :::" + key.toString());
                                        } else { // NOT LINK/UNLINK cases
                                            ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_CAL", false);
                                            log.info("CNSM_CAL written key  :::" + key.toString());
                                        }
                                    } else { //PHYSICAL DELETE
                                        ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_CAL", true);
                                        log.info("CNSM_CAL physical delete key  :::" + key.toString());
                                    }
                                }
                                return KeyValue.pair(key, value);
                            }
                        });
                    } catch (Throwable throwable) {

                        log.error("SQLException in Table CNSM_CAL :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());

                        //log.error("SQLException in Table CNSM_CAL :::time::::" + sdf.format(new Date()) + ":::::::" + new MetadataTransformer());

                        log.error("Exception",throwable);
                        //throw new RuntimeException("");
                        log.error("Restarting CNSM_CAL POD"); System.exit(1);
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


    @Bean
    public KStream<CNSM_COB_PRIMACY_KEY, CNSM_COB_PRIMACY> cnsmCobPrimacy(StreamsBuilder builder) {


        KStream<CNSM_COB_PRIMACY_KEY, CNSM_COB_PRIMACY>
                memberKStream = builder.stream(topicConfig.getCnsmCobPrimacy());

        try {
            memberKStream.map((key, value) -> {
                final int[] retryCount = {0};
                log.info("CNSM_COB_PRIMACY Consuming key  :::" + key.toString());
              //  log.info("CNSM_COB_PRIMACY Consuming value  :::" + value.toString());

                try {
                    if (!value.getHeaders().getOperation().name().equals("REFRESH")) {
                        if (!value.getHeaders().getOperation().name().equals("DELETE")) { //INSERT, UPDATE
                            if (value.getCNSMCOBPRIMACYDATA().getROWUSERID().trim().equalsIgnoreCase("LINK/UNLINK")) {
                                if (!value.getCNSMCOBPRIMACYDATA().getROWSTSCD().trim().equalsIgnoreCase("D")
                                        && !value.getCNSMCOBPRIMACYDATA().getROWSTSCD().trim().equalsIgnoreCase("A")) {
                                    ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_COB_PRIMACY", false);
                                }
                                log.info("CNSM_COB_PRIMACY LINK/UNLINK case key  :::" + key.toString());
                            } else { // NOT LINK/UNLINK cases
                                ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_COB_PRIMACY", false);
                                log.info("CNSM_COB_PRIMACY written key  :::" + key.toString());
                            }
                        } else { //PHYSICAL DELETE
                            ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_COB_PRIMACY", true);
                            log.info("CNSM_COB_PRIMACY physical delete key  :::" + key.toString());
                        }
                    }
                }      catch (Exception e) {
                    try {
                        retryTemplate().execute(new RetryCallback<KeyValue<CNSM_COB_PRIMACY_KEY, CNSM_COB_PRIMACY>, Exception>() {
                            @Override
                            public KeyValue<CNSM_COB_PRIMACY_KEY, CNSM_COB_PRIMACY> doWithRetry(RetryContext context) throws Exception {
                                retryCount[0]++;
                                log.info("************************************ RETRY COUNT : " + retryCount[0]+ ":::::::" + key.toString());
                                //throw new SQLException("");
                                if (!value.getHeaders().getOperation().name().equals("REFRESH")) {
                                    if (!value.getHeaders().getOperation().name().equals("DELETE")) { //INSERT, UPDATE
                                        if (value.getCNSMCOBPRIMACYDATA().getROWUSERID().trim().equalsIgnoreCase("LINK/UNLINK")) {
                                            if (!value.getCNSMCOBPRIMACYDATA().getROWSTSCD().trim().equalsIgnoreCase("D")
                                                    && !value.getCNSMCOBPRIMACYDATA().getROWSTSCD().trim().equalsIgnoreCase("A")) {
                                                ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_COB_PRIMACY", false);
                                            }
                                            log.info("CNSM_COB_PRIMACY LINK/UNLINK case key  :::" + key.toString());
                                        } else { // NOT LINK/UNLINK cases
                                            ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_COB_PRIMACY", false);
                                            log.info("CNSM_COB_PRIMACY written key  :::" + key.toString());
                                        }
                                    } else { //PHYSICAL DELETE
                                        ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_COB_PRIMACY", true);
                                        log.info("CNSM_COB_PRIMACY physical delete key  :::" + key.toString());
                                    }
                                }
                                return KeyValue.pair(key, value);
                            }
                        });
                    } catch (Throwable throwable) {

                        log.error("SQLException in Table CNSM_COB_PRIMACY :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());

                      //  log.error("SQLException in Table CNSM_COB_PRIMACY :::time::::" + sdf.format(new Date()) + ":::::::" + new MetadataTransformer());

                        log.error("Exception",throwable);
                        //throw new RuntimeException("");
                        log.error("Restarting CNSM_COB_PRIMACY POD"); System.exit(1);
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


    //The getROWSTSCD is not present in the getCNSMCOBPRISECDATA class
    @Bean
    public KStream<CNSM_COB_PRISEC_KEY, CNSM_COB_PRISEC> cnsmCobPrisec(StreamsBuilder builder) {


        KStream<CNSM_COB_PRISEC_KEY, CNSM_COB_PRISEC>
                memberKStream = builder.stream(topicConfig.getCnsmCobPrisec());

        try {
            memberKStream.map((key, value) -> {
                final int[] retryCount = {0};
                log.info("CNSM_COB_PRISEC Consuming key  :::" + key.toString());
                //log.info("CNSM_COB_PRISEC Consuming value  :::" + value.toString());

                try {
                    if (!value.getHeaders().getOperation().name().equals("REFRESH")) {
                        if (!value.getHeaders().getOperation().name().equals("DELETE")) { //INSERT, UPDATE
                            if (value.getCNSMCOBPRISECDATA().getROWUSERID().trim().equalsIgnoreCase("LINK/UNLINK")) {
                                String rowstscd = ingestionLogDAO.rowstscd(value.toString());
                                if (!rowstscd.trim().equalsIgnoreCase("D")
                                        && !rowstscd.trim().equalsIgnoreCase("A")) {
                                    ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_COB_PRISEC", false);
                                }
                                log.info("CNSM_COB_PRISEC LINK/UNLINK case key  :::" + key.toString());
                            } else { // NOT LINK/UNLINK cases
                                ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_COB_PRISEC", false);
                                log.info("CNSM_COB_PRISEC written key  :::" + key.toString());
                            }
                        } else { //PHYSICAL DELETE
                            ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_COB_PRISEC", true);
                            log.info("CNSM_COB_PRISEC physical delete key  :::" + key.toString());
                        }
                    }
                }
                catch (Exception e) {
                    try {
                        retryTemplate().execute(new RetryCallback<KeyValue<CNSM_COB_PRISEC_KEY, CNSM_COB_PRISEC>, Exception>() {
                            @Override
                            public KeyValue<CNSM_COB_PRISEC_KEY, CNSM_COB_PRISEC> doWithRetry(RetryContext context) throws Exception {
                                retryCount[0]++;
                                log.info("************************************ RETRY COUNT : " + retryCount[0]+ ":::::::" + key.toString());
                                //throw new SQLException("");
                                if (!value.getHeaders().getOperation().name().equals("REFRESH")) {
                                    if (!value.getHeaders().getOperation().name().equals("DELETE")) { //INSERT, UPDATE
                                        if (value.getCNSMCOBPRISECDATA().getROWUSERID().trim().equalsIgnoreCase("LINK/UNLINK")) {
                                            String rowstscd = ingestionLogDAO.rowstscd(value.toString());
                                            if (!rowstscd.trim().equalsIgnoreCase("D")
                                                    && !rowstscd.trim().equalsIgnoreCase("A")) {
                                                ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_COB_PRISEC", false);
                                            }
                                            log.info("CNSM_COB_PRISEC LINK/UNLINK case key  :::" + key.toString());
                                        } else { // NOT LINK/UNLINK cases
                                            ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_COB_PRISEC", false);
                                            log.info("CNSM_COB_PRISEC written key  :::" + key.toString());
                                        }
                                    } else { //PHYSICAL DELETE
                                        ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_COB_PRISEC", true);
                                        log.info("CNSM_COB_PRISEC physical delete key  :::" + key.toString());
                                    }
                                }

                                return KeyValue.pair(key, value);
                            }
                        });
                    } catch (Throwable throwable) {

                        log.error("SQLException in Table CNSM_COB_PRISEC :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());

                        //log.error("SQLException in Table CNSM_COB_PRISEC :::time::::" + sdf.format(new Date()) + ":::::::" + new MetadataTransformer());

                        log.error("Exception",throwable);
                        //throw new RuntimeException("");
                        log.error("Restarting CNSM_COB_PRISEC POD"); System.exit(1);
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




    @Bean
    public KStream<CNSM_CUST_DEFN_FLD_KEY, CNSM_CUST_DEFN_FLD> cnsmCustDefnFldStream(StreamsBuilder builder) {


        KStream<CNSM_CUST_DEFN_FLD_KEY, CNSM_CUST_DEFN_FLD>
                memberKStream = builder.stream(topicConfig.getCnsmCustDefnFld());

        try {
            memberKStream.map((key, value) -> {
                final int[] retryCount = {0};
                log.info("CNSM_CUST_DEFN_FLD Consuming key  :::" + key.toString());
               // log.info("CNSM_CUST_DEFN_FLD Consuming value  :::" + value.toString());

                try {
                    if (!value.getHeaders().getOperation().name().equals("REFRESH")) {
                        if (!value.getHeaders().getOperation().name().equals("DELETE")) { //INSERT, UPDATE
                            if (value.getCNSMCUSTDEFNFLDDATA().getROWUSERID().trim().equalsIgnoreCase("LINK/UNLINK")) {
                                if (!value.getCNSMCUSTDEFNFLDDATA().getROWSTSCD().trim().equalsIgnoreCase("D")
                                        && !value.getCNSMCUSTDEFNFLDDATA().getROWSTSCD().trim().equalsIgnoreCase("A")) {
                                    ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_CUST_DEFN_FLD", false);
                                }
                                log.info("CNSM_CUST_DEFN_FLD LINK/UNLINK case key  :::" + key.toString());
                            } else { // NOT LINK/UNLINK cases
                                ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_CUST_DEFN_FLD", false);
                                log.info("CNSM_CUST_DEFN_FLD written key  :::" + key.toString());
                            }
                        } else { //PHYSICAL DELETE
                            ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_CUST_DEFN_FLD", true);
                            log.info("CNSM_CUST_DEFN_FLD physical delete key  :::" + key.toString());
                        }
                    }
                }      catch (Exception e) {
                    try {
                        retryTemplate().execute(new RetryCallback<KeyValue<CNSM_CUST_DEFN_FLD_KEY, CNSM_CUST_DEFN_FLD>, Exception>() {
                            @Override
                            public KeyValue<CNSM_CUST_DEFN_FLD_KEY, CNSM_CUST_DEFN_FLD> doWithRetry(RetryContext context) throws Exception {
                                retryCount[0]++;
                                log.info("************************************ RETRY COUNT : " + retryCount[0]+ ":::::::" + key.toString());
                                //throw new SQLException("");
                                if (!value.getHeaders().getOperation().name().equals("REFRESH")) {
                                    if (!value.getHeaders().getOperation().name().equals("DELETE")) { //INSERT, UPDATE
                                        if (value.getCNSMCUSTDEFNFLDDATA().getROWUSERID().trim().equalsIgnoreCase("LINK/UNLINK")) {
                                            if (!value.getCNSMCUSTDEFNFLDDATA().getROWSTSCD().trim().equalsIgnoreCase("D")
                                                    && !value.getCNSMCUSTDEFNFLDDATA().getROWSTSCD().trim().equalsIgnoreCase("A")) {
                                                ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_CUST_DEFN_FLD", false);
                                            }
                                            log.info("CNSM_CUST_DEFN_FLD LINK/UNLINK case key  :::" + key.toString());
                                        } else { // NOT LINK/UNLINK cases
                                            ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_CUST_DEFN_FLD", false);
                                            log.info("CNSM_CUST_DEFN_FLD written key  :::" + key.toString());
                                        }
                                    } else { //PHYSICAL DELETE
                                        ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_CUST_DEFN_FLD", true);
                                        log.info("CNSM_CUST_DEFN_FLD physical delete key  :::" + key.toString());
                                    }
                                }

                                return KeyValue.pair(key, value);
                            }
                        });
                    } catch (Throwable throwable) {

                        log.error("SQLException in Table CNSM_CUST_DEFN_FLD :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());

                       // log.error("SQLException in Table CNSM_CUST_DEFN_FLD :::time::::" + sdf.format(new Date()) + ":::::::" + new MetadataTransformer());

                        log.error("Exception",throwable);
                        //throw new RuntimeException("");
                        log.error("Restarting CNSM_CUST_DEFN_FLD POD"); System.exit(1);
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

    @Bean
    public KStream<CNSM_COV_CUST_DEFN_FLD_KEY, CNSM_COV_CUST_DEFN_FLD> cnsmCovCustDefnFldStream(StreamsBuilder builder) {


        KStream<CNSM_COV_CUST_DEFN_FLD_KEY, CNSM_COV_CUST_DEFN_FLD>
                memberKStream = builder.stream(topicConfig.getCnsmCovCustDefnFld());

        try {
            memberKStream.map((key, value) -> {
                final int[] retryCount = {0};
                log.info("CNSM_COV_CUST_DEFN_FLD Consuming key  :::" + key.toString());
              //  log.info("CNSM_COV_CUST_DEFN_FLD Consuming value  :::" + value.toString());

                try {
                    if (!value.getHeaders().getOperation().name().equals("REFRESH")) {
                        if (!value.getHeaders().getOperation().name().equals("DELETE")) { //INSERT, UPDATE
                            if (value.getCNSMCOVCUSTDEFNFLDDATA().getROWUSERID().trim().equalsIgnoreCase("LINK/UNLINK")) {
                                if (!value.getCNSMCOVCUSTDEFNFLDDATA().getROWSTSCD().trim().equalsIgnoreCase("D")
                                        && !value.getCNSMCOVCUSTDEFNFLDDATA().getROWSTSCD().trim().equalsIgnoreCase("A")) {
                                    ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_COV_CUST_DEFN_FLD", false);
                                }
                                log.info("CNSM_COV_CUST_DEFN_FLD LINK/UNLINK case key  :::" + key.toString());
                            } else { // NOT LINK/UNLINK cases
                                ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_COV_CUST_DEFN_FLD", false);
                                log.info("CNSM_COV_CUST_DEFN_FLD written key  :::" + key.toString());
                            }
                        } else { //PHYSICAL DELETE
                            ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_COV_CUST_DEFN_FLD", true);
                            log.info("CNSM_COV_CUST_DEFN_FLD physical delete key  :::" + key.toString());
                        }
                    }
                }      catch (Exception e) {
                    try {
                        retryTemplate().execute(new RetryCallback<KeyValue<CNSM_COV_CUST_DEFN_FLD_KEY, CNSM_COV_CUST_DEFN_FLD>, Exception>() {
                            @Override
                            public KeyValue<CNSM_COV_CUST_DEFN_FLD_KEY, CNSM_COV_CUST_DEFN_FLD> doWithRetry(RetryContext context) throws Exception {
                                retryCount[0]++;
                                log.info("************************************ RETRY COUNT : " + retryCount[0]+ ":::::::" + key.toString());
                                //throw new SQLException("");
                                if (!value.getHeaders().getOperation().name().equals("REFRESH")) {
                                    if (!value.getHeaders().getOperation().name().equals("DELETE")) { //INSERT, UPDATE
                                        if (value.getCNSMCOVCUSTDEFNFLDDATA().getROWUSERID().trim().equalsIgnoreCase("LINK/UNLINK")) {
                                            if (!value.getCNSMCOVCUSTDEFNFLDDATA().getROWSTSCD().trim().equalsIgnoreCase("D")
                                                    && !value.getCNSMCOVCUSTDEFNFLDDATA().getROWSTSCD().trim().equalsIgnoreCase("A")) {
                                                ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_COV_CUST_DEFN_FLD", false);
                                            }
                                            log.info("CNSM_COV_CUST_DEFN_FLD LINK/UNLINK case key  :::" + key.toString());
                                        } else { // NOT LINK/UNLINK cases
                                            ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_COV_CUST_DEFN_FLD", false);
                                            log.info("CNSM_COV_CUST_DEFN_FLD written key  :::" + key.toString());
                                        }
                                    } else { //PHYSICAL DELETE
                                        ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_COV_CUST_DEFN_FLD", true);
                                        log.info("CNSM_COV_CUST_DEFN_FLD physical delete key  :::" + key.toString());
                                    }
                                }

                                return KeyValue.pair(key, value);
                            }
                        });
                    } catch (Throwable throwable) {

                        log.error("SQLException in Table CNSM_COV_CUST_DEFN_FLD :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());

                      //  log.error("SQLException in Table CNSM_COV_CUST_DEFN_FLD :::time::::" + sdf.format(new Date()) + ":::::::" + new MetadataTransformer());

                        log.error("Exception",throwable);
                        //throw new RuntimeException("");
                        log.error("Restarting CNSM_COV_CUST_DEFN_FLD POD"); System.exit(1);
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


    @Bean
    public KStream<CNSM_EFT_KEY, CNSM_EFT> cnsmEftStream(StreamsBuilder builder) {


        KStream<CNSM_EFT_KEY, CNSM_EFT>
                memberKStream = builder.stream(topicConfig.getCnsmEft());

        try {
            memberKStream.map((key, value) -> {
                final int[] retryCount = {0};
                log.info("CNSM_EFT Consuming key  :::" + key.toString());
               // log.info("CNSM_EFT Consuming value  :::" + value.toString());

                try {
                    if (!value.getHeaders().getOperation().name().equals("REFRESH")) {
                        if (!value.getHeaders().getOperation().name().equals("DELETE")) { //INSERT, UPDATE
                            if (value.getCNSMEFTDATA().getROWUSERID().trim().equalsIgnoreCase("LINK/UNLINK")) {
                                if (!value.getCNSMEFTDATA().getROWSTSCD().trim().equalsIgnoreCase("D")
                                        && !value.getCNSMEFTDATA().getROWSTSCD().trim().equalsIgnoreCase("A")) {
                                    ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_EFT", false);
                                }
                                log.info("CNSM_EFT LINK/UNLINK case key  :::" + key.toString());
                            } else { // NOT LINK/UNLINK cases
                                ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_EFT", false);
                                log.info("CNSM_EFT written key  :::" + key.toString());
                            }
                        } else { //PHYSICAL DELETE
                            ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_EFT", true);
                            log.info("CNSM_EFT physical delete key  :::" + key.toString());
                        }
                    }
                }      catch (Exception e) {
                    try {
                        retryTemplate().execute(new RetryCallback<KeyValue<CNSM_EFT_KEY, CNSM_EFT>, Exception>() {
                            @Override
                            public KeyValue<CNSM_EFT_KEY, CNSM_EFT> doWithRetry(RetryContext context) throws Exception {
                                retryCount[0]++;
                                log.info("************************************ RETRY COUNT : " + retryCount[0]+ ":::::::" + key.toString());
                                //throw new SQLException("");
                                if (!value.getHeaders().getOperation().name().equals("REFRESH")) {
                                    if (!value.getHeaders().getOperation().name().equals("DELETE")) { //INSERT, UPDATE
                                        if (value.getCNSMEFTDATA().getROWUSERID().trim().equalsIgnoreCase("LINK/UNLINK")) {
                                            if (!value.getCNSMEFTDATA().getROWSTSCD().trim().equalsIgnoreCase("D")
                                                    && !value.getCNSMEFTDATA().getROWSTSCD().trim().equalsIgnoreCase("A")) {
                                                ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_EFT", false);
                                            }
                                            log.info("CNSM_EFT LINK/UNLINK case key  :::" + key.toString());
                                        } else { // NOT LINK/UNLINK cases
                                            ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_EFT", false);
                                            log.info("CNSM_EFT written key  :::" + key.toString());
                                        }
                                    } else { //PHYSICAL DELETE
                                        ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_EFT", true);
                                        log.info("CNSM_EFT physical delete key  :::" + key.toString());
                                    }
                                }

                                return KeyValue.pair(key, value);
                            }
                        });
                    } catch (Throwable throwable) {

                        log.error("SQLException in Table CNSM_EFT :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());

                        log.error("SQLException in Table CNSM_EFT :::time::::" + sdf.format(new Date()) + ":::::::" + new MetadataTransformer());

                        log.error("Exception",throwable);
                        //throw new RuntimeException("");
                        log.error("Restarting CNSM_EFT POD"); System.exit(1);
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

    @Bean
    public KStream<CNSM_MDCR_ELIG_KEY, CNSM_MDCR_ELIG> cnsmMdcrEligStream(StreamsBuilder builder) {


        KStream<CNSM_MDCR_ELIG_KEY, CNSM_MDCR_ELIG>
                memberKStream = builder.stream(topicConfig.getCnsmMdcrElig());

        try {
            memberKStream.map((key, value) -> {
                final int[] retryCount = {0};
                log.info("CNSM_MDCR_ELIG Consuming key  :::" + key.toString());
              // log.info("CNSM_MDCR_ELIG Consuming value  :::" + value.toString());

                try {
                    if (!value.getHeaders().getOperation().name().equals("REFRESH")) {
                        if (!value.getHeaders().getOperation().name().equals("DELETE")) { //INSERT, UPDATE
                            if (value.getCNSMMDCRELIGDATA().getROWUSERID().trim().equalsIgnoreCase("LINK/UNLINK")) {
                                if (!value.getCNSMMDCRELIGDATA().getROWSTSCD().trim().equalsIgnoreCase("D")
                                        && !value.getCNSMMDCRELIGDATA().getROWSTSCD().trim().equalsIgnoreCase("A")) {
                                    ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_MDCR_ELIG", false);
                                }
                                log.info("CNSM_MDCR_ELIG LINK/UNLINK case key  :::" + key.toString());
                            } else { // NOT LINK/UNLINK cases
                                ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_MDCR_ELIG", false);
                                log.info("CNSM_MDCR_ELIG written key  :::" + key.toString());
                            }
                        } else { //PHYSICAL DELETE
                            ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_MDCR_ELIG", true);
                            log.info("CNSM_MDCR_ELIG physical delete key  :::" + key.toString());
                        }
                    }
                }      catch (Exception e) {
                    try {
                        retryTemplate().execute(new RetryCallback<KeyValue<CNSM_MDCR_ELIG_KEY, CNSM_MDCR_ELIG>, Exception>() {
                            @Override
                            public KeyValue<CNSM_MDCR_ELIG_KEY, CNSM_MDCR_ELIG> doWithRetry(RetryContext context) throws Exception {
                                retryCount[0]++;
                                log.info("************************************ RETRY COUNT : " + retryCount[0]+ ":::::::" + key.toString());
                                //throw new SQLException("");
                                if (!value.getHeaders().getOperation().name().equals("REFRESH")) {
                                    if (!value.getHeaders().getOperation().name().equals("DELETE")) { //INSERT, UPDATE
                                        if (value.getCNSMMDCRELIGDATA().getROWUSERID().trim().equalsIgnoreCase("LINK/UNLINK")) {
                                            if (!value.getCNSMMDCRELIGDATA().getROWSTSCD().trim().equalsIgnoreCase("D")
                                                    && !value.getCNSMMDCRELIGDATA().getROWSTSCD().trim().equalsIgnoreCase("A")) {
                                                ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_MDCR_ELIG", false);
                                            }
                                            log.info("CNSM_MDCR_ELIG LINK/UNLINK case key  :::" + key.toString());
                                        } else { // NOT LINK/UNLINK cases
                                            ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_MDCR_ELIG", false);
                                            log.info("CNSM_MDCR_ELIG written key  :::" + key.toString());
                                        }
                                    } else { //PHYSICAL DELETE
                                        ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_MDCR_ELIG", true);
                                        log.info("CNSM_MDCR_ELIG physical delete key  :::" + key.toString());
                                    }
                                }

                                return KeyValue.pair(key, value);
                            }
                        });
                    } catch (Throwable throwable) {

                        log.error("SQLException in Table CNSM_MDCR_ELIG :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());

                        log.error("SQLException in Table CNSM_MDCR_ELIG :::time::::" + sdf.format(new Date()) + ":::::::" + new MetadataTransformer());

                        log.error("Exception",throwable);
                        //throw new RuntimeException("");
                        log.error("Restarting CNSM_MDCR_ELIG POD"); System.exit(1);
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


    @Bean
    public KStream<CNSM_MDCR_PRIMACY_KEY, CNSM_MDCR_PRIMACY> cnsmMdcrPrimacyStream(StreamsBuilder builder) {


        KStream<CNSM_MDCR_PRIMACY_KEY, CNSM_MDCR_PRIMACY>
                memberKStream = builder.stream(topicConfig.getCnsmMdcrPrimacy());

        try {
            memberKStream.map((key, value) -> {
                final int[] retryCount = {0};
                log.info("CNSM_MDCR_PRIMACY Consuming key  :::" + key.toString());
               // log.info("CNSM_MDCR_PRIMACY Consuming value  :::" + value.toString());

                try {
                    if (!value.getHeaders().getOperation().name().equals("REFRESH")) {
                        if (!value.getHeaders().getOperation().name().equals("DELETE")) { //INSERT, UPDATE
                            if (value.getCNSMMDCRPRIMACYDATA().getROWUSERID().trim().equalsIgnoreCase("LINK/UNLINK")) {
                                if (!value.getCNSMMDCRPRIMACYDATA().getROWSTSCD().trim().equalsIgnoreCase("D")
                                        && !value.getCNSMMDCRPRIMACYDATA().getROWSTSCD().trim().equalsIgnoreCase("A")) {
                                    ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_MDCR_PRIMACY", false);
                                }
                                log.info("CNSM_MDCR_PRIMACY LINK/UNLINK case key  :::" + key.toString());
                            } else { // NOT LINK/UNLINK cases
                                ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_MDCR_PRIMACY", false);
                                log.info("CNSM_MDCR_PRIMACY written key  :::" + key.toString());
                            }
                        } else { //PHYSICAL DELETE
                            ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_MDCR_PRIMACY", true);
                            log.info("CNSM_MDCR_PRIMACY physical delete key  :::" + key.toString());
                        }
                    }
                }      catch (Exception e) {
                    try {
                        retryTemplate().execute(new RetryCallback<KeyValue<CNSM_MDCR_PRIMACY_KEY, CNSM_MDCR_PRIMACY>, Exception>() {
                            @Override
                            public KeyValue<CNSM_MDCR_PRIMACY_KEY, CNSM_MDCR_PRIMACY> doWithRetry(RetryContext context) throws Exception {
                                retryCount[0]++;
                                log.info("************************************ RETRY COUNT : " + retryCount[0]+ ":::::::" + key.toString());
                                //throw new SQLException("");
                                if (!value.getHeaders().getOperation().name().equals("REFRESH")) {
                                    if (!value.getHeaders().getOperation().name().equals("DELETE")) { //INSERT, UPDATE
                                        if (value.getCNSMMDCRPRIMACYDATA().getROWUSERID().trim().equalsIgnoreCase("LINK/UNLINK")) {
                                            if (!value.getCNSMMDCRPRIMACYDATA().getROWSTSCD().trim().equalsIgnoreCase("D")
                                                    && !value.getCNSMMDCRPRIMACYDATA().getROWSTSCD().trim().equalsIgnoreCase("A")) {
                                                ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_MDCR_PRIMACY", false);
                                            }
                                            log.info("CNSM_MDCR_PRIMACY LINK/UNLINK case key  :::" + key.toString());
                                        } else { // NOT LINK/UNLINK cases
                                            ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_MDCR_PRIMACY", false);
                                            log.info("CNSM_MDCR_PRIMACY written key  :::" + key.toString());
                                        }
                                    } else { //PHYSICAL DELETE
                                        ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_MDCR_PRIMACY", true);
                                        log.info("CNSM_MDCR_PRIMACY physical delete key  :::" + key.toString());
                                    }
                                }
                                return KeyValue.pair(key, value);
                            }
                        });
                    } catch (Throwable throwable) {

                        log.error("SQLException in Table CNSM_MDCR_PRIMACY :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());

                        log.error("SQLException in Table CNSM_MDCR_PRIMACY :::time::::" + sdf.format(new Date()) + ":::::::" + new MetadataTransformer());

                        log.error("Exception",throwable);
                        //throw new RuntimeException("");
                        log.error("Restarting CNSM_MDCR_PRIMACY POD"); System.exit(1);
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


    @Bean
    public KStream<CNSM_PRXST_COND_KEY, CNSM_PRXST_COND> cnsmPrxstCondStream(StreamsBuilder builder) {


        KStream<CNSM_PRXST_COND_KEY, CNSM_PRXST_COND>
                memberKStream = builder.stream(topicConfig.getCnsmPrxstCond());

        try {
            memberKStream.map((key, value) -> {
                final int[] retryCount = {0};
                log.info("CNSM_PRXST_COND Consuming key  :::" + key.toString());
              //  log.info("CNSM_PRXST_COND Consuming value  :::" + value.toString());

                try {
                    if (!value.getHeaders().getOperation().name().equals("REFRESH")) {
                        if (!value.getHeaders().getOperation().name().equals("DELETE")) { //INSERT, UPDATE
                            if (value.getCNSMPRXSTCONDDATA().getROWUSERID().trim().equalsIgnoreCase("LINK/UNLINK")) {
                                if (!value.getCNSMPRXSTCONDDATA().getROWSTSCD().trim().equalsIgnoreCase("D")
                                        && !value.getCNSMPRXSTCONDDATA().getROWSTSCD().trim().equalsIgnoreCase("A")) {
                                    ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_PRXST_COND", false);
                                }
                                log.info("CNSM_PRXST_COND LINK/UNLINK case key  :::" + key.toString());
                            } else { // NOT LINK/UNLINK cases
                                ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_PRXST_COND", false);
                                log.info("CNSM_PRXST_COND written key  :::" + key.toString());
                            }
                        } else { //PHYSICAL DELETE
                            ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_PRXST_COND", true);
                            log.info("CNSM_PRXST_COND physical delete key  :::" + key.toString());
                        }
                    }
                }      catch (Exception e) {
                    try {
                        retryTemplate().execute(new RetryCallback<KeyValue<CNSM_PRXST_COND_KEY, CNSM_PRXST_COND>, Exception>() {
                            @Override
                            public KeyValue<CNSM_PRXST_COND_KEY, CNSM_PRXST_COND> doWithRetry(RetryContext context) throws Exception {
                                retryCount[0]++;
                                log.info("************************************ RETRY COUNT : " + retryCount[0]+ ":::::::" + key.toString());
                                //throw new SQLException("");
                                if (!value.getHeaders().getOperation().name().equals("REFRESH")) {
                                    if (!value.getHeaders().getOperation().name().equals("DELETE")) { //INSERT, UPDATE
                                        if (value.getCNSMPRXSTCONDDATA().getROWUSERID().trim().equalsIgnoreCase("LINK/UNLINK")) {
                                            if (!value.getCNSMPRXSTCONDDATA().getROWSTSCD().trim().equalsIgnoreCase("D")
                                                    && !value.getCNSMPRXSTCONDDATA().getROWSTSCD().trim().equalsIgnoreCase("A")) {
                                                ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_PRXST_COND", false);
                                            }
                                            log.info("CNSM_PRXST_COND LINK/UNLINK case key  :::" + key.toString());
                                        } else { // NOT LINK/UNLINK cases
                                            ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_PRXST_COND", false);
                                            log.info("CNSM_PRXST_COND written key  :::" + key.toString());
                                        }
                                    } else { //PHYSICAL DELETE
                                        ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CNSM_PRXST_COND", true);
                                        log.info("CNSM_PRXST_COND physical delete key  :::" + key.toString());
                                    }
                                }
                                return KeyValue.pair(key, value);
                            }
                        });
                    } catch (Throwable throwable) {

                        log.error("SQLException in Table CNSM_PRXST_COND :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());

                        log.error("SQLException in Table CNSM_PRXST_COND :::time::::" + sdf.format(new Date()) + ":::::::" + new MetadataTransformer());

                        log.error("Exception",throwable);
                        //throw new RuntimeException("");
                        log.error("Restarting CNSM_PRXST_COND POD"); System.exit(1);
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



    @Bean
    public KStream<CNSM_SLRY_BAS_DED_OOP_KEY, CNSM_SLRY_BAS_DED_OOP> cnsmSlryBasDedOopStream(StreamsBuilder builder) {


        KStream<CNSM_SLRY_BAS_DED_OOP_KEY, CNSM_SLRY_BAS_DED_OOP>
                memberKStream = builder.stream(topicConfig.getCnsmSlryBasDedOop());

        try {
            memberKStream.map((key, value) -> {
                final int[] retryCount = {0};

                BigDecimal salaryYear = consumerUtil.convertByteToDecimal(value, 4, 4);
                log.info("CNSM_SLRY_BAS_DED_OOP Consuming key  :::" + key.toString() +"SLRY_YR "+salaryYear);
                // log.info("CNSM_SLRY_BAS_DED_OOP Consuming value  :::" + value.toString());
                try {
                    if (!value.getHeaders().getOperation().name().equals("REFRESH")) {
                        if (!value.getHeaders().getOperation().name().equals("DELETE")) { //INSERT, UPDATE
                            if (value.getCNSMSLRYBASDEDOOPDATA().getROWUSERID().trim().equalsIgnoreCase("LINK/UNLINK")) {
                                if (!value.getCNSMSLRYBASDEDOOPDATA().getROWSTSCD().trim().equalsIgnoreCase("D")
                                        && !value.getCNSMSLRYBASDEDOOPDATA().getROWSTSCD().trim().equalsIgnoreCase("A")) {
                                    ingestionLogDAO.ingestDataToIngestionLog(value.toString(), value, salaryYear.toString(), false);
                                }
                                log.info("CNSM_SLRY_BAS_DED_OOP LINK/UNLINK case key  :::" + key.toString() + "SLRY_YR " + salaryYear);
                            } else { // NOT LINK/UNLINK cases
                                ingestionLogDAO.ingestDataToIngestionLog(value.toString(), value, salaryYear.toString(), false);
                                log.info("CNSM_SLRY_BAS_DED_OOP written key  :::" + key.toString() + "SLRY_YR " + salaryYear);
                            }
                        } else { //PHYSICAL DELETE
                            ingestionLogDAO.ingestDataToIngestionLog(value.toString(), value, salaryYear.toString(), true);
                            log.info("CNSM_SLRY_BAS_DED_OOP physical delete key  :::" + key.toString() + "SLRY_YR " + salaryYear);
                        }
                    }
                }      catch (Exception e) {
                    try {
                        retryTemplate().execute(new RetryCallback<KeyValue<CNSM_SLRY_BAS_DED_OOP_KEY, CNSM_SLRY_BAS_DED_OOP>, Exception>() {
                            @Override
                            public KeyValue<CNSM_SLRY_BAS_DED_OOP_KEY, CNSM_SLRY_BAS_DED_OOP> doWithRetry(RetryContext context) throws Exception {
                                retryCount[0]++;
                                log.info("************************************ RETRY COUNT : " + retryCount[0]+ ":::::::" + key.toString());
                                //throw new SQLException("");
                                if (!value.getHeaders().getOperation().name().equals("REFRESH")) {
                                    if (!value.getHeaders().getOperation().name().equals("DELETE")) { //INSERT, UPDATE
                                        if (value.getCNSMSLRYBASDEDOOPDATA().getROWUSERID().trim().equalsIgnoreCase("LINK/UNLINK")) {
                                            if (!value.getCNSMSLRYBASDEDOOPDATA().getROWSTSCD().trim().equalsIgnoreCase("D")
                                                    && !value.getCNSMSLRYBASDEDOOPDATA().getROWSTSCD().trim().equalsIgnoreCase("A")) {
                                                ingestionLogDAO.ingestDataToIngestionLog(value.toString(), value, salaryYear.toString(), false);
                                            }
                                            log.info("CNSM_SLRY_BAS_DED_OOP LINK/UNLINK case key  :::" + key.toString() + "SLRY_YR " + salaryYear);
                                        } else { // NOT LINK/UNLINK cases
                                            ingestionLogDAO.ingestDataToIngestionLog(value.toString(), value, salaryYear.toString(), false);
                                            log.info("CNSM_SLRY_BAS_DED_OOP written key :::" + key.toString() + "SLRY_YR " + salaryYear);
                                        }
                                    } else { //PHYSICAL DELETE
                                        ingestionLogDAO.ingestDataToIngestionLog(value.toString(), value, salaryYear.toString(), true);
                                        log.info("CNSM_SLRY_BAS_DED_OOP physical delete key :::" + key.toString() + "SLRY_YR " + salaryYear);
                                    }
                                }
                                return KeyValue.pair(key, value);
                            }
                        });
                    } catch (Throwable throwable) {

                        log.error("SQLException in Table CNSM_SLRY_BAS_DED_OOP :time::::" + sdf.format(new Date()) + "::::::" + key.toString() +"SLRY_YR "+salaryYear);

                        log.error("SQLException in Table CNSM_SLRY_BAS_DED_OOP :::time::::" + sdf.format(new Date()) + ":::::" + key.toString() +"SLRY_YR "+salaryYear);

                        log.error("Exception",throwable);
                        //throw new RuntimeException("");
                        log.error("Restarting CNSM_SLRY_BAS_DED_OOP POD"); System.exit(1);
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


   @Bean
    public KStream<COV_INFO_KEY, COV_INFO> covInfoStream(StreamsBuilder builder) {


        KStream<COV_INFO_KEY, COV_INFO>
                memberKStream = builder.stream(topicConfig.getCovInfo());

        try {
            memberKStream.map((key, value) -> {
                final int[] retryCount = {0};
                log.info("COV_INFO Consuming key  :::" + key.toString());
              //  log.info("COV_INFO Consuming value  :::" + value.toString());

                try {
                    if (!value.getHeaders().getOperation().name().equals("REFRESH")) {
                        if (!value.getHeaders().getOperation().name().equals("DELETE")) { //INSERT, UPDATE
                            if (value.getCOVINFODATA().getROWUSERID().trim().equalsIgnoreCase("LINK/UNLINK")) {
                                if (!value.getCOVINFODATA().getROWSTSCD().trim().equalsIgnoreCase("D")
                                        && !value.getCOVINFODATA().getROWSTSCD().trim().equalsIgnoreCase("A")) {
                                    ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "COV_INFO", false);
                                }
                                log.info("COV_INFO LINK/UNLINK case key  :::" + key.toString());
                            } else { // NOT LINK/UNLINK cases
                                ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "COV_INFO", false);
                                log.info("COV_INFO written key  :::" + key.toString());
                            }
                        } else { //PHYSICAL DELETE
                            ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "COV_INFO", true);
                            log.info("COV_INFO physical delete key  :::" + key.toString());
                        }
                    }
                }      catch (Exception e) {
                    try {
                        retryTemplate().execute(new RetryCallback<KeyValue<COV_INFO_KEY, COV_INFO>, Exception>() {
                            @Override
                            public KeyValue<COV_INFO_KEY, COV_INFO> doWithRetry(RetryContext context) throws Exception {
                                retryCount[0]++;
                                log.info("************************************ RETRY COUNT : " + retryCount[0]+ ":::::::" + key.toString());
                                //throw new SQLException("");
                                if (!value.getHeaders().getOperation().name().equals("REFRESH")) {
                                    if (!value.getHeaders().getOperation().name().equals("DELETE")) { //INSERT, UPDATE
                                        if (value.getCOVINFODATA().getROWUSERID().trim().equalsIgnoreCase("LINK/UNLINK")) {
                                            if (!value.getCOVINFODATA().getROWSTSCD().trim().equalsIgnoreCase("D")
                                                    && !value.getCOVINFODATA().getROWSTSCD().trim().equalsIgnoreCase("A")) {
                                                ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "COV_INFO", false);
                                            }
                                            log.info("COV_INFO LINK/UNLINK case key  :::" + key.toString());
                                        } else { // NOT LINK/UNLINK cases
                                            ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "COV_INFO", false);
                                            log.info("COV_INFO written key  :::" + key.toString());
                                        }
                                    } else { //PHYSICAL DELETE
                                        ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "COV_INFO", true);
                                        log.info("COV_INFO physical delete key  :::" + key.toString());
                                    }
                                }

                                return KeyValue.pair(key, value);
                            }
                        });
                    } catch (Throwable throwable) {

                        log.error("SQLException in Table COV_INFO :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());

                        log.error("SQLException in Table COV_INFO :::time::::" + sdf.format(new Date()) + ":::::::" + new MetadataTransformer());

                        log.error("Exception",throwable);
                        //throw new RuntimeException("");
                        log.error("Restarting COV_INFO POD"); System.exit(1);
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

    @Bean
    public KStream<CUST_INFO_KEY, CUST_INFO> custInfoStream(StreamsBuilder builder) {


        KStream<CUST_INFO_KEY, CUST_INFO>
                memberKStream = builder.stream(topicConfig.getCustInfo());

        try {
            memberKStream.map((key, value) -> {
                final int[] retryCount = {0};
                log.info("CUST_INFO Consuming key  :::" + key.toString());
               // log.info("CUST_INFO Consuming value  :::" + value.toString());

                try {
                    if (!value.getHeaders().getOperation().name().equals("REFRESH")) {
                        if (!value.getHeaders().getOperation().name().equals("DELETE")) { //INSERT, UPDATE
                            if (value.getCUSTINFODATA().getROWUSERID().trim().equalsIgnoreCase("LINK/UNLINK")) {
                                if (!value.getCUSTINFODATA().getROWSTSCD().trim().equalsIgnoreCase("D")
                                        && !value.getCUSTINFODATA().getROWSTSCD().trim().equalsIgnoreCase("A")) {
                                    ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CUST_INFO", false);
                                }
                                log.info("CUST_INFO LINK/UNLINK case key  :::" + key.toString());
                            } else { // NOT LINK/UNLINK cases
                                ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CUST_INFO", false);
                                log.info("CUST_INFO written key  :::" + key.toString());
                            }
                        } else { //PHYSICAL DELETE
                            ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CUST_INFO", true);
                            log.info("CUST_INFO physical delete key  :::" + key.toString());
                        }
                    }
                }     catch (Exception e) {
                    try {
                        retryTemplate().execute(new RetryCallback<KeyValue<CUST_INFO_KEY, CUST_INFO>, Exception>() {
                            @Override
                            public KeyValue<CUST_INFO_KEY, CUST_INFO> doWithRetry(RetryContext context) throws Exception {
                                retryCount[0]++;
                                log.info("************************************ RETRY COUNT : " + retryCount[0]+ ":::::::" + key.toString());
                                //throw new SQLException("");
                                if (!value.getHeaders().getOperation().name().equals("REFRESH")) {
                                    if (!value.getHeaders().getOperation().name().equals("DELETE")) { //INSERT, UPDATE
                                        if (value.getCUSTINFODATA().getROWUSERID().trim().equalsIgnoreCase("LINK/UNLINK")) {
                                            if (!value.getCUSTINFODATA().getROWSTSCD().trim().equalsIgnoreCase("D")
                                                    && !value.getCUSTINFODATA().getROWSTSCD().trim().equalsIgnoreCase("A")) {
                                                ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CUST_INFO", false);
                                            }
                                            log.info("CUST_INFO LINK/UNLINK case key  :::" + key.toString());
                                        } else { // NOT LINK/UNLINK cases
                                            ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CUST_INFO", false);
                                            log.info("CUST_INFO written key  :::" + key.toString());
                                        }
                                    } else { //PHYSICAL DELETE
                                        ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "CUST_INFO", true);
                                        log.info("CUST_INFO physical delete key  :::" + key.toString());
                                    }
                                }
                                return KeyValue.pair(key, value);
                            }
                        });
                    } catch (Throwable throwable) {

                        log.error("SQLException in Table CUST_INFO :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());

                        log.error("SQLException in Table CUST_INFO :::time::::" + sdf.format(new Date()) + ":::::::" + new MetadataTransformer());

                        log.error("Exception",throwable);
                        //throw new RuntimeException("");
                        log.error("Restarting CUST_INFO POD"); System.exit(1);
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

    @Bean
    public KStream<PLN_BEN_SET_KEY, PLN_BEN_SET> plnBenSet(StreamsBuilder builder) {


        KStream<PLN_BEN_SET_KEY, PLN_BEN_SET>
                memberKStream = builder.stream(topicConfig.getPlnBenSet());

        try {
            memberKStream.map((key, value) -> {
                final int[] retryCount = {0};
                log.info("PLN_BEN_SET Consuming key  :::" + key.toString());
               // log.info("PLN_BEN_SET Consuming value  :::" + value.toString());

                try {
                    if (!value.getHeaders().getOperation().name().equals("REFRESH")) {
                        if (!value.getHeaders().getOperation().name().equals("DELETE")) { //INSERT, UPDATE
                            if (value.getPLNBENSETDATA().getROWUSERID().trim().equalsIgnoreCase("LINK/UNLINK")) {
                                if (!value.getPLNBENSETDATA().getROWSTSCD().trim().equalsIgnoreCase("D")
                                        && !value.getPLNBENSETDATA().getROWSTSCD().trim().equalsIgnoreCase("A")) {
                                    ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "PLN_BEN_SET", false);
                                }
                                log.info("PLN_BEN_SET LINK/UNLINK case key  :::" + key.toString());
                            } else { // NOT LINK/UNLINK cases
                                ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "PLN_BEN_SET", false);
                                log.info("PLN_BEN_SET written key  :::" + key.toString());
                            }
                        } else { //PHYSICAL DELETE
                            ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "PLN_BEN_SET", true);
                            log.info("PLN_BEN_SET physical delete key  :::" + key.toString());
                        }
                    }
                }      catch (Exception e) {
                    try {
                        retryTemplate().execute(new RetryCallback<KeyValue<PLN_BEN_SET_KEY, PLN_BEN_SET>, Exception>() {
                            @Override
                            public KeyValue<PLN_BEN_SET_KEY, PLN_BEN_SET> doWithRetry(RetryContext context) throws Exception {
                                retryCount[0]++;
                                log.info("************************************ RETRY COUNT : " + retryCount[0]+ ":::::::" + key.toString());
                                //throw new SQLException("");
                                if (!value.getHeaders().getOperation().name().equals("REFRESH")) {
                                    if (!value.getHeaders().getOperation().name().equals("DELETE")) { //INSERT, UPDATE
                                        if (value.getPLNBENSETDATA().getROWUSERID().trim().equalsIgnoreCase("LINK/UNLINK")) {
                                            if (!value.getPLNBENSETDATA().getROWSTSCD().trim().equalsIgnoreCase("D")
                                                    && !value.getPLNBENSETDATA().getROWSTSCD().trim().equalsIgnoreCase("A")) {
                                                ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "PLN_BEN_SET", false);
                                            }
                                            log.info("PLN_BEN_SET LINK/UNLINK case key  :::" + key.toString());
                                        } else { // NOT LINK/UNLINK cases
                                            ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "PLN_BEN_SET", false);
                                            log.info("PLN_BEN_SET written key  :::" + key.toString());
                                        }
                                    } else { //PHYSICAL DELETE
                                        ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "PLN_BEN_SET", true);
                                        log.info("PLN_BEN_SET physical delete key  :::" + key.toString());
                                    }
                                }
                                return KeyValue.pair(key, value);
                            }
                        });
                    } catch (Throwable throwable) {

                        log.error("SQLException in Table PLN_BEN_SET :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());

                        log.error("SQLException in Table PLN_BEN_SET :::time::::" + sdf.format(new Date()) + ":::::::" + new MetadataTransformer());

                        log.error("Exception",throwable);
                        //throw new RuntimeException("");
                        log.error("Restarting PLN_BEN_SET POD"); System.exit(1);
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

    @Bean
    public KStream<PLN_BEN_SET_DET_KEY, PLN_BEN_SET_DET> plnBenSetDetStream(StreamsBuilder builder) {


        KStream<PLN_BEN_SET_DET_KEY, PLN_BEN_SET_DET>
                memberKStream = builder.stream(topicConfig.getPlnBenSetDet());

        try {
            memberKStream.map((key, value) -> {
                final int[] retryCount = {0};
                log.info("PLN_BEN_SET_DET Consuming key  :::" + key.toString());
              //  log.info("PLN_BEN_SET_DET Consuming value  :::" + value.toString());

                try {
                    if (!value.getHeaders().getOperation().name().equals("REFRESH")) {
                        if (!value.getHeaders().getOperation().name().equals("DELETE")) { //INSERT, UPDATE
                            if (value.getPLNBENSETDETDATA().getROWUSERID().trim().equalsIgnoreCase("LINK/UNLINK")) {
                                if (!value.getPLNBENSETDETDATA().getROWSTSCD().trim().equalsIgnoreCase("D")
                                        && !value.getPLNBENSETDETDATA().getROWSTSCD().trim().equalsIgnoreCase("A")) {
                                    ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "PLN_BEN_SET_DET", false);
                                }
                                log.info("PLN_BEN_SET_DET LINK/UNLINK case key  :::" + key.toString());
                            } else { // NOT LINK/UNLINK cases
                                ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "PLN_BEN_SET_DET", false);
                                log.info("PLN_BEN_SET_DET written key  :::" + key.toString());
                            }
                        } else { //PHYSICAL DELETE
                            ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "PLN_BEN_SET_DET", true);
                            log.info("PLN_BEN_SET_DET physical delete key  :::" + key.toString());
                        }
                    }
                } catch (Exception e) {
                    try {
                        retryTemplate().execute(new RetryCallback<KeyValue<PLN_BEN_SET_DET_KEY, PLN_BEN_SET_DET>, Exception>() {
                            @Override
                            public KeyValue<PLN_BEN_SET_DET_KEY, PLN_BEN_SET_DET> doWithRetry(RetryContext context) throws Exception {
                                retryCount[0]++;
                                log.info("************************************ RETRY COUNT : " + retryCount[0] + ":::::::" + key.toString());
                                //throw new SQLException("");
                                if (!value.getHeaders().getOperation().name().equals("REFRESH")) {
                                    if (!value.getHeaders().getOperation().name().equals("DELETE")) { //INSERT, UPDATE
                                        if (value.getPLNBENSETDETDATA().getROWUSERID().trim().equalsIgnoreCase("LINK/UNLINK")) {
                                            if (!value.getPLNBENSETDETDATA().getROWSTSCD().trim().equalsIgnoreCase("D")
                                                    && !value.getPLNBENSETDETDATA().getROWSTSCD().trim().equalsIgnoreCase("A")) {
                                                ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "PLN_BEN_SET_DET", false);
                                            }
                                            log.info("PLN_BEN_SET_DET LINK/UNLINK case key  :::" + key.toString());
                                        } else { // NOT LINK/UNLINK cases
                                            ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "PLN_BEN_SET_DET", false);
                                            log.info("PLN_BEN_SET_DET written key  :::" + key.toString());
                                        }
                                    } else { //PHYSICAL DELETE
                                        ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "PLN_BEN_SET_DET", true);
                                        log.info("PLN_BEN_SET_DET physical delete key  :::" + key.toString());
                                    }
                                }
                                return KeyValue.pair(key, value);
                            }
                        });
                    } catch (Throwable throwable) {

                        log.error("SQLException in Table PLN_BEN_SET_DET :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());

                        log.error("SQLException in Table PLN_BEN_SET_DET :::time::::" + sdf.format(new Date()) + ":::::::" + new MetadataTransformer());

                        log.error("Exception", throwable);
                        //throw new RuntimeException("");
                        log.error("Restarting PLN_BEN_SET_DET POD");
                        System.exit(1);
                        //log.error("Exception",throwable);
                    }
                }

                return KeyValue.pair(key, value);

            }).transform(new MetadataTransformer());
        }
        catch (Exception e) {
            e.printStackTrace();
        }


        return memberKStream;
    }


    @Bean
    public KStream<POL_INFO_KEY, POL_INFO> polInfoStream(StreamsBuilder builder) {


        KStream<POL_INFO_KEY, POL_INFO>
                memberKStream = builder.stream(topicConfig.getPolInfo());

        try {
            memberKStream.map((key, value) -> {
                final int[] retryCount = {0};
                log.info("POL_INFO Consuming key  :::" + key.toString());
              //  log.info("POL_INFO Consuming value  :::" + value.toString());

                try {
                    if (!value.getHeaders().getOperation().name().equals("REFRESH")) {
                        if (!value.getHeaders().getOperation().name().equals("DELETE")) { //INSERT, UPDATE
                            if (value.getPOLINFODATA().getROWUSERID().trim().equalsIgnoreCase("LINK/UNLINK")) {
                                if (!value.getPOLINFODATA().getROWSTSCD().trim().equalsIgnoreCase("D")
                                        && !value.getPOLINFODATA().getROWSTSCD().trim().equalsIgnoreCase("A")) {
                                    ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "POL_INFO", false);
                                }
                                log.info("POL_INFO LINK/UNLINK case key  :::" + key.toString());
                            } else { // NOT LINK/UNLINK cases
                                ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "POL_INFO", false);
                                log.info("POL_INFO written key  :::" + key.toString());
                            }
                        } else { //PHYSICAL DELETE
                            ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "POL_INFO", true);
                            log.info("POL_INFO physical delete key  :::" + key.toString());
                        }
                    }
                }      catch (Exception e) {
                    try {
                        retryTemplate().execute(new RetryCallback<KeyValue<POL_INFO_KEY, POL_INFO>, Exception>() {
                            @Override
                            public KeyValue<POL_INFO_KEY, POL_INFO> doWithRetry(RetryContext context) throws Exception {
                                retryCount[0]++;
                                log.info("************************************ RETRY COUNT : " + retryCount[0]+ ":::::::" + key.toString());
                                //throw new SQLException("");
                                if (!value.getHeaders().getOperation().name().equals("REFRESH")) {
                                    if (!value.getHeaders().getOperation().name().equals("DELETE")) { //INSERT, UPDATE
                                        if (value.getPOLINFODATA().getROWUSERID().trim().equalsIgnoreCase("LINK/UNLINK")) {
                                            if (!value.getPOLINFODATA().getROWSTSCD().trim().equalsIgnoreCase("D")
                                                    && !value.getPOLINFODATA().getROWSTSCD().trim().equalsIgnoreCase("A")) {
                                                ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "POL_INFO", false);
                                            }
                                            log.info("POL_INFO LINK/UNLINK case key  :::" + key.toString());
                                        } else { // NOT LINK/UNLINK cases
                                            ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "POL_INFO", false);
                                            log.info("POL_INFO written key  :::" + key.toString());
                                        }
                                    } else { //PHYSICAL DELETE
                                        ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "POL_INFO", true);
                                        log.info("POL_INFO physical delete key  :::" + key.toString());
                                    }
                                }

                                return KeyValue.pair(key, value);
                            }
                        });
                    } catch (Throwable throwable) {

                        log.error("SQLException in Table POL_INFO :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());

                        log.error("SQLException in Table POL_INFO :::time::::" + sdf.format(new Date()) + ":::::::" + new MetadataTransformer());

                        log.error("Exception",throwable);
                        //throw new RuntimeException("");
                        log.error("Restarting POL_INFO POD"); System.exit(1);
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

    @Bean
    public KStream<RX_BEN_SET_DET_KEY, RX_BEN_SET_DET> rxBenSetDetStream(StreamsBuilder builder) {


        KStream<RX_BEN_SET_DET_KEY, RX_BEN_SET_DET>
                memberKStream = builder.stream(topicConfig.getRxBenSetDet());

        try {
            KStream stream = memberKStream.map((key, value) -> {
                final int[] retryCount = {0};
                log.info("RX_BEN_SET_DET Consuming key  :::" + key.toString());
              //  log.info("RX_BEN_SET_DET Consuming value  :::" + value.toString());

                try {
                    if (!value.getHeaders().getOperation().name().equals("REFRESH")) {
                        if (!value.getHeaders().getOperation().name().equals("DELETE")) { //INSERT, UPDATE
                            if (value.getRXBENSETDETDATA().getROWUSERID().trim().equalsIgnoreCase("LINK/UNLINK")) {
                                if (!value.getRXBENSETDETDATA().getROWSTSCD().trim().equalsIgnoreCase("D")
                                        && !value.getRXBENSETDETDATA().getROWSTSCD().trim().equalsIgnoreCase("A")) {
                                    ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "RX_BEN_SET_DET", false);
                                }
                                log.info("RX_BEN_SET_DET LINK/UNLINK case key  :::" + key.toString());
                            } else { // NOT LINK/UNLINK cases
                                ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "RX_BEN_SET_DET", false);
                                log.info("RX_BEN_SET_DET written key  :::" + key.toString());
                            }
                        } else { //PHYSICAL DELETE
                            ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "RX_BEN_SET_DET", true);
                            log.info("RX_BEN_SET_DET physical delete key  :::" + key.toString());
                        }
                    }
                }      catch (Exception e) {
                    try {
                        retryTemplate().execute(new RetryCallback<KeyValue<RX_BEN_SET_DET_KEY, RX_BEN_SET_DET>, Exception>() {
                            @Override
                            public KeyValue<RX_BEN_SET_DET_KEY, RX_BEN_SET_DET> doWithRetry(RetryContext context) throws Exception {
                                retryCount[0]++;
                                log.info("************************************ RETRY COUNT : " + retryCount[0]+ ":::::::" + key.toString());
                                //throw new SQLException("");
                                if (!value.getHeaders().getOperation().name().equals("REFRESH")) {
                                    if (!value.getHeaders().getOperation().name().equals("DELETE")) { //INSERT, UPDATE
                                        if (value.getRXBENSETDETDATA().getROWUSERID().trim().equalsIgnoreCase("LINK/UNLINK")) {
                                            if (!value.getRXBENSETDETDATA().getROWSTSCD().trim().equalsIgnoreCase("D")
                                                    && !value.getRXBENSETDETDATA().getROWSTSCD().trim().equalsIgnoreCase("A")) {
                                                ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "RX_BEN_SET_DET", false);
                                            }
                                            log.info("RX_BEN_SET_DET LINK/UNLINK case key  :::" + key.toString());
                                        } else { // NOT LINK/UNLINK cases
                                            ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "RX_BEN_SET_DET", false);
                                            log.info("RX_BEN_SET_DET written key  :::" + key.toString());
                                        }
                                    } else { //PHYSICAL DELETE
                                        ingestionLogDAO.ingestDataToIngestionLog(value.toString(), "RX_BEN_SET_DET", true);
                                        log.info("RX_BEN_SET_DET physical delete key  :::" + key.toString());
                                    }
                                }
                                return KeyValue.pair(key, value);
                            }
                        });
                    } catch (Throwable throwable) {

                        log.error("SQLException in Table RX_BEN_SET_DET :time::::" + sdf.format(new Date()) + ":::::::" + key.toString());

                        log.error("SQLException in Table RX_BEN_SET_DET :::time::::" + sdf.format(new Date()) + ":::::::" + new MetadataTransformer());

                        log.error("Exception",throwable);
                        //throw new RuntimeException("");
                        log.error("Restarting RX_BEN_SET_DET POD"); System.exit(1);
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

}


