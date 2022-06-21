package com.optum.exts.cdb.stream.utility;

import com.optum.attunity.cdb.model.*;
import org.junit.Test;

public class AvroComparisonTest {

    public void setUp(){

    }

    /*@Test
    public void lCnsmSrchTest(){

        L_CNSM_SRCH l_cnsm_srch_prev = new L_CNSM_SRCH(new L_CNSM_SRCH_DATA()1, 2, "DUMMY_SRC_CD", "DUMMY_LEGACY_SRC_ID");

        L_CNSM_SRCH l_cnsm_srch_next = new L_CNSM_SRCH(1, 2, "DUMMY_SRC_FR", "DUMMY_LEGACY_SRC_QR");

        System.out.println("Difference On L_CNSM_SRCH: " + AvroComparisonLib.getDifferenceBetween(l_cnsm_srch_prev, l_cnsm_srch_next));


    }*/

    /*@Test
    public void lCovPrdtDtTest(){

        L_COV_PRDT_DT l_cov_prdt_dt_prev = new L_COV_PRDT_DT( 1, 2, "SOME_SRCID", "SOME_LEGACY_POL_NBR", "SOME_LEGACY_SRC_ID", "SOME_COV_TYPE_CD", "SOME_COV_EFF_DATE");

        L_COV_PRDT_DT l_cov_prdt_dt_next = new L_COV_PRDT_DT( 1, 2, "RX", "UHC-1234", "UHC-RX", "AMBULANCE", "1/1/2020");

        System.out.println("Difference On L_COV_PRDT_DT: " + AvroComparisonLib.getDifferenceBetween(l_cov_prdt_dt_prev, l_cov_prdt_dt_next));


    }*/

    /*@Test
    public void lHltSrvDtTest(){

        L_HLT_SRV_DT l_hlt_srv_dt = new L_HLT_SRV_DT(1, 2, "MED", "MED-1234", "MEDICAL", "MEDICAL-PRODUCT-1234", "MP-1234", "2/20/2021");

        L_HLT_SRV_DT l_hlt_srv_dt_change = new L_HLT_SRV_DT(1, 2, "PH", "MED-1234", "PHYSICIAN", "MEDICAL-PRODUCT-1234", "MP-1234", "5/20/2021");

        System.out.println("Difference On L_HLT_SRV_DT: " + AvroComparisonLib.getDifferenceBetween(l_hlt_srv_dt, l_hlt_srv_dt_change));


    }*/

    /*@Test
    public void cnsmStsTest(){

        CNSM_STS cnsm_sts = new CNSM_STS(2, 3, "CNSM1234", "CNSM", "CNSM-LGCY", "12/20/1991");

        CNSM_STS cnsm_sts_latest = new CNSM_STS(2, 3, "CNSM1234", "CNSM", "CNSM-LGCY", "10/20/1991");

        System.out.println("Difference On CNSM_STS: " + AvroComparisonLib.getDifferenceBetween(cnsm_sts, cnsm_sts_latest));


    }*/

    /*@Test
    public void cnsmMdcrEnrlTest(){

        CNSM_MDCR_ENRL cnsm_mdcr_enrl = new CNSM_MDCR_ENRL(2, 3, "CNSM1234", "CNSM", "MDCR-LGCY", "10/20/1991");

        CNSM_MDCR_ENRL cnsm_mdcr_enrl_latest = new CNSM_MDCR_ENRL(2, 3, "CNSM1234", "CNSM", "MDCR", "10/20/1991");

        System.out.println("Difference On CNSM_MDCR_ENRL: " + AvroComparisonLib.getDifferenceBetween(cnsm_mdcr_enrl, cnsm_mdcr_enrl_latest));


    }*/

    public void tearDown(){

    }


}
