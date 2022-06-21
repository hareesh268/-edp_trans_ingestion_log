package com.optum.exts.cdb.stream.utility;


import com.optum.attunity.cdb.model.CNSM_SLRY_BAS_DED_OOP;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.specific.SpecificRecordBase;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Configuration;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.sql.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.text.DateFormat;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.TimeZone;

/**
 * Created by rgupta59
 */
@Configuration
public class ConsumerUtil {


    private static final org.slf4j.Logger log = LoggerFactory.getLogger(ConsumerUtil.class);
    public Date dateParser(String strDate) {
        boolean isEmpty = strDate == null || strDate.trim().length() == 0;
        if (isEmpty) {
            // handle the validation
            return new Date (getSysTimeStamp().getTime());
        }
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        //String dateInString = "7-Jun-2013";

        try {
            long strDateInt = Long.parseLong(strDate);
            //long presentDate = strDateInt*(24*60*60*1000);
            // Date date2 = new Date(formatter.parse(strDate).getTime());
            //Date date = formatter.parse(Integer.valueOf(strDate));
            //Date parsedDate = new Date(formatter.parse(strDate));
            //Date date = new Date(presentDate);
            //return date;
            return convertLongToDate(strDateInt*24*60*60l);

        }  catch (Exception e) {
            log.error("dateParser Exception");
            e.printStackTrace();
        }
        return null;

    }

    public static Date convertLongToDate(long longDate){
        LocalDateTime datetime = LocalDateTime.ofInstant(Instant.ofEpochSecond(longDate), ZoneOffset.UTC);
        return Date.valueOf(DateTimeFormatter.ofPattern("yyyy-MM-dd").format(datetime));
    }

    public Timestamp timeStampParser(String strtTimestmp) {
        boolean isEmpty = strtTimestmp == null || strtTimestmp.trim().length() == 0;
        if (isEmpty) {
            // handle the validation
            return getSysTimeStamp();
        }
        try {
            long strDateLong = Long.parseLong(strtTimestmp);
            Date date = new Date(strDateLong);
            Timestamp timestamp = new java.sql.Timestamp(strDateLong/1000);
            return timestamp;

        }  catch (Exception e) {
            log.error("dateParser Exception");
            e.printStackTrace();
        }
        return null;

    }

    public static Timestamp timeStampParser(Long timestampInLong) {
        if ( timestampInLong != null ) {
            int nanos= (int)((timestampInLong%1000000)*1000);
            Instant instant = Instant.ofEpochMilli(timestampInLong/1000);
            LocalDateTime ldt = instant.atZone(ZoneId.of("Etc/UTC")).toLocalDateTime();
            ZonedDateTime zdt = ZonedDateTime.of(ldt.getYear(),ldt.getMonth().getValue(),ldt.getDayOfMonth(),ldt.getHour(),ldt.getMinute(),ldt.getSecond(),nanos, ZoneId.of("Etc/UTC"));
            String date = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS").format(zdt);
            return java.sql.Timestamp.valueOf(date);
            /*Date epochDate = new Date(timestampInLong /1000);
            DateFormat genericFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSS");
            //genericFormat.setTimeZone(TimeZone.getTimeZone("CST"));
            genericFormat.setTimeZone(TimeZone.getTimeZone("Etc/UTC"));
            String formatted = genericFormat.format(epochDate);
            Timestamp reqTimeStamp = Timestamp.valueOf(formatted);
            return reqTimeStamp;*/
        } else {
            return null;
        }
    }



    public BigDecimal convertByteToDecimal(SpecificRecordBase value, int fieldPosition, int precision){
        Conversions.DecimalConversion conversion = new Conversions.DecimalConversion();
        ByteBuffer fieldValue = ((ByteBuffer)((SpecificRecordBase)value.get(0)).get(fieldPosition)).duplicate();
        //Using duplicate() to fetch another address to the same location, otherwise it was creating a reference which was editing the actual value
        BigDecimal fieldDecimalValue =  conversion.fromBytes(fieldValue, null, LogicalTypes.decimal(precision,0));
        return fieldDecimalValue;
    }

   /* public BigDecimal parseAmount(String strtTimestmp) {
        ByteBuffer buf = (ByteBuffer) record.value().get(("Price"));
        byte[] arr = new byte[buf.remaining()];
        buf.get(arr);
        BigInteger bi = new BigInteger(1, arr);
        BigDecimal bd = new BigDecimal(bi).movePointLeft(4);
        System.out.println(bd);
    }*/

    /*public Timestamp timeStampParser(String strtTimestmp) {
        boolean isEmpty = strtTimestmp == null || strtTimestmp.trim().length() == 0;
        if (isEmpty) {
            // handle the validation
            return getSysTimeStamp();
        }
        try {
            //System.out.println(strtTimestmp);
            String[] dateTime= strtTimestmp.replace("T"," ").split("\\.");
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
            Date parsedDate = new java.sql.Date( dateFormat.parse(dateTime[0].replace("T"," ")+"."+ ((dateTime[1].length() > 3) ? dateTime[1].substring(0,3) : StringUtils.rightPad(dateTime[1],3,"0") )).getTime());
            // System.out.println(parsedDate);
            Timestamp timestamp = new java.sql.Timestamp(parsedDate.getTime());
            //System.out.println(timestamp);
            return timestamp;
        }
        catch(Exception e) { //this generic but you can control another types of exception
            log.error("timeStampParser Exception");
            e.printStackTrace();
        }
        return null;
    }*/


    public Timestamp getSysTimeStamp() {
        try {
           /* java.util.Date date = new java.util.Date(); // this object contains the current date value
            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
            Timestamp timestamp = new java.sql.Timestamp(date.getTime());*/

            DateFormat genericFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSsss");
            genericFormat.setTimeZone(TimeZone.getTimeZone("CST"));
            String formatted = genericFormat.format(new java.sql.Date(new java.util.Date().getTime()));
            Timestamp reqTimeStamp = Timestamp.valueOf(formatted);
            return reqTimeStamp;
        } catch(Exception e) { //this generic but you can control another types of exception
            log.error("sysTimeStamp Exception");
            e.printStackTrace();
        }
        return null;

    }



    /**
     * This method gets the date format to CST
     * Accepted date format would be yyyy-MM-dd'T'HH:mm:ss.SSS
     * @param dateStr
     * @return
     */
    public Timestamp getTimeInCST(String dateStr) throws ParseException {

        final String DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS";

        LocalDateTime localDateTime = LocalDateTime.parse(dateStr, DateTimeFormatter.ofPattern(DATE_FORMAT));

        ZoneId utcZoneId = ZoneId.of("UTC");
        ZonedDateTime utcZonedDateTime = localDateTime.atZone(utcZoneId);

        ZoneId chicagoZoneId = ZoneId.of("America/Chicago");
        ZonedDateTime cstDateTime = utcZonedDateTime.withZoneSameInstant(chicagoZoneId);

        DateTimeFormatter format = DateTimeFormatter.ofPattern(DATE_FORMAT);
       // log.info("Date (UTC) : " + format.format(utcZonedDateTime));
       // log.info("Date (CST) : " + format.format(cstDateTime));

        return Timestamp.valueOf(cstDateTime.toLocalDateTime());
    }


    public String removeAsciiNull(String data) {

        boolean isEmpty = data == null || data.trim().length() == 0;
        if (isEmpty) {
            // handle the validation
            return " ";
        }
        else
            return data.replaceAll("\\\\u0000", "").trim();

    }

}
