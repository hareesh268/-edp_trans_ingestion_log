package com.optum.exts.cdb.stream.utility;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecordBase;

import java.util.List;

public class AvroComparisonLib {

    public static String getDifferenceBetween(SpecificRecordBase initialRecord, SpecificRecordBase latestRecord) {

        List<Schema.Field> fieldList = getFields(initialRecord);

        AvroFieldChanges avroFieldChanges = new AvroFieldChanges();

        fieldList.forEach( item -> {

            if (!initialRecord.get(item.name()).equals(latestRecord.get(item.name()))) {
                avroFieldChanges.addChanges(item.name(), initialRecord.get(item.name()), latestRecord.get(item.name()));
            }});

        return avroFieldChanges.getChanges();
    }

    private static List<Schema.Field> getFields(SpecificRecordBase record) {

        return record.getSchema().getFields();

    }
}
