package com.optum.exts.cdb.stream.transformer;

import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
Added this class for logging the Queue metadata and corresponding Record Big key Information.
This could help to prove if there is latency between the data ingestion.

@Author: Aditya Molugu.
 */
public class MetadataTransformer implements TransformerSupplier<SpecificRecordBase, SpecificRecordBase,
        KeyValue<SpecificRecordBase, SpecificRecordBase>> {

    private static final Logger log = LoggerFactory.getLogger(MetadataTransformer.class);


    @Override
    public Transformer<SpecificRecordBase, SpecificRecordBase, KeyValue<SpecificRecordBase, SpecificRecordBase>> get() {
        return new Transformer<SpecificRecordBase, SpecificRecordBase,
                KeyValue<SpecificRecordBase, SpecificRecordBase>>() {

            ProcessorContext processorContext;

            @Override
            public void init(ProcessorContext context) {
                this.processorContext = context;
            }

            @Override
            public KeyValue<SpecificRecordBase, SpecificRecordBase> transform(SpecificRecordBase key,
                                                                              SpecificRecordBase value) {
                if(key != null) {
                    log.info("Displaying Queue Metadata and Record information::"
                            + " PartitionId:: " + processorContext.partition()
                            + " OffsetId:: " + processorContext.offset()
                            + " Topic:: " + processorContext.topic()
                            + key.toString());

                    return new KeyValue<>(key, value);
                } else {
                    log.info("key is be null");
                    return new KeyValue<>(key, value);
                }

            }

            @Override
            public void close() {

            }
        };
    }
}
