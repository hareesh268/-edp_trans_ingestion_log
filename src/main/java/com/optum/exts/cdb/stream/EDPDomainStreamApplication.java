package com.optum.exts.cdb.stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.UnsatisfiedDependencyException;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;

import java.io.InputStream;
import java.util.Scanner;

import static org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_BUILDER_BEAN_NAME;
import static org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME;

/**
 * Created by rgupta59
 */
@SpringBootApplication(
        scanBasePackageClasses = {EDPDomainStreamApplication.class}
)

@EnableKafka

public class EDPDomainStreamApplication {


/*    public static void main(String[] args) {
        SpringApplication.run(EDPDomainStreamApplication.class, args);
    }*/


    private static final Logger log = LoggerFactory.getLogger(EDPDomainStreamApplication.class);

    static ConfigurableApplicationContext context = null;

    public static void main(String[] args) {
        //TODO to remove null initialization
        try{
            context = SpringApplication.run(EDPDomainStreamApplication.class, args);
        } catch (Exception e){
            System.out.println("Printing here for sring application stop");
            SpringApplication.exit(context, () -> 0);
            //System.exit(1);
        }

    }


 /*   @Bean
    public String ipStream() {
    InputStream ipStream = Thread.currentThread().getContextClassLoader().getResourceAsStream("primary_key_input.json");
        String jsonString = "";
        if (ipStream != null) {
            Scanner scanner = new Scanner(ipStream);

            while (scanner.hasNextLine()) {
                jsonString += scanner.nextLine();
            }


        }
        return jsonString;

    }*/


    @Bean(name = DEFAULT_STREAMS_BUILDER_BEAN_NAME)
    public StreamsBuilderFactoryBean defaultKafkaStreamsBuilder(
            @Qualifier(DEFAULT_STREAMS_CONFIG_BEAN_NAME)
                    ObjectProvider<KafkaStreamsConfiguration> streamsConfigProvider) {

        KafkaStreamsConfiguration streamsConfig = streamsConfigProvider.getIfAvailable();
        if (streamsConfig != null) {
            StreamsBuilderFactoryBean streamsBuilderFactoryBean = new StreamsBuilderFactoryBean(streamsConfig);
            /*streamsBuilderFactoryBean.setStateListener((newState, oldState) -> {
                if (newState == KafkaStreams.State.NOT_RUNNING && oldState == KafkaStreams.State.PENDING_SHUTDOWN) {
                    log.error("Kafka Stream is not running");
                }
            });*/

            Thread.UncaughtExceptionHandler uncaughtExceptionHandler = new Thread.UncaughtExceptionHandler() {
                @Override
                public void uncaughtException(Thread t, Throwable e) {
                    log.error("KafkaStreams job failed due to ", e.getCause().getCause());
                    log.error("Exiting the POD due to exception");
                    System.exit(1);
                    //SpringApplication.exit(context, () -> 1);
                }
            };
            streamsBuilderFactoryBean.setUncaughtExceptionHandler(uncaughtExceptionHandler);
            return streamsBuilderFactoryBean;
        } else {
            throw new UnsatisfiedDependencyException(KafkaStreamsDefaultConfiguration.class.getName(),
                    DEFAULT_STREAMS_BUILDER_BEAN_NAME, "streamsConfig", "There is no '" +
                    DEFAULT_STREAMS_CONFIG_BEAN_NAME + "' Properties bean in the application context.\n" +
                    "Consider declaring one or don't use @EnableKafkaStreams.");
        }
    }

}