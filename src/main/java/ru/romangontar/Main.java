package ru.romangontar;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

import io.slurm.clients.discounted.Discounted;
import io.slurm.clients.discountsize.DiscountSize;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class Main {
    public static void main(String[] args)
    {
        Properties streamsConfiguration = new Properties();

        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "discount_size_filter");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.81.253:19092,192.168.81.253:29092,192.168.81.253:39092");
        streamsConfiguration.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://192.168.81.253:8091");
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);

        final Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url",
                "http://192.168.81.253:8091");
        final Serde<Discounted> discountedSerde = new SpecificAvroSerde<>();
        discountedSerde.configure(serdeConfig, false);
        final Serde<DiscountSize> discountSizeSerde = new SpecificAvroSerde<>();
        discountSizeSerde.configure(serdeConfig, false);

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, Discounted> discountedes = builder.stream("discounted", Consumed.with(Serdes.String(), discountedSerde));
        KStream<String, DiscountSize> discountSizes = discountedes
                .mapValues(discounted -> new DiscountSize(discounted.getItemId(),((discounted.getOriginalPrice() - discounted.getDiscountedPrice()) * 100) / discounted.getOriginalPrice()));
        discountSizes.to("discount_size");
        KStream<String, DiscountSize> heavilyDiscountedItems = discountSizes
                .filter((key, discountSize) -> discountSize.getDiscountPercent() >= 30);
        heavilyDiscountedItems.to("heavily_discounted_items");

        KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}