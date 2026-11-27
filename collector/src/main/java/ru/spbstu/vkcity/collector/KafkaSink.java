package ru.spbstu.vkcity.collector;

import java.io.IOException;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Отправляет сырые объекты в kafka:
 */
class KafkaSink implements Sink {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaSink.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final KafkaProducer<String, byte[]> producer;
    private final String prefix;
    private final AtomicLong sent = new AtomicLong();
    private final AtomicLong failed = new AtomicLong();

    KafkaSink(String bootstrapServers, String topicPrefix) {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        p.put(ProducerConfig.ACKS_CONFIG, "all");
        p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        p.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        p.put(ProducerConfig.LINGER_MS_CONFIG, "50");
        p.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, String.valueOf(5 * 1024 * 1024));
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        this.producer = new KafkaProducer<>(p);
        this.prefix = topicPrefix;
    }

    private void send(String topic, String key, JsonNode node) throws IOException {
        byte[] value = MAPPER.writeValueAsBytes(node);
        producer.send(new ProducerRecord<>(prefix + topic, key, value), (meta, ex) -> {
            if (ex != null) {
                failed.incrementAndGet();
                LOG.error("Kafka: не удалось отправить {} в {}: {}", key, prefix + topic, ex.getMessage());
            } else {
                sent.incrementAndGet();
            }
        });
    }

    @Override
    public void group(JsonNode g) throws IOException {
        send("groups", g.path("id").asText(), g);
    }

    @Override
    public void post(JsonNode p) throws IOException {
        send("posts", p.path("_group_id").asText() + ":" + p.path("id").asText(), p);
    }

    @Override
    public void comment(JsonNode c) throws IOException {
        send("comments", c.path("_group_id").asText() + ":" + c.path("post_id").asText() + ":" + c.path("id").asText(), c);
    }

    long failedCount() {
        return failed.get();
    }

    @Override
    public void close() {
        producer.flush();
        producer.close(Duration.ofSeconds(30));
        LOG.info("Kafka: отправлено {}, ошибок {}", sent.get(), failed.get());
    }
}
