package io.wizzie.reputation.otx.managers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.wizzie.bootstrapper.bootstrappers.impl.KafkaBootstrapper;
import io.wizzie.bootstrapper.builder.Config;
import io.wizzie.reputation.otx.utils.ConfigProperties;
import io.wizzie.reputation.otx.utils.Utils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class KafkaManager {
    private static final Logger log = LoggerFactory.getLogger(KafkaManager.class);

    KafkaProducer<String, String> producer;
    KafkaConsumer<String, String> consumer;

    List<String> bootstrapTopics;
    String reputationTopic;
    ObjectMapper mapper;
    String appId;

    public KafkaManager(Config config) {
        bootstrapTopics = config.get(KafkaBootstrapper.BOOTSTRAP_TOPICS_CONFIG);
        reputationTopic = config.get(ConfigProperties.REPUTATION_TOPIC);
        appId = config.getOrDefault(ConfigProperties.APPLICATION_ID, "reputation-service");

        mapper = new ObjectMapper();

        Properties producerProperties = config.getProperties();
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        Properties consumerProperties = config.getProperties();
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, String.format("%s-%s", appId, "otx"));


        producer = new KafkaProducer<>(producerProperties);
        consumer = new KafkaConsumer<>(consumerProperties);
    }

    public void updateRevision(Integer revision) {
        for (String bootstrapTopic : bootstrapTopics) {
            producer.send(new ProducerRecord<>(bootstrapTopic, appId, revision.toString()));
        }
    }

    public void allForYou(List<String[]> data) {

        log.info("Processing all list ...");

        List<Map> dataToSave = new ArrayList<>();
        List<String> keysToSave = new ArrayList<>();

        for (int i = 0; i < data.size(); i++) {

            String[] nextLine = data.get(i);

            Map<String, Object> map = new HashMap<>();

            Integer score = Integer.valueOf(nextLine[1]) * Integer.valueOf(nextLine[2]) * 2;

            map.put("otx_score", score);
            map.put("otx_score_name", Utils.giveMeScore(score));
            map.put("otx_category", nextLine[3]);

            keysToSave.add(nextLine[0]);
            dataToSave.add(map);

        }

        log.info("Processed all list!");
        log.info("Saving ... ");

        for (int i = 0; i < dataToSave.size(); i++) {
            try {
                String repDataIp = mapper.writeValueAsString(dataToSave.get(i));
                producer.send(new ProducerRecord<>(reputationTopic, keysToSave.get(i), repDataIp));
            } catch (JsonProcessingException e) {
                log.error(e.getMessage(), e);
            }
        }

        log.info("Saved {} records", dataToSave.size());
    }

    public void incrementalForYou(List<String[]> data) {
        if (!data.isEmpty()) {
            log.info("Processing incremental list ...");

            List<Map> dataToSave = new ArrayList<>();
            List<String> keysToSave = new ArrayList<>();
            List<String> keysToDelete = new ArrayList<>();

            for (String[] nextLine : data) {

                if (nextLine[0].charAt(0) == '-') {
                    keysToDelete.add(nextLine[0].substring(1, nextLine[0].length()));
                } else {
                    Map<String, Object> map = new HashMap<>();

                    Integer score = Integer.valueOf(nextLine[1]) * Integer.valueOf(nextLine[2]) * 2;

                    map.put("otx_score", score);
                    map.put("otx_score_name", Utils.giveMeScore(score));
                    map.put("otx_category", nextLine[3]);

                    keysToSave.add(nextLine[0].substring(1, nextLine[0].length()));
                    dataToSave.add(map);
                }
            }

            log.info("Processed incremental list!");

            if (keysToDelete.size() > 0) {
                try {
                    log.info("Deleting ... ");
                    for (String keyToDelete : keysToDelete) {
                        producer.send(new ProducerRecord<>(reputationTopic, keyToDelete, null));
                    }
                    log.info("Deleted: " + keysToDelete.size());
                } catch (Exception ex) {
                    log.error("Can't delete!", ex);
                }
            }

            log.info("Saving ... ");

            Map<String, Map<String, Object>> mapToSave = new HashMap<String, Map<String, Object>>();
            Integer size = dataToSave.size();

            for (int i = 0; i < size; i++) {
                try {
                    String repDataIp = mapper.writeValueAsString(dataToSave.get(i));
                    producer.send(new ProducerRecord<>(reputationTopic, keysToSave.get(i), repDataIp));
                } catch (JsonProcessingException e) {
                    log.error(e.getMessage(), e);
                }
            }

            log.info("Saved: " + mapToSave.size());
        } else {
            log.info("Revision is empty!");
        }
    }

    public void resetAll() {
        log.info("Reset all data. Getting partition management ... ");
        List<PartitionInfo> partitions = consumer.partitionsFor(reputationTopic);

        List<TopicPartition> topicPartitions = partitions.stream()
                .map(partition -> new TopicPartition(reputationTopic, partition.partition()))
                .collect(Collectors.toList());

        consumer.assign(topicPartitions);
        consumer.seekToEnd(topicPartitions);

        Map<Integer, Long> maxOffsets = new HashMap<>();

        for (TopicPartition topicPartition : topicPartitions) {
            maxOffsets.put(topicPartition.partition(), consumer.position(topicPartition));
        }

        consumer.seekToBeginning(topicPartitions);

        List<String> keys = new ArrayList<>();
        Boolean shouldContinue = true;

        log.info("Starting to recovery all keys ... ");

        Map<Integer, Long> currentRecovery = new HashMap<>();
        while (shouldContinue) {
            ConsumerRecords<String, String> consumerRecords = consumer.poll(0);

            for (TopicPartition topicPartition : topicPartitions) {
                List<ConsumerRecord<String, String>> records = consumerRecords.records(topicPartition);
                for (ConsumerRecord<String, String> record : records) {
                    if (record.value() != null) {
                        keys.add(record.key());
                    }
                }

                long currentOffset = consumer.position(topicPartition);
                shouldContinue = maxOffsets.get(topicPartition.partition()) != currentOffset;
                currentRecovery.put(topicPartition.partition(), currentOffset);
            }

            StringBuilder builder = new StringBuilder();
            builder.append("Recovery Status --> ");

            for (Map.Entry<Integer, Long> entry : maxOffsets.entrySet()) {
                builder.append(entry.getKey())
                        .append(": ")
                        .append(currentRecovery.get(entry.getKey()))
                        .append("/")
                        .append(entry.getValue())
                        .append(", ");
            }

            log.info(builder.toString());
        }

        log.info("Starting to remove all keys ...");

        for (String key : keys) {
            producer.send(new ProducerRecord<>(reputationTopic, key, null));
        }

        log.info("Reset done!");
    }

    public void stop() {
        producer.close();
    }
}
