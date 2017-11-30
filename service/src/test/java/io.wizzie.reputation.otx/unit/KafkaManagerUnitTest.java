package io.wizzie.reputation.otx.unit;

import io.wizzie.bootstrapper.builder.Config;

import io.wizzie.reputation.otx.UpdaterService;
import io.wizzie.reputation.otx.managers.KafkaManager;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.junit.*;

import java.io.*;
import java.util.*;

import static org.junit.Assert.*;


public class KafkaManagerUnitTest {
    private final static int NUM_BROKERS = 1;

    @ClassRule
    public static EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);

    static Config config;


    @BeforeClass
    public static void initTest() throws IOException, InterruptedException {
        config = new Config(Thread.currentThread().getContextClassLoader().getResource("test-config.json").getPath());
        config.put("bootstrap.servers",CLUSTER.bootstrapServers());
    }

    @Test
    public void updateRevisionUnitTest() throws InterruptedException {

        Properties consumerConfigA = new Properties();
        consumerConfigA.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        consumerConfigA.put(ConsumerConfig.GROUP_ID_CONFIG, "test-otx");
        consumerConfigA.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfigA.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        KafkaManager kafkaManager = new KafkaManager(config.clone());
        kafkaManager.updateRevision(2);
        List<KeyValue<String, String>> receivedMessagesFromOutput1 = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfigA, (String)((ArrayList)config.get("bootstrap.kafka.topics")).get(0), 1);
        assertNotNull(receivedMessagesFromOutput1);
        assertEquals(new KeyValue<>("reputation-service","2"),receivedMessagesFromOutput1.get(0));
    }

    @Test
    public void allForYouUnitTest() throws InterruptedException {

        Properties consumerConfigA = new Properties();
        consumerConfigA.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        consumerConfigA.put(ConsumerConfig.GROUP_ID_CONFIG, "test-otx");
        consumerConfigA.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfigA.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        KafkaManager kafkaManager = new KafkaManager(config.clone());

        String inputString = "103.211.40.253#4#2#Malicious Host#IN#Bhiwandi#19.2999992371,73.0667037964#3";
        InputStream input = new ByteArrayInputStream(inputString.getBytes());
        BufferedReader buffer = new BufferedReader(new InputStreamReader(input));
        UpdaterService updaterService = new UpdaterService(config.clone());
        List<String[]> preparedData = updaterService.prepareData(buffer);

        kafkaManager.allForYou(preparedData);
        List<KeyValue<String,String>> receivedMessagesFromOutput1 = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfigA, config.get("reputation.topic"), 1);
        assertNotNull(receivedMessagesFromOutput1);
        String expectedString = "{\"otx_score_name\":\"very low\",\"otx_category\":\"Malicious Host\",\"otx_score\":16}";
        assertEquals(new KeyValue<>("103.211.40.253", expectedString),receivedMessagesFromOutput1.get(0));
    }


    @Test
    public void incrementalForYouUnitTest() throws InterruptedException {

        Properties consumerConfigA = new Properties();
        consumerConfigA.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        consumerConfigA.put(ConsumerConfig.GROUP_ID_CONFIG, "test-otx");
        consumerConfigA.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfigA.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        KafkaManager kafkaManager = new KafkaManager(config.clone());

        String inputString = "+103.211.40.253#4#2#Malicious Host#IN#Bhiwandi#19.2999992371,73.0667037964#3";
        InputStream input = new ByteArrayInputStream(inputString.getBytes());
        BufferedReader buffer = new BufferedReader(new InputStreamReader(input));
        UpdaterService updaterService = new UpdaterService(config.clone());
        List<String[]> preparedData = updaterService.prepareData(buffer);

        kafkaManager.incrementalForYou(preparedData);
        List<KeyValue<String,String>> receivedMessagesFromOutput1 = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfigA, config.get("reputation.topic"), 1);
        assertNotNull(receivedMessagesFromOutput1);
        String expectedString = "{\"otx_score_name\":\"very low\",\"otx_category\":\"Malicious Host\",\"otx_score\":16}";
        assertEquals(new KeyValue<>("103.211.40.253", expectedString),receivedMessagesFromOutput1.get(0));
    }

    @Test
    public void resetAllUnitTest() throws InterruptedException {

        Properties consumerConfigA = new Properties();
        consumerConfigA.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        consumerConfigA.put(ConsumerConfig.GROUP_ID_CONFIG, "test-otx");
        consumerConfigA.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfigA.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        KafkaManager kafkaManager = new KafkaManager(config.clone());

        String inputString = "+103.211.40.253#4#2#Malicious Host#IN#Bhiwandi#19.2999992371,73.0667037964#3";
        InputStream input = new ByteArrayInputStream(inputString.getBytes());
        BufferedReader buffer = new BufferedReader(new InputStreamReader(input));
        UpdaterService updaterService = new UpdaterService(config.clone());
        List<String[]> preparedData = updaterService.prepareData(buffer);

        kafkaManager.incrementalForYou(preparedData);
        List<KeyValue<String,String>> receivedMessagesFromOutput1 = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfigA, config.get("reputation.topic"), 1);
        assertNotNull(receivedMessagesFromOutput1);
        kafkaManager.resetAll();
        List<KeyValue<String,String>> receivedMessagesFromOutput2 = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfigA, config.get("reputation.topic"), 1);
        assertEquals(new KeyValue<>("103.211.40.253", null),receivedMessagesFromOutput2.get(0));
    }

    @AfterClass
    public static void stop(){
    }
}
