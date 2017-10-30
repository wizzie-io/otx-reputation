package io.wizzie.reputation.otx.unit;

import io.wizzie.bootstrapper.builder.Config;

import io.wizzie.reputation.otx.UpdaterService;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.junit.*;

import static org.junit.Assert.*;

import java.io.*;
import java.util.List;
import java.util.Map;
import java.util.Properties;


public class UpdaterServiceUnitTest {
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
    public void updateUnitTest() throws InterruptedException {

        Properties consumerConfigA = new Properties();
        consumerConfigA.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        consumerConfigA.put(ConsumerConfig.GROUP_ID_CONFIG, "test-otx");
        consumerConfigA.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfigA.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);

        UpdaterService updaterService = new UpdaterService(config.clone());
        updaterService.update();
        List<KeyValue<String, Map>> receivedMessagesFromOutput1 = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfigA, config.get("reputation.topic"), 1);
        assertNotNull(receivedMessagesFromOutput1);
    }

    @Test
    public void prepareDataUnitTest() throws InterruptedException {

        String inputString = "103.211.40.253#4#2#Malicious Host#IN#Bhiwandi#19.2999992371,73.0667037964#3";
        InputStream input = new ByteArrayInputStream(inputString.getBytes());
        BufferedReader buffer = new BufferedReader(new InputStreamReader(input));
        UpdaterService updaterService = new UpdaterService(config.clone());
        List<String[]> preparedData = updaterService.prepareData(buffer);

        assertEquals("103.211.40.253 4 2 Malicious Host IN Bhiwandi 19.2999992371,73.0667037964 3",
                preparedData.get(0)[0] + " " + preparedData.get(0)[1] + " " + preparedData.get(0)[2] + " "
                + preparedData.get(0)[3] + " " + preparedData.get(0)[4] + " " + preparedData.get(0)[5] + " " +
                preparedData.get(0)[6] + " " + preparedData.get(0)[7]);
    }

    @AfterClass
    public static void stop(){
        CLUSTER.stop();
    }
}
