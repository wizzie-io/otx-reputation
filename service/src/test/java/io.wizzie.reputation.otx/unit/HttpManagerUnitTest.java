package io.wizzie.reputation.otx.unit;

import io.wizzie.bootstrapper.builder.Config;

import io.wizzie.reputation.otx.managers.HttpManager;

import org.junit.*;

import java.io.BufferedReader;
import java.io.IOException;

import static org.junit.Assert.*;


public class HttpManagerUnitTest {

    static Config config;

    @BeforeClass
    public static void initTest() throws IOException, InterruptedException {
        config = new Config(Thread.currentThread().getContextClassLoader().getResource("test-config.json").getPath());
        config.put("bootstrap.servers","unused");
    }

    @Test
    public void currentRevUnitTest() {
        HttpManager httpManager = new HttpManager();
        Integer currentRev = httpManager.currentRevision();
        Assert.assertNotNull(currentRev);
    }

    @Test
    public void allDataUnitTest() {
        HttpManager httpManager = new HttpManager();
        BufferedReader buffer = httpManager.allData();
        assertNotNull(buffer);
        assertTrue(buffer.lines().count() > 0);
    }

    @Test
    public void revDataUnitTest() {
        HttpManager httpManager = new HttpManager();
        Integer currentRev = httpManager.currentRevision();
        assertNotNull(currentRev);
    }

    @AfterClass
    public static void stop(){
    }
}
