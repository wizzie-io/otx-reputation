package io.wizzie.reputation.otx;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.wizzie.bootstrapper.builder.BootstrapperBuilder;
import io.wizzie.bootstrapper.builder.Config;
import io.wizzie.metrics.MetricsManager;
import io.wizzie.reputation.otx.utils.ConfigProperties;
import io.wizzie.reputation.otx.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class OtxService {
    private static final Logger log = LoggerFactory.getLogger(OtxService.class);
    static AtomicBoolean running;

    public static void main(String[] args) throws IOException {
        if (args.length != 0) {

            if (!args[0].equals("generate")) {
                Config config = new Config(args[0]);

                UpdaterService updaterService = new UpdaterService(config.clone());
                MetricsManager metricsManager = new MetricsManager(config.clone().getMapConf());
                metricsManager.start();

                BootstrapperBuilder
                        .makeBuilder()
                        .boostrapperClass("io.wizzie.bootstrapper.bootstrappers.impl.KafkaBootstrapper")
                        .withConfigInstance(config)
                        .listener(updaterService)
                        .build();

                Long sleepInterval = Utils.toLong(config.getOrDefault(ConfigProperties.INTERVAL_MS, 60000L));

                Runtime.getRuntime().addShutdownHook(new Thread() {
                    public void run() {
                        log.info("Exiting...");
                        running.set(false);
                        updaterService.stop();
                        metricsManager.interrupt();
                    }
                });

                running = new AtomicBoolean(true);
                while (running.get()) {
                    updaterService.update();

                    log.info("Next update at: " + new Date(System.currentTimeMillis() + sleepInterval));
                    try {
                        Thread.sleep(sleepInterval);
                    } catch (InterruptedException e) {
                        log.error(e.getMessage(), e);
                    }
                }
            } else {
                UpdaterService updaterService = new UpdaterService(null);

                List<String[]> data = updaterService.prepareData(updaterService.httpManager.allData());
                List<Map<String, Object>> toFile = new ArrayList<>();

                for (int i = 1; i < data.size(); i++) {

                    String[] nextLine = data.get(i);

                    Map<String, Object> map = new HashMap<>();

                    Integer score = Integer.valueOf(nextLine[1]) * Integer.valueOf(nextLine[2]) * 2;

                    map.put("darklist_score", score);
                    map.put("darklist_score_name", Utils.giveMeScore(score));
                    map.put("darklist_category", nextLine[3]);

                    Map<String, Object> pair = new HashMap<>();
                    pair.put("ip", nextLine[0]);
                    pair.put("enrich_with", map);
                    toFile.add(pair);
                }

                ObjectMapper mapper = new ObjectMapper();

                try {
                    PrintWriter writer = new PrintWriter(args[1], "UTF-8");

                    writer.print(mapper.writeValueAsString(toFile));
                    writer.flush();
                    writer.close();

                    log.info("You json darklist is on: " + args[1]);
                } catch (FileNotFoundException | JsonProcessingException | UnsupportedEncodingException e) {
                    log.error(e.getMessage(), e);
                }
            }
        } else {
            log.info("1. Usage: io.wizzie.reputation.otx.OtxService <config_file>");
            log.info("2. Usage: io.wizzie.reputation.otx.OtxService generate <output_file>");
        }
    }
}
