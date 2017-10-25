package io.wizzie.reputation.otx;

import io.wizzie.bootstrapper.builder.Config;
import io.wizzie.bootstrapper.builder.Listener;
import io.wizzie.bootstrapper.builder.SourceSystem;
import io.wizzie.reputation.otx.managers.HttpManager;
import io.wizzie.reputation.otx.managers.KafkaManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class UpdaterService implements Listener {
    private final Integer MAX_STORED_REVISION = 200;
    private static final Logger log = LoggerFactory.getLogger(UpdaterService.class);

    Integer myRev;
    HttpManager httpManager;
    KafkaManager kafkaManager;

    public UpdaterService(Config config) {
        httpManager = new HttpManager();

        if(config != null) {
            kafkaManager = new KafkaManager(config);
        }
    }

    @Override
    public void updateConfig(SourceSystem sourceSystem, String revision) {
        myRev = Integer.parseInt(revision);
    }

    public void update() {
        Integer currentRev = httpManager.currentRevision();
        boolean fullData;

        log.info("MyRev: " + myRev + " CurrentRev: " + currentRev);
        if (myRev != null) {
            fullData = false;

            if (currentRev.equals(myRev)) {
                log.info("Incremental update [{}] -->", myRev - 1);
                BufferedReader revData = httpManager.revData(myRev - 1);
                kafkaManager.incrementalForYou(prepareData(revData));
            } else {
                if(myRev < currentRev - MAX_STORED_REVISION) {
                    fullData = true;
                } else {
                    for (Integer iterRev = myRev; iterRev <= currentRev - 1; iterRev++) {
                        log.info("Incremental update [{}] -->", iterRev);
                        BufferedReader revData = httpManager.revData(iterRev);
                        kafkaManager.incrementalForYou(prepareData(revData));
                    }
                }
                kafkaManager.updateRevision(currentRev);
            }
        } else {
            fullData = true;
        }

        if (fullData) {
            log.info("Full update -->");
            kafkaManager.resetAll();
            BufferedReader data = httpManager.allData();
            kafkaManager.allForYou(prepareData(data));
            kafkaManager.updateRevision(currentRev);
        }

        log.info("\n");
    }

    public List<String[]> prepareData(BufferedReader data) {
        log.info("Preparing data ...");
        List<String[]> csvAll;
        try {
            String aline;
            List<String> toParse = new ArrayList<>();

            while ((aline = data.readLine()) != null) {
                toParse.add(aline);
            }

            csvAll = csvAll(toParse);

        } catch (IOException ex) {
            ex.printStackTrace();
            csvAll = null;
        }

        try {
            data.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        log.info("Prepared data!");

        return csvAll;
    }

    private List<String[]> csvAll(List<String> toParse) throws IOException {
        List<String[]> csvAll = new ArrayList<String[]>();
        for (String aline : toParse) {
            String[] csv = aline.split("#");
            csvAll.add(csv);
        }

        return csvAll;
    }

    public void stop(){
        kafkaManager.stop();
    }
}
