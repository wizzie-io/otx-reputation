package io.wizzie.reputation.otx.managers;

import io.wizzie.reputation.otx.utils.http.HttpURLs;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

public class HttpManager {
    private static final Logger log = LoggerFactory.getLogger(HttpManager.class);

    public Integer currentRevision() {
        Integer rev = 0;

        try {
            HttpClient httpclient = HttpClients.createDefault();
            HttpGet httpGet = new HttpGet(HttpURLs.URL + HttpURLs.CURRENT_REVISION);

            HttpResponse response = httpclient.execute(httpGet);
            if (response.getStatusLine().getStatusCode() == 200) {

                HttpEntity entity = response.getEntity();

                InputStream instream = entity.getContent();

                String theString = IOUtils.toString(instream).trim();

                rev = Integer.valueOf(theString.trim());
            }
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }

        return rev;
    }

    public BufferedReader allData() {
        BufferedReader buffer = null;

        try {
            HttpClient httpclient = HttpClients.createDefault();
            HttpGet httpGet = new HttpGet(HttpURLs.URL + HttpURLs.ALL_DATA);

            log.info("Downloading all list ...");
            HttpResponse response = httpclient.execute(httpGet);
            buffer = processResponse(response);

            if (response.getStatusLine().getStatusCode() == 200) {
                log.info("Downloaded all list! Status Code: " + response.getStatusLine().getStatusCode());
            } else {
                log.warn("Fail downloading all list. Status Code: {}, Msg: {}",
                        response.getStatusLine().getStatusCode(),
                        response.getStatusLine().getReasonPhrase());
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        return buffer;
    }

    public BufferedReader revData(Integer rev) {
        BufferedReader buffer = null;

        try {
            HttpClient httpclient = HttpClients.createDefault();
            HttpGet httpGet = new HttpGet(HttpURLs.URL + HttpURLs.ONE_REVISION + rev);

            log.info("Downloading incremental list ...");

            HttpResponse response = httpclient.execute(httpGet);
            buffer = processResponse(response);

            log.info("Downloaded incremental list! Status Code: " + response.getStatusLine().getStatusCode());

        } catch (IOException e) {
            e.printStackTrace();
        }

        return buffer;
    }

    private BufferedReader processResponse(HttpResponse response) throws IOException {
        if (response.getStatusLine().getStatusCode() == 200) {
            HttpEntity entity = response.getEntity();

            InputStream instream = entity.getContent();

            Reader readerCsv = new InputStreamReader(instream);
            return new BufferedReader(readerCsv);
        } else {
            Reader readerCsv = new StringReader("");
            return new BufferedReader(readerCsv);
        }
    }
}
