package io.wizzie.reputation.otx.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Utils {
    private static final Logger log = LoggerFactory.getLogger(Utils.class);

    public static String giveMeScore(Integer score) {
        String score_name = "";
        if (100 >= score && score > 95) {
            score_name = "very high";
        } else if (95 >= score && score > 85) {
            score_name = "high";
        } else if (85 >= score && score > 70) {
            score_name = "medium";
        } else if (70 >= score && score > 50) {
            score_name = "low";
        } else if (50 >= score && score >= 0) {
            score_name = "very low";
        }
        return score_name;
    }

    public static Long toLong(Object l) {
        Long result = null;

        try {
            if (l != null) {
                if (l instanceof Integer) {
                    result = ((Integer) l).longValue();
                } else if (l instanceof Long) {
                    result = (Long) l;
                } else if (l instanceof String) {
                    result = Long.valueOf((String) l);
                }
            }
        } catch (NumberFormatException ex) {
            log.error(ex.getMessage(), ex);
        }

        return result;
    }
}
