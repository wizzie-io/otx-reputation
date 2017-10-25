package io.wizzie.reputation.otx.metrics;

import io.wizzie.bootstrapper.builder.Config;

/**
 * Simple metric listener
 */
interface MetricListener {

    /**
     * Initialize MetricListener object
     * @param config Configuration for MetricListener
     */
    void init(Config config);

    /**
     * Allow update metric and send it
     * @param metricName The name of  metric
     * @param metricValue The value of metric
     */
    void updateMetric(String metricName, Object metricValue);

    /**
     * Close metric listener
     */
    void close();

    /**
     * Allow to get name of MetricListener object
     * @return Name of MetricListener object
     */
    String name();
}
