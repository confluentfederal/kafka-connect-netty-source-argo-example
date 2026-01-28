package io.confluent.ps.connect;

import io.confluent.ps.connect.metrics.FetcherMetrics;

public abstract class AbstractFetcherWithMetrics implements Runnable {

    protected FetcherMetrics metrics;

    public void setMetrics(FetcherMetrics metrics) {
        this.metrics = metrics;
    }

}
