package io.confluent.ps.connect.time;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import lombok.Data;

public class DurationProducer {

    private ZonedDateTime startTime;
    private long fetchDuration;

    private boolean running = false;

    private Lock lock = new ReentrantLock();

    private ArrayList<ZonedDuration> returnedDurations = new ArrayList<>();

    public DurationProducer(ZonedDateTime startTime, long fetchDuration) {
        this.startTime = startTime;
        this.fetchDuration = fetchDuration;
        this.returnedDurations = new ArrayList<>();
    }

    public void start() {
        this.lock.lock();
        try {
            this.running = true;
        } finally {
            this.lock.unlock();
        }
    }

    public void stop() {
        this.lock.lock();
        try {
            this.running = false;
        } finally {
            this.lock.unlock();
        }
    }

    public boolean isRunning() {
        this.lock.lock();
        try {
            return this.running;
        } finally {
            this.lock.unlock();
        }
    }

    /**
     * Get the next duration based on the current start time and fetch duration. If the producer is not running, it returns null.
     *
     * @return
     */
    public ZonedDuration getNextDuration() {
        this.lock.lock();
        try {
            if (!this.running) {
                return null; // Not running, cannot produce duration
            }

            if (! this.returnedDurations.isEmpty()) {
                // Prevent unbounded growth of returned durations list
                return this.returnedDurations.remove(0);
            }

            ZonedDateTime endTime = startTime.plusSeconds(fetchDuration);
            ZonedDuration duration = new ZonedDuration();
            duration.setStartTime(startTime);
            duration.setEndTime(endTime);
            startTime = endTime; // Update start time for the next call
            return duration;
        } finally {
            this.lock.unlock();
        }
    }

    /**
     * Return a duration back to the producer. This is useful if a fetcher encounters an error and needs to retry the same duration.
     *
     * @param duration
     */
    public void returnDuration(ZonedDuration duration) {
        this.lock.lock();
        try {
            this.returnedDurations.add(duration);
        } finally {
            this.lock.unlock();
        }
    }

    @Data
    public static class ZonedDuration {
        private ZonedDateTime startTime;
        private ZonedDateTime endTime;
    }

}
