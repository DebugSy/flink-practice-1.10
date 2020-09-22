package com.flink.demo.cases.case05;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.executiongraph.JobStatusListener;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmaster.JobMaster;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.metrics.groups.JobManagerJobMetricGroup;
import org.apache.flink.runtime.scheduler.SchedulerNG;
import org.apache.flink.runtime.webmonitor.WebMonitorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class FlinkJobStatusListener implements JobStatusListener {

    private static final Logger logger = LoggerFactory.getLogger(FlinkJobStatusListener.class);

    private final JobGraph jobGraph;

    private final JobManagerJobMetricGroup jobManagerJobMetricGroup;

    private final SchedulerNG schedulerNG;

    private final JobMaster jobMaster;

    private JobID jobId;

    private boolean started = false;

    public FlinkJobStatusListener(JobGraph jobGraph, JobManagerJobMetricGroup jobManagerJobMetricGroup,
                                  SchedulerNG schedulerNG, JobMaster jobMaster, ScheduledExecutor scheduledExecutor) {
        this.jobGraph = jobGraph;
        this.jobManagerJobMetricGroup = jobManagerJobMetricGroup;
        this.schedulerNG = schedulerNG;
        this.jobId = jobGraph.getJobID();
        this.jobMaster = jobMaster;
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        Thread.sleep(1000 * 5);
                        scheduledExecutor.schedule(new Runnable() {
                            @Override
                            public void run() {
                                JobStatus jobStatus = schedulerNG.requestJobStatus();
                                CompletableFuture<ArchivedExecutionGraph> archivedExecutionGraphCompletableFuture = jobMaster.requestJob(Time.seconds(1));
                                archivedExecutionGraphCompletableFuture.whenComplete((archivedExecutionGraph, throwable) -> {
                                    JobDetails jobDetails = WebMonitorUtils.createDetailsForJob(archivedExecutionGraph);
                                    logger.info("report {} jobDetails {}", jobId, jobDetails);
                                });
                                logger.info("report {} status {}", jobId, jobStatus);
                            }
                        }, 0, TimeUnit.SECONDS);
                    } catch (Throwable e) {
                        logger.error("report thread throw exception");
                    }
                }
            }
        });
        thread.setName("FlinkJobStatusListener-Daemon-Thread");
        thread.setDaemon(true);
        thread.start();

    }

    @Override
    public void jobStatusChanges(JobID jobId, JobStatus newJobStatus, long timestamp, Throwable error) {
        logger.info("flink job status info: {} {} {}", jobId, newJobStatus, timestamp);
    }
}
