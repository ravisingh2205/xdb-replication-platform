package com.xdb.replication.publication.connect;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;

/** Mirror of Kafka Connect's {@code GET /connectors/{name}/status} response. */
@JsonIgnoreProperties(ignoreUnknown = true)
public record ConnectorStatus(String name, State connector, List<Task> tasks) {

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record State(String state, String worker_id, String trace) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Task(int id, String state, String worker_id, String trace) {
    }

    public boolean isRunning() {
        return connector != null && "RUNNING".equals(connector.state())
                && tasks != null && !tasks.isEmpty()
                && tasks.stream().allMatch(t -> "RUNNING".equals(t.state()));
    }

    public boolean isPaused() {
        return connector != null && "PAUSED".equals(connector.state());
    }

    public boolean hasFailure() {
        return (connector != null && "FAILED".equals(connector.state()))
                || (tasks != null && tasks.stream().anyMatch(t -> "FAILED".equals(t.state())));
    }

    /** First line of the first failure trace, for the UI. */
    public String failureSummary() {
        String trace = connector != null && connector.trace() != null ? connector.trace()
                : tasks == null ? null : tasks.stream().map(Task::trace).filter(t -> t != null).findFirst().orElse(null);
        if (trace == null) {
            return null;
        }
        int nl = trace.indexOf('\n');
        return nl > 0 ? trace.substring(0, nl) : trace;
    }
}
