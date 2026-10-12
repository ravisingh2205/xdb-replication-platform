package com.xdb.replication.common.web;

/** Domain exceptions mapped to HTTP status codes by {@link GlobalExceptionHandler}. */
public final class ApiExceptions {

    private ApiExceptions() {
    }

    public static class NotFoundException extends RuntimeException {
        public NotFoundException(String message) {
            super(message);
        }
    }

    public static class ConflictException extends RuntimeException {
        public ConflictException(String message) {
            super(message);
        }
    }

    /** A downstream dependency (Kafka Connect, another service) failed. */
    public static class UpstreamException extends RuntimeException {
        public UpstreamException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
