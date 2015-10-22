package org.ciena.onosproject.cluster.etcd.impl;

public class PartitionNotFoundException extends Exception {

    private static final long serialVersionUID = -6811047272128748671L;

    private static final String MESSAGE = "unable to find parition with the id '%s'";

    private final String partitionId;

    public PartitionNotFoundException(String message, String partitionId, Throwable cause) {
        super(message, cause);
        this.partitionId = partitionId;
    }

    public PartitionNotFoundException(String partitionId, Throwable cause) {
        this(String.format(MESSAGE, partitionId), partitionId, cause);
    }

    public PartitionNotFoundException(String partitionId) {
        this(String.format(MESSAGE, partitionId), partitionId, null);
    }

    /**
     * @return the partition ID associated with the exception
     */
    public String getPartitionId() {
        return this.partitionId;
    }
}
