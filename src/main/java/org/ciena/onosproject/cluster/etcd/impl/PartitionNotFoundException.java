/*
 * Copyright 2015 Ciena Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
