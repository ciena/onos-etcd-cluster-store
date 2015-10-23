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

public class ClusterMetadataReadFailureException extends ClusterMetadataException {

    private static final long serialVersionUID = -5743193140032227371L;

    private static final String MESSAGE = "unable to read the cluster metadata for '%s': detail '%s'";

    public ClusterMetadataReadFailureException(Type type, String data, Throwable cause) {
        super(String.format(MESSAGE, typeToString(type), data), cause);
    }

    public ClusterMetadataReadFailureException(Type type, String data) {
        super(String.format(MESSAGE, typeToString(type), data), null);
    }
}
