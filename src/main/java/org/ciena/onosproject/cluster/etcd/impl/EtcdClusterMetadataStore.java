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

import static com.google.common.base.Preconditions.checkNotNull;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

import org.boon.core.Handler;
import org.boon.etcd.ClientBuilder;
import org.boon.etcd.Etcd;
import org.boon.etcd.Response;
import org.ciena.onosproject.cluster.etcd.impl.ClusterMetadataException.Type;
import org.onlab.packet.IpAddress;
import org.onosproject.cluster.ClusterMetadata;
import org.onosproject.cluster.ClusterMetadataEvent;
import org.onosproject.cluster.ClusterMetadataStore;
import org.onosproject.cluster.ClusterMetadataStoreDelegate;
import org.onosproject.cluster.ControllerNode;
import org.onosproject.cluster.DefaultControllerNode;
import org.onosproject.cluster.NodeId;
import org.onosproject.cluster.Partition;
import org.onosproject.store.service.Versioned;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

public class EtcdClusterMetadataStore implements ClusterMetadataStore {

    private static final Logger log = LoggerFactory.getLogger(EtcdClusterMetadataStore.class);

    private static final String CLUSTER_CONFIG_KEY = "onos/cluster/%s/config";
    private static final String CLUSTER_OPERATIONAL_KEY = "onos/cluster/%s/operational/%s";

    private static final String CONTROLLER_NODES_KEY = "nodes";
    private static final String NODE_IP_KEY = "ip";
    private static final String NODE_PORT_KEY = "port";
    private static final String CONTROLLER_PARTITIONS_KEY = "partitions";
    private static final String DEFAULT_CLUSTER_NAME = "default";
    private static final String DEFAULT_ETCD_CONNECTION = "http://127.0.0.1:4001";

    private ClusterMetadataStoreDelegate delegate = null;
    private Etcd etcd = null;
    private AtomicBoolean active = new AtomicBoolean(false);
    private final String clusterName;
    private final String configKey;
    private final String connection;

    public EtcdClusterMetadataStore(String connection, String clusterName) {
        this.clusterName = clusterName;
        this.configKey = String.format(CLUSTER_CONFIG_KEY, clusterName);

        this.connection = (connection == null ? DEFAULT_ETCD_CONNECTION : connection);
    }

    public EtcdClusterMetadataStore() {
        this(null, DEFAULT_CLUSTER_NAME);
    }

    /**
     * Utility function that will retry a command until it succeeds to a set numbers of retries has happened.
     *
     * @param name
     *            used for logging purposes
     * @param repeat
     *            the number of times to retry
     * @param sleep
     *            ms to sleep between retries
     * @param action
     *            the action {@link java.lang.Runnable} to (re)try
     * @throws Exception
     *             if the command still fails after all retries and exception is propagated
     */
    private void retry(String name, int repeat, long sleep, Runnable action) throws Exception {
        for (; repeat > 0; repeat -= 1) {
            try {
                action.run();
                return;
            } catch (Exception e) {
                if (repeat > 1) {
                    log.info("action '{}' failed, attempting retry. there are {} retries left", name, repeat - 1);
                } else {
                    log.error("action '{}' failed after all retries", name);
                    throw e;
                }
            }

            // I hate sleeps, but it seems appropriate
            Thread.sleep(sleep);
        }
    }

    /**
     * Utility function that will retry a command that returns a result, until it succeeds to a set numbers of retries
     * has happened.
     *
     * @param name
     *            used for logging purposes
     * @param repeat
     *            the number of times to retry
     * @param sleep
     *            ms to sleep between retries
     * @param action
     *            the action {@link java.lang.Callable} to (re)try
     * @throws Exception
     *             if the command still fails after all retries and exception is propagated
     */
    private <T> T retryWithResult(String name, int repeat, long sleep, Callable<T> action) throws Exception {
        for (; repeat > 0; repeat -= 1) {
            try {
                return action.call();
            } catch (Exception e) {
                if (repeat > 1) {
                    log.info("action '{}' failed, attempting retry. there are {} retries left", name, repeat - 1);
                } else {
                    log.error("action '{}' failed after all retries", name);
                    throw e;
                }
            }

            // I hate sleeps, but it seems appropriate
            Thread.sleep(sleep);
        }

        throw new RuntimeException("CANNOT COMPLETE");
    }

    public void activate() {
        // Create a connection to etcd
        log.info("creating a connection to etcd at '{}' to support an external cluster metadata store, using key '{}'",
                connection, configKey);
        etcd = ClientBuilder.builder().hosts(URI.create(connection)).createClient();

        /*
         * Prime the pump by doing a query against etcd to get the latest. This way we can publish to ONOS the latest
         * data as well as do a wait to get all changes since that key
         */
        log.error("Attempting to get " + configKey);
        Response prime = etcd.getConsistent(configKey);
        long primeIdx = 0;
        if (prime.successful()) {
            if (delegate != null) {
                delegate.notify(new ClusterMetadataEvent(ClusterMetadataEvent.Type.METADATA_CHANGED,
                        responseToClusterMetadata(prime), prime.node().getModifiedIndex()));
            }
            primeIdx = prime.node().getModifiedIndex() + 1;
        } else if (prime.responseCode() != 404 /* NotFound */) {
            /*
             * If we get something other than NotFound (i.e. no one has set the key) then this is bad news and we need
             * to throw an exception.
             */
            log.error(
                    "unexpected failure when attempting to retrieved cluster information from etcd, error code == '{}'",
                    prime.responseCode());
            // TODO: should be checked exception
            throw new RuntimeException(
                    new ClusterMetadataReadFailureException(Type.Cluster, String.valueOf(prime.responseCode())));

        } else /* NotFound */ {
            log.info("key used for cluster meta data, '{}', not found", configKey);
        }

        active.set(true);
        log.debug("starting change watch for key '{}'", configKey);
        etcd.wait(new Handler<Response>() {
            @Override
            public void handle(Response event) {
                if (event.successful()) {
                    log.info("received update '{}' from etcd with data '{}'", event.node().getModifiedIndex(),
                            event.node().getValue());
                    if (delegate != null) {
                        delegate.notify(new ClusterMetadataEvent(ClusterMetadataEvent.Type.METADATA_CHANGED,
                                responseToClusterMetadata(event), event.node().getModifiedIndex()));
                    }
                    if (active.get()) {
                        etcd.wait(this, configKey, event.node().getModifiedIndex() + 1);
                    }
                } else if (event.responseCode() == 404 /* NotFount */) {
                    log.warn("key used for cluster information, '{}', not found", configKey);
                } else {
                    log.error("unexpected failure when waiting for change on cluster information"
                            + " from etcd, error code == '{}'", event.responseCode());
                }
            }
        }, configKey, primeIdx);
    }

    public void deactivate() {
        active.set(false);
    }

    @Override
    public boolean hasDelegate() {
        return this.delegate != null;
    }

    @Override
    public void setDelegate(ClusterMetadataStoreDelegate delegate) {
        checkNotNull(delegate, "Delegate cannot be set to null");
        this.delegate = delegate;
    }

    @Override
    public void unsetDelegate(ClusterMetadataStoreDelegate delegate) {
        if (this.delegate == delegate) {
            this.delegate = null;
        }
    }

    /**
     * Converts the information returned from etcd for cluster information into the data structure used internally by
     * ONOS.
     *
     * @see org.onosproject.cluster.ClusterMetadata
     *
     * @param response
     * @return
     */
    private ClusterMetadata responseToClusterMetadata(Response response) {
        if (!response.successful()) {
            throw new RuntimeException(String.format("Unable to read meta data store for cluster information: %d",
                    response.responseCode()));
        }

        /*
         * The data in etcd is a stringified JSON object. This means that we can use standard tools to convert if to a
         * usable structure (map of maps) and encode the data into the required structures
         */
        JsonObject jobj = new JsonObject(response.node().getValue());

        List<ControllerNode> nodes = new ArrayList<ControllerNode>();
        JsonObject jnodes = jobj.getObject(CONTROLLER_NODES_KEY);
        JsonObject jnode;
        for (String id : jnodes.getFieldNames()) {
            jnode = jnodes.getObject(id);
            nodes.add(new DefaultControllerNode(new NodeId(id), IpAddress.valueOf(jnode.getString(NODE_IP_KEY)),
                    jnode.getInteger(NODE_PORT_KEY)));
        }

        List<Partition> partitions = new ArrayList<Partition>();
        JsonObject jparts = jobj.getObject(CONTROLLER_PARTITIONS_KEY);
        List<NodeId> members;
        for (String partId : jparts.getFieldNames()) {
            members = new ArrayList<NodeId>();
            for (Object obj : jparts.getArray(partId)) {
                members.add(new NodeId((String) obj));
            }
            partitions.add(new Partition(partId, members));
        }

        return ClusterMetadata.builder().withName(clusterName).withControllerNodes(nodes).withPartitions(partitions)
                .build();
    }

    @Override
    public Versioned<ClusterMetadata> getClusterMetadata() {

        try {
            Response response = retryWithResult("get cluster meta data", 3, 200, new Callable<Response>() {
                @Override
                public Response call() {
                    return etcd.getConsistent(configKey);
                }
            });

            if (response.successful()) {
                return new Versioned<ClusterMetadata>(responseToClusterMetadata(response),
                        response.node().getModifiedIndex());
            }
            throw new RuntimeException(new ClusterMetadataReadFailureException(Type.Cluster, ""));
        } catch (Exception e) {
            // TODO: should be checked exception
            throw new RuntimeException(new ClusterMetadataReadFailureException(Type.Cluster, "", e));
        }
    }

    @Override
    public void setClusterMetadata(ClusterMetadata md) {
        // Convert to JsonObject for storage in etcd
        JsonObject result = new JsonObject();
        JsonObject nodes = new JsonObject();
        JsonObject jobj;
        for (ControllerNode node : md.getNodes()) {
            jobj = new JsonObject();
            jobj.putString(NODE_IP_KEY, node.ip().toString());
            jobj.putNumber(NODE_PORT_KEY, node.tcpPort());
            nodes.putObject(node.id().toString(), jobj);
        }
        result.putObject(CONTROLLER_NODES_KEY, nodes);

        JsonObject parts = new JsonObject();
        JsonArray nodeList;
        for (Partition part : md.getPartitions()) {
            nodeList = new JsonArray();
            for (NodeId member : part.getMembers()) {
                nodeList.addString(member.toString());
            }
            parts.putArray(part.getName(), nodeList);
        }
        result.putObject(CONTROLLER_PARTITIONS_KEY, parts);

        try {
            retry("update cluster metadata", 3, 200, new Runnable() {
                @Override
                public void run() {
                    log.error("setting '{}' == '{}'", configKey, result.encodePrettily());
                    Response response = etcd.set(configKey, result.encode());
                    if (!response.successful()) {
                        log.error("unable to set value, received reponse code '{}'", response.responseCode());
                        throw new RuntimeException(new ClusterMetadataWriteFailureException(Type.Cluster, ""));
                    } else {
                        log.info("successfully set");
                    }
                }
            });
        } catch (Exception e) {
            // TODO should be checked exception
            throw new RuntimeException(new ClusterMetadataWriteFailureException(Type.Cluster, "", e));
        }

    }

    @Override
    public Collection<NodeId> getActiveReplicas(String partitionId) {
        List<NodeId> result = new ArrayList<NodeId>();

        String operKey = String.format(CLUSTER_OPERATIONAL_KEY, clusterName, partitionId);

        try {
            Response response = retryWithResult(String.format("get active replica in partition '%s'", partitionId), 3,
                    200, new Callable<Response>() {

                @Override
                public Response call() throws Exception {
                    return etcd.getConsistent(operKey);
                }
            });
            if (response.successful()) {

                JsonArray part = new JsonArray(response.node().getValue());
                if (part != null) {
                    for (Object jnode : part) {
                        result.add(new NodeId((String) jnode));
                    }
                }
            }
            return result;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void setActiveReplica(String partitionId, NodeId nodeId) {
        /*
         * No real easy way to manage this as a "piecemeal" operation, so instead the code has to pull the whole
         * structure and then update the structure with the updated information. We need to be careful about the change
         * indexes so that we don't stomp on something unexpectedly so we will use optimistic locking (i.e., only write
         * back if the current version is what we thought it was when we started the change).
         */
        String operKey = String.format(CLUSTER_OPERATIONAL_KEY, clusterName, partitionId);
        try {
            retry(String.format("set active replica '%s' in partition '%s'", nodeId.toString(), partitionId), 3, 200,
                    new Runnable() {
                @Override
                public void run() {
                    Response response = etcd.getConsistent(operKey);
                    if (response.successful() || response.responseCode() == 404) {
                        JsonArray members = null;
                        if (response.responseCode() == 404) {
                            members = new JsonArray();
                        } else {
                            members = new JsonArray(response.node().getValue());
                        }

                        // Not terrible efficient, but walk the array looking for a match
                        String target = nodeId.toString();
                        for (Object id : members) {
                            if (target.equals(id)) {
                                // Already exists, we are done.
                                return;
                            }
                        }

                        // Does not exist, we need to add
                        members.addString(target);

                        // Push change back to etcd
                        response = etcd.compareAndSwapByModifiedIndex(operKey,
                                response.node().getModifiedIndex(), members.encode());
                        if (response.successful()) {
                            // We are done
                            return;
                        }
                        throw new RuntimeException(new ClusterMetadataWriteFailureException(
                                ClusterMetadataWriteFailureException.Type.Partition, partitionId));
                    }
                }
            });
        } catch (Exception e) {
            // TODO: really shouldn't be a runtime exception
            throw new RuntimeException(new ClusterMetadataWriteFailureException(
                    ClusterMetadataWriteFailureException.Type.Partition, partitionId, e));
        }
    }

    @Override
    public void unsetActiveReplica(String partitionId, NodeId nodeId) {
        String operKey = String.format(CLUSTER_OPERATIONAL_KEY, clusterName, partitionId);
        try {
            retry(String.format("set active replica '%s' in partition '%s'", nodeId.toString(), partitionId), 3, 200,
                    new Runnable() {
                @Override
                public void run() {
                    Response response = etcd.getConsistent(operKey);
                    if (response.successful() || response.responseCode() == 404) {
                        if (response.responseCode() == 404) {
                            // done
                            return;
                        }
                        // If the specified node isn't in the named partition add it
                        JsonArray members = new JsonArray(response.node().getValue());

                        /*
                         * Walk through the member list and build a new array of all existing members except the
                         * one to be removed.
                         */
                        JsonArray replace = new JsonArray();
                        String target = nodeId.toString();
                        for (Object id : members) {
                            if (!target.equals(id)) {
                                replace.addString((String) id);
                            }
                        }

                        if (replace.size() == members.size()) {
                            // No change, no need to update
                            return;
                        }

                        // Push change back to etcd
                        response = etcd.compareAndSwapByModifiedIndex(operKey,
                                response.node().getModifiedIndex(), replace.encode());
                        if (response.successful()) {
                            // We are done
                            return;
                        }
                        throw new RuntimeException(new ClusterMetadataWriteFailureException(
                                ClusterMetadataWriteFailureException.Type.Partition, partitionId));
                    }
                }
            });
        } catch (Exception e) {
            // TODO: really shouldn't be a runtime exception
            throw new RuntimeException(new ClusterMetadataWriteFailureException(
                    ClusterMetadataWriteFailureException.Type.Partition, partitionId, e));
        }
    }
}
