package org.ciena.onosproject.cluster.etcd.impl;

import static com.google.common.base.Preconditions.checkNotNull;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.boon.core.Handler;
import org.boon.etcd.ClientBuilder;
import org.boon.etcd.Etcd;
import org.boon.etcd.Response;
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

    private static final String CLUSTER_KEY = "onos/cluster/%s";

    private static final String CONTROLLER_NODES_KEY = "nodes";
    private static final String NODE_IP_KEY = "ip";
    private static final String NODE_PORT_KEY = "port";
    private static final String CONTROLLER_PARTITIONS_KEY = "partitions";
    private static final String PARTITION_NODES_KEY = "nodes";
    private static final String DEFAULT_CLUSTER_NAME = "default";
    private static final String DEFAULT_ETCD_CONNECTION = "http://127.0.0.1:4001";

    private ClusterMetadataStoreDelegate delegate = null;
    private Etcd etcd = null;
    private AtomicBoolean active = new AtomicBoolean(false);
    private final String clusterName;
    private final String key;
    private final String connection;

    public EtcdClusterMetadataStore(String connection, String clusterName) {
        this.clusterName = clusterName;
        this.key = String.format(CLUSTER_KEY, clusterName);

        this.connection = (connection == null ? DEFAULT_ETCD_CONNECTION : connection);
    }

    public EtcdClusterMetadataStore() {
        this(null, DEFAULT_CLUSTER_NAME);
    }

    public void activate() {
        // Create a connection to etcd
        log.info("creating a connection to etcd at '{}' to support an external cluster metadata store, using key '{}'",
                connection, key);
        etcd = ClientBuilder.builder().hosts(URI.create(connection)).createClient();

        /*
         * Prime the pump by doing a query against etcd to get the latest. This
         * way we can publish to ONOS the latest data as well as do a wait to
         * get all changes since that key
         */
        Response prime = etcd.getConsistent(key);
        long primeIdx = 0;
        if (prime.successful()) {
            if (delegate != null) {
                delegate.notify(new ClusterMetadataEvent(ClusterMetadataEvent.Type.METADATA_CHANGED,
                        responseToClusterMetadata(prime), prime.node().getModifiedIndex()));
            }
            primeIdx = prime.node().getModifiedIndex() + 1;
        } else if (prime.responseCode() != 404 /* NotFound */) {
            /*
             * If we get something other than NotFound (i.e. no one has set the
             * key) then this is bad news and we need to throw an exception.
             */
            log.error(
                    "unexpected failure when attempting to retrieved cluster information from etcd, error code == '{}'",
                    prime.responseCode());
            // TODO: Throw exception

        } else /* NotFound */ {
            log.info("key used for cluster meta data, '{}', not found", key);
        }

        active.set(true);
        log.debug("starting change watch for key '{}'", key);
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
                        etcd.wait(this, key, event.node().getModifiedIndex() + 1);
                    }
                } else if (event.responseCode() == 404 /* NotFount */) {
                    log.warn("key used for cluster information, '{}', not found", key);
                } else {
                    log.error("unexpected failure when waiting for change on cluster information"
                            + " from etcd, error code == '{}'", event.responseCode());
                }
            }
        }, key, primeIdx);
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
     * Converts the information returned from etcd for cluster information into
     * the data structure used internally by ONOS.
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
         * The data in etcd is a stringified JSON object. This means that we can
         * use standard tools to convert if to a usable structure (map of maps)
         * and encode the data into the required structures
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

        Response response = etcd.getConsistent(key);

        if (response.successful()) {
            return new Versioned<ClusterMetadata>(responseToClusterMetadata(response),
                    response.node().getModifiedIndex());
        }

        // TODO: throw exception
        return null;
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

        log.error("setting '{}' == '{}'", key, result.encodePrettily());
        Response response = etcd.set(key, result.encode());
        if (!response.successful()) {
            log.error("unable to set value, received reponse code '{}'", response.responseCode());
            // TODO: exception
        } else {
            log.info("successfully set");
        }
    }

    @Override
    public Collection<NodeId> getActiveReplicas(String partitionId) {
        List<NodeId> result = new ArrayList<NodeId>();

        Response response = etcd.getConsistent(key);
        if (response.successful()) {

            JsonObject jobj = new JsonObject(response.node().getValue());
            JsonObject part = jobj.getObject(CONTROLLER_PARTITIONS_KEY).getObject(partitionId, null);
            if (part != null) {
                for (Object jnode : part.getArray(PARTITION_NODES_KEY)) {
                    result.add(new NodeId((String) jnode));
                }
            }
        }
        return result;
    }

    @Override
    public void setActiveReplica(String partitionId, NodeId nodeId) {
        // TODO Auto-generated method stub

    }

    @Override
    public void unsetActiveReplica(String partitionId, NodeId nodeId) {
        // TODO Auto-generated method stub
    }
}
