package ripple.server.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ripple.server.helper.Api;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Zhen Tang
 */
public class HealthManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(HealthManager.class);

    private Node node;
    private Map<NodeMetadata, Boolean> aliveMap;

    public Node getNode() {
        return node;
    }

    public void setNode(Node node) {
        this.node = node;
    }

    public Map<NodeMetadata, Boolean> getAliveMap() {
        return aliveMap;
    }

    public void setAliveMap(Map<NodeMetadata, Boolean> aliveMap) {
        this.aliveMap = aliveMap;
    }

    public HealthManager(Node node) {
        this.setNode(node);
        this.setAliveMap(new ConcurrentHashMap<>());
    }

    public void init() {
        for (NodeMetadata metadata : this.getNode().getNodeList()) {
            this.getAliveMap().put(metadata, false);
        }
    }

    public void checkHealth() {
        for (NodeMetadata metadata : this.getNode().getNodeList()) {
            if (this.getAliveMap().get(metadata) == false) {
                boolean result = Api.heartbeat(metadata.getAddress(), metadata.getPort());
                this.getAliveMap().put(metadata, result);
            }
        }
    }

    public boolean isAlive(NodeMetadata metadata) {
        return this.getAliveMap().get(metadata);
    }
}