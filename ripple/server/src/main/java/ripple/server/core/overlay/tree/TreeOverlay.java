package ripple.server.core.overlay.tree;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ripple.common.entity.NodeMetadata;
import ripple.server.core.overlay.Overlay;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Zhen Tang
 */
public class TreeOverlay implements Overlay {
    private static final Logger LOGGER = LoggerFactory.getLogger(TreeOverlay.class);

    private int branch;
    private ConcurrentHashMap<NodeMetadata, CompleteTree> trees;

    public TreeOverlay(int branch) {
        this.setBranch(branch);
        this.setTrees(new ConcurrentHashMap<>());
    }

    public int getBranch() {
        return branch;
    }

    public void setBranch(int branch) {
        this.branch = branch;
    }

    public ConcurrentHashMap<NodeMetadata, CompleteTree> getTrees() {
        return trees;
    }

    public void setTrees(ConcurrentHashMap<NodeMetadata, CompleteTree> trees) {
        this.trees = trees;
    }

    @Override
    public void buildOverlay(List<NodeMetadata> nodeList) {
        int i = 0;
        for (i = 0; i < nodeList.size(); i++) {
            NodeMetadata source = nodeList.get(i);
            List<NodeMetadata> list = this.calculateNodeListForSource(source, nodeList);
            CompleteTree tree = new CompleteTree(this.getBranch(), list);
            this.getTrees().put(source, tree);
        }
    }

    public List<NodeMetadata> calculateNodeListForSource(NodeMetadata source, List<NodeMetadata> nodeList) {
        List<NodeMetadata> ret = new ArrayList<>(nodeList);
        int i = 0;
        for (i = 0; i < nodeList.size(); i++) {
            if (source.equals(nodeList.get(i))) {
                break;
            }
        }
        Collections.rotate(ret, -i);
        return ret;
    }

    public List<NodeMetadata> generateNodeList(List<TreeNode> treeNodeList) {
        List<NodeMetadata> ret = new ArrayList<>();
        int i = 0;
        for (i = 0; i < treeNodeList.size(); i++) {
            NodeMetadata nodeMetadata = treeNodeList.get(i).getNodeMetadata();
            LOGGER.info("[TreeOverlay] Attempting to send to node {} ({}:{})."
                    , nodeMetadata.getId(), nodeMetadata.getAddress(), nodeMetadata.getPort());
            ret.add(nodeMetadata);
        }
        return ret;
    }

    @Override
    public List<NodeMetadata> calculateNodesToSync(NodeMetadata source, NodeMetadata current) {
        LOGGER.info("[TreeOverlay] calculateNodesToSync() called. sourceId = {}, currentId = {}", source.getId(), current.getId());
        CompleteTree tree = this.getTrees().get(source);
        for (TreeNode treeNode : tree.getNodeList()) {
            if (treeNode.getNodeMetadata().equals(current)) {
                return this.generateNodeList(treeNode.getChildren());
            }
        }
        LOGGER.warn("[TreeOverlay] I cannot find children list for node {}.", current.getId());
        return new ArrayList<>();
    }
}
