package ripple.server;

import javax.servlet.http.HttpServlet;

public class BaseServlet extends HttpServlet {
    private AbstractNode node;

    public BaseServlet(AbstractNode node) {
        this.setNode(node);
    }

    public AbstractNode getNode() {
        return node;
    }

    private void setNode(AbstractNode node) {
        this.node = node;
    }

}