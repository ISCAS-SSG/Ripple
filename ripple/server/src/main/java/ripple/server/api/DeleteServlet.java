package ripple.server.api;

import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ripple.common.Parameter;
import ripple.server.core.BaseServlet;
import ripple.server.core.Node;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * @author Zhen Tang
 */
public class DeleteServlet extends BaseServlet {
    private static final Logger LOGGER = LoggerFactory.getLogger(DeleteServlet.class);

    public DeleteServlet(Node node) {
        super(node);
    }

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException {
        String applicationName = request.getHeader(Parameter.APPLICATION_NAME);
        String key = request.getHeader(Parameter.KEY);

        LOGGER.info("[DeleteServlet] Receive POST request. Application Name = {}, Key = {}.", applicationName, key);

        boolean result = this.getNode().delete(applicationName, key);

        response.setContentType("application/json;charset=UTF-8");
        response.setStatus(HttpStatus.OK_200);
        response.getWriter().println(this.getNode().getObjectMapper().writeValueAsString(result));
    }
}