// Copyright (c) 2023 Institute of Software, Chinese Academy of Sciences
// Ripple is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

package ripple.server.ui;

import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ripple.common.entity.ClientMetadata;
import ripple.server.core.Node;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * @author Zhen Tang
 */
public class ClientClusterServlet extends BaseServlet {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClientClusterServlet.class);

    public ClientClusterServlet(Node node) {
        super(node);
    }

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
        LOGGER.info("[ClientClusterServlet] Receive GET request.");

        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("                <p>\n");
        stringBuilder.append("                    ")
                .append(this.getNode().getStringTable().totalNumberOfClients())
                .append(" <strong>")
                .append(this.getNode().getSubscription().size() * this.getNode().getConnectedClients().size())
                .append("</strong>")
                .append("\n");
        stringBuilder.append("                </p>\n");
//        if (this.getNode().getConnectedClients().size() > 0) {
//            stringBuilder.append("                <table class=\"table table-striped\">\n");
//            stringBuilder.append("                    <thead>\n");
//            stringBuilder.append("                    <tr>\n");
//            stringBuilder.append("                        <th>");
//            stringBuilder.append(this.getNode().getStringTable().lineNumber());
//            stringBuilder.append("</th>\n");
//            stringBuilder.append("                        <th>");
//            stringBuilder.append(this.getNode().getStringTable().clientIpAddress());
//            stringBuilder.append("</th>\n");
//            stringBuilder.append("                        <th>");
//            stringBuilder.append(this.getNode().getStringTable().clientApiPort());
//            stringBuilder.append("</th>\n");
//            stringBuilder.append("                    </tr>\n");
//            stringBuilder.append("                    </thead>\n");
//            stringBuilder.append("                    <tbody>\n");
//
//            int i = 0;
//            for (ClientMetadata metadata : this.getNode().getConnectedClients()) {
//                stringBuilder.append("                    <tr>\n");
//                stringBuilder.append("                        <td>")
//                        .append(i + 1)
//                        .append("</td>\n");
//                stringBuilder.append("                        <td>")
//                        .append(metadata.getAddress())
//                        .append("</td>\n");
//                stringBuilder.append("                        <td>")
//                        .append(metadata.getPort())
//                        .append("</td>\n");
//                stringBuilder.append("                    </tr>\n");
//                i++;
//            }
//
//            stringBuilder.append("                    </tbody>\n");
//            stringBuilder.append("                </table>\n");
//        }

        String content = stringBuilder.toString();

        String pageContent = PageGenerator.buildPage("Ripple Server - " + this.getNode().getStringTable().connectedClients()
                , this.getNode().getStringTable().connectedClients(), content, this.getNode().getStringTable());

        response.setContentType("text/html;charset=UTF-8");
        response.setStatus(HttpStatus.OK_200);
        response.getWriter().println(pageContent);
    }
}
