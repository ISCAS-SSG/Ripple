// Copyright (c) 2023 Institute of Software, Chinese Academy of Sciences
// Ripple is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

package ripple.client.core.ui;

import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ripple.client.RippleClient;
import ripple.common.entity.Item;
import ripple.common.entity.NodeMetadata;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * @author Zhen Tang
 */
public class GetSubscriptionServlet extends BaseServlet {
    private static final Logger LOGGER = LoggerFactory.getLogger(GetSubscriptionServlet.class);

    public GetSubscriptionServlet(RippleClient client) {
        super(client);
    }

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
        LOGGER.info("[GetSubscriptionServlet] Receive GET request.");

        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("                <p>\n");
        stringBuilder.append("                    ")
                .append(this.getClient().getStringTable().clientNumberOfSubscription())
                .append(" <strong>")
                .append(this.getClient().getSubscriptions().size())
                .append("</strong>")
                .append("\n");
        stringBuilder.append("                </p>\n");
        if (this.getClient().getSubscriptions().size() > 0) {
            stringBuilder.append("                <table class=\"table table-striped\">\n");
            stringBuilder.append("                    <thead>\n");
            stringBuilder.append("                    <tr>\n");
            stringBuilder.append("                        <th>");
            stringBuilder.append(this.getClient().getStringTable().lineNumber());
            stringBuilder.append("</th>\n");
            stringBuilder.append("                        <th>");
            stringBuilder.append(this.getClient().getStringTable().applicationName());
            stringBuilder.append("</th>\n");
            stringBuilder.append("                        <th>");
            stringBuilder.append(this.getClient().getStringTable().key());
            stringBuilder.append("</th>\n");
            stringBuilder.append("                        <th>");
            stringBuilder.append(this.getClient().getStringTable().serverId());
            stringBuilder.append("</th>\n");
            stringBuilder.append("                        <th>");
            stringBuilder.append(this.getClient().getStringTable().serverIpAddress());
            stringBuilder.append("</th>\n");
            stringBuilder.append("                        <th>");
            stringBuilder.append(this.getClient().getStringTable().serverApiPort());
            stringBuilder.append("</th>\n");
            stringBuilder.append("                    </tr>\n");
            stringBuilder.append("                    </thead>\n");
            stringBuilder.append("                    <tbody>\n");

            int i = 0;
            for (Item item : this.getClient().getSubscriptions().keySet()) {
                NodeMetadata nodeMetadata = this.getClient().getSubscriptions().get(item);
                stringBuilder.append("                    <tr>\n");
                stringBuilder.append("                        <td>")
                        .append(i + 1)
                        .append("</td>\n");
                stringBuilder.append("                        <td>")
                        .append(item.getApplicationName())
                        .append("</td>\n");
                stringBuilder.append("                        <td>")
                        .append(item.getKey())
                        .append("</td>\n");
                stringBuilder.append("                        <td>")
                        .append(nodeMetadata.getId())
                        .append("</td>\n");
                stringBuilder.append("                        <td>")
                        .append(nodeMetadata.getAddress())
                        .append("</td>\n");
                stringBuilder.append("                        <td>")
                        .append(nodeMetadata.getPort())
                        .append("</td>\n");
                stringBuilder.append("                    </tr>\n");
                i++;
            }

            stringBuilder.append("                    </tbody>\n");
            stringBuilder.append("                </table>\n");
        }

        String content = stringBuilder.toString();

        String pageContent = PageGenerator.buildPage("Ripple Client - " + this.getClient().getStringTable().clientGetSubscription()
                , this.getClient().getStringTable().clientGetSubscription(), content, this.getClient().getStringTable());

        response.setContentType("text/html;charset=UTF-8");
        response.setStatus(HttpStatus.OK_200);
        response.getWriter().println(pageContent);
    }
}
