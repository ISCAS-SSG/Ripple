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

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * @author Zhen Tang
 */
public class ModifyConfigServlet extends BaseServlet {
    private static final Logger LOGGER = LoggerFactory.getLogger(ModifyConfigServlet.class);

    public ModifyConfigServlet(RippleClient client) {
        super(client);
    }

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
        LOGGER.info("[ModifyConfigServlet] Receive GET request.");

        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("                <form action=\"").append(Endpoint.UI_MODIFY_CONFIG).append("\" method=\"post\">\n");
        stringBuilder.append("                    <div class=\"form-group\">\n");
        stringBuilder.append("                        <label for=\"applicationName\">");
        stringBuilder.append(this.getClient().getStringTable().applicationName());
        stringBuilder.append("</label>\n");
        stringBuilder.append("                        <input id=\"applicationName\" name=\"applicationName\" class=\"form-control\" type=\"text\" />\n");
        stringBuilder.append("                    </div>\n");
        stringBuilder.append("                    <div class=\"form-group\">\n");
        stringBuilder.append("                        <label for=\"key\">");
        stringBuilder.append(this.getClient().getStringTable().key());
        stringBuilder.append("</label>\n");
        stringBuilder.append("                        <input id=\"key\" name=\"key\" class=\"form-control\" type=\"text\" />\n");
        stringBuilder.append("                    </div>\n");
        stringBuilder.append("                    <div class=\"form-group\">\n");
        stringBuilder.append("                        <label for=\"value\">");
        stringBuilder.append(this.getClient().getStringTable().value());
        stringBuilder.append("</label>\n");
        stringBuilder.append("                        <input id=\"value\" name=\"value\" class=\"form-control\" type=\"text\" />\n");
        stringBuilder.append("                    </div>\n");
        stringBuilder.append("                    <div class=\"form-group\">\n");
        stringBuilder.append("                        <button type=\"submit\" class=\"form-control btn btn-outline-primary\">");
        stringBuilder.append(this.getClient().getStringTable().modifyConfig());
        stringBuilder.append("</button>\n");
        stringBuilder.append("                    </div>\n");
        stringBuilder.append("                </form>\n");
        String content = stringBuilder.toString();

        String pageContent = PageGenerator.buildPage("Ripple Client - " + this.getClient().getStringTable().modifyConfig()
                , this.getClient().getStringTable().modifyConfig(), content, this.getClient().getStringTable());

        response.setContentType("text/html;charset=UTF-8");
        response.setStatus(HttpStatus.OK_200);
        response.getWriter().println(pageContent);
    }

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        String applicationName = request.getParameter("applicationName");
        String key = request.getParameter("key");
        String value = request.getParameter("value");
        LOGGER.info("[ModifyConfigServlet] Receive POST request. applicationName = {}, key = {}, value = {}."
                , applicationName, key, value);

        this.getClient().put(applicationName, key, value);

        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("                <p>\n");
        stringBuilder.append("                    ");
        stringBuilder.append(this.getClient().getStringTable().operationSubmitted());
        stringBuilder.append("\n");
        stringBuilder.append("                </p>\n");
        String content = stringBuilder.toString();

        String pageContent = PageGenerator.buildPage("Ripple Client - " + this.getClient().getStringTable().modifyConfig()
                , this.getClient().getStringTable().modifyConfig(), content, this.getClient().getStringTable());

        response.setContentType("text/html;charset=UTF-8");
        response.setStatus(HttpStatus.OK_200);
        response.getWriter().println(pageContent);
    }
}
