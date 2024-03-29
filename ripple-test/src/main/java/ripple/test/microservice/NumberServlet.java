// Copyright (c) 2023 Institute of Software, Chinese Academy of Sciences
// Ripple is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

package ripple.test.microservice;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.eclipse.jetty.http.HttpStatus;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

public class NumberServlet extends HttpServlet {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private NumberService numberService;

    private NumberService getNumberService() {
        return numberService;
    }

    private void setNumberService(NumberService numberService) {
        this.numberService = numberService;
    }

    public NumberServlet(NumberService numberService) {
        this.setNumberService(numberService);
    }

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("<html>\n");
        stringBuilder.append("    <head>\n");
        stringBuilder.append("        <title>Number Service</title>\n");
        stringBuilder.append("    </head>\n");
        stringBuilder.append("    <body>\n");
        stringBuilder.append("        <div>\n");
        stringBuilder.append("            <p>\n");
        stringBuilder.append("                ").append("该服务返回数字：").append(this.getNumberService().getNumber()).append("\n");
        stringBuilder.append("            </p>\n");
        stringBuilder.append("        </div>\n");
        stringBuilder.append("    </body>\n");
        stringBuilder.append("</html>\n");
        String content = stringBuilder.toString();

        response.setContentType("text/html;charset=UTF-8");
        response.setStatus(HttpStatus.OK_200);
        response.getWriter().println(content);
    }

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException {
        response.setContentType("application/json;charset=UTF-8");
        response.setStatus(HttpStatus.OK_200);
        response.getWriter().println(MAPPER.writeValueAsString(this.getNumberService().getNumber()));
    }
}
