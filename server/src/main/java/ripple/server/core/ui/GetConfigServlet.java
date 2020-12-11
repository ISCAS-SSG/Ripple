package ripple.server.core.ui;

import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ripple.common.Item;
import ripple.common.Message;
import ripple.common.MessageType;
import ripple.common.UpdateMessage;
import ripple.server.core.BaseServlet;
import ripple.server.core.Node;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.List;

/**
 * @author Zhen Tang
 */
public class GetConfigServlet extends BaseServlet {
    private static final Logger LOGGER = LoggerFactory.getLogger(GetConfigServlet.class);

    public GetConfigServlet(Node node) {
        super(node);
    }

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
        LOGGER.info("[GetConfigServlet] Receive GET request.");

        List<Item> allConfigs = this.getNode().getAll();

        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("                <p>\n");
        stringBuilder.append("                    ")
                .append("当前服务器节点本地存储中共检索到 <strong>")
                .append(allConfigs.size())
                .append("</strong> 条配置。")
                .append("\n");
        stringBuilder.append("                </p>\n");
        if (allConfigs.size() > 0) {
            stringBuilder.append("                <table class=\"table table-striped\">\n");
            stringBuilder.append("                    <thead>\n");
            stringBuilder.append("                    <tr>\n");
            stringBuilder.append("                        <th>序号</th>\n");
            stringBuilder.append("                        <th>应用名称</th>\n");
            stringBuilder.append("                        <th>键</th>\n");
            stringBuilder.append("                        <th>历史记录</th>\n");
            stringBuilder.append("                    </tr>\n");
            stringBuilder.append("                    </thead>\n");
            stringBuilder.append("                    <tbody>\n");

            int i = 0;
            for (i = 0; i < allConfigs.size(); i++) {
                Item item = allConfigs.get(i);

                String history = "";
                for (Message message : item.getMessages()) {
                    history += "                            <span>UUID: " + message.getUuid() + "; </span>";
                    history += "                            <span>类型: " + (message.getType().equals(MessageType.UPDATE) ? "更新" : "删除") + "; </span>";
                    if (message instanceof UpdateMessage) {
                        history += "                            <span>值: " + ((UpdateMessage) message).getValue() + "; </span>";
                    }
                    history += "                            <span>最后修改时间: " + SimpleDateFormat.getDateTimeInstance().format(message.getLastUpdate()) + "; </span>";
                    history += "                            <span>服务器ID: " + SimpleDateFormat.getDateTimeInstance().format(message.getLastUpdateServerId()) + "; </span>";
                }

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
                        .append(history)
                        .append("</td>\n");
                stringBuilder.append("                    </tr>\n");
            }

            stringBuilder.append("                    </tbody>\n");
            stringBuilder.append("                </table>\n");
        }

        String content = stringBuilder.toString();

        String pageContent = PageGenerator.buildPage("Ripple Server - 查询配置", "查询配置", content);

        response.setContentType("text/html;charset=UTF-8");
        response.setStatus(HttpStatus.OK_200);
        response.getWriter().println(pageContent);
    }
}
