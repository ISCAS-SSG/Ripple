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

import ripple.common.helper.PageHelper;
import ripple.common.helper.StringTable;

/**
 * @author Zhen Tang
 */
public final class PageGenerator {
    private PageGenerator() {

    }

    private static void appendSideBar(StringBuilder stringBuilder, String currentFunction, StringTable stringTable) {
        stringBuilder.append("        <nav class=\"col-md-2 d-none d-md-block bg-light sidebar\">\n");
        stringBuilder.append("            <div class=\"sidebar-sticky\">\n");
        stringBuilder.append("                <ul class=\"nav flex-column mb-2\">\n");
        PageHelper.buildSidebarItem(stringBuilder, "主页", Endpoint.UI_HOME, "home", currentFunction);
        stringBuilder.append("                </ul>\n");
        PageHelper.buildSidebarTitle(stringBuilder, "配置管理");
        stringBuilder.append("                <ul class=\"nav flex-column mb-2\">\n");
        PageHelper.buildSidebarItem(stringBuilder, "查询配置", Endpoint.UI_GET_CONFIG, "search", currentFunction);
        PageHelper.buildSidebarItem(stringBuilder, stringTable.addConfig(), Endpoint.UI_ADD_CONFIG, "file-plus", currentFunction);
        PageHelper.buildSidebarItem(stringBuilder, "修改配置", Endpoint.UI_MODIFY_CONFIG, "edit", currentFunction);
        PageHelper.buildSidebarItem(stringBuilder, "增量更新", Endpoint.UI_INCREMENTAL_UPDATE, "edit-3", currentFunction);
        PageHelper.buildSidebarItem(stringBuilder, "删除配置", Endpoint.UI_REMOVE_CONFIG, "trash-2", currentFunction);
        stringBuilder.append("                </ul>\n");
        PageHelper.buildSidebarTitle(stringBuilder, "订阅管理");
        stringBuilder.append("                <ul class=\"nav flex-column mb-2\">\n");
        PageHelper.buildSidebarItem(stringBuilder, "查询订阅关系", Endpoint.UI_GET_SUBSCRIPTION, "share-2", currentFunction);
        stringBuilder.append("                </ul>\n");
        PageHelper.buildSidebarTitle(stringBuilder, "集群管理");
        stringBuilder.append("                <ul class=\"nav flex-column mb-2\">\n");
        PageHelper.buildSidebarItem(stringBuilder, "服务器集群信息", Endpoint.UI_SERVER_CLUSTER, "server", currentFunction);
        PageHelper.buildSidebarItem(stringBuilder, "已连接的客户端", Endpoint.UI_CLIENT_CLUSTER, "list", currentFunction);
        stringBuilder.append("                </ul>\n");
        stringBuilder.append("            </div>\n");
        stringBuilder.append("        </nav>\n");
    }

    public static String buildPage(String pageTitle, String currentFunction, String content, StringTable stringTable) {
        StringBuilder stringBuilder = new StringBuilder();
        PageHelper.appendHtmlStartTag(stringBuilder, stringTable.htmlLanguage());
        PageHelper.appendHeadSection(stringBuilder, pageTitle);
        PageHelper.appendBodyStartTag(stringBuilder);
        PageHelper.appendNavigationBar(stringBuilder, "Ripple Server");
        PageHelper.appendDivStartTag(stringBuilder);
        PageGenerator.appendSideBar(stringBuilder, currentFunction, stringTable);
        PageHelper.appendMainSection(stringBuilder, currentFunction, content);
        PageHelper.appendDivEndTag(stringBuilder);
        PageHelper.appendFeatherScriptSection(stringBuilder);
        PageHelper.appendBodyEndTag(stringBuilder);
        PageHelper.appendHtmlEndTag(stringBuilder);
        return stringBuilder.toString();
    }
}
