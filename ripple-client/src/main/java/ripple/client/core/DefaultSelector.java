// Copyright (c) 2025 Institute of Software, Chinese Academy of Sciences
// Ripple is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

package ripple.client.core;

import ripple.common.entity.NodeMetadata;

import java.util.List;

public class DefaultSelector implements NodeSelector {
    @Override
    public NodeMetadata selectNodeToConnect(String applicationName, String key, List<NodeMetadata> nodeList) {
        return nodeList.get(0);
    }
}