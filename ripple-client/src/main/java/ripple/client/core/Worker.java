// Copyright (c) 2023 Institute of Software, Chinese Academy of Sciences
// Ripple is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

package ripple.client.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ripple.client.RippleClient;
import ripple.common.entity.NodeMetadata;

/**
 * @author Zhen Tang
 */
public class Worker implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(Worker.class);
    private static final int INTERVAL = 10000;

    private RippleClient rippleClient;

    public RippleClient getRippleClient() {
        return rippleClient;
    }

    public void setRippleClient(RippleClient rippleClient) {
        this.rippleClient = rippleClient;
    }

    public Worker(RippleClient rippleClient) {
        this.setRippleClient(rippleClient);
    }

    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted() && this.getRippleClient().isRunning()) {
            try {
//                LOGGER.info("[Worker] Gathering CPU usage of servers for every {} ms.", INTERVAL);
//                for (NodeMetadata nodeMetadata : this.getRippleClient().getNodeList()) {
//                    this.getRippleClient().systemInfo(nodeMetadata.getId());
//                }
                Thread.sleep(INTERVAL);
            } catch (InterruptedException exception) {
                LOGGER.info("[Worker] Interrupted.");
                return;
            } catch (Exception exception) {
                exception.printStackTrace();
            }
        }
    }
}
