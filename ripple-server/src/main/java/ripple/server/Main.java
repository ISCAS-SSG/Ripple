// Copyright (c) 2023 Institute of Software, Chinese Academy of Sciences
// Ripple is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

package ripple.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ripple.common.entity.NodeMetadata;
import ripple.common.hashing.ModHashing;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

/**
 * @author Zhen Tang
 */
public class Main {
    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        String profile = System.getProperty("profile");

        if (profile != null) {
            String ipPrefix = "192.168.101.";
            int serverCount = 50;
            String selectAddress = null;
            int myId = -1;
            int apiPort = 20000;
            int uiPort = 20001;
            String language = "chinese";
            String storageLocation = "database.sqlite";
            System.out.println("[Main] Profile is " + profile);
            List<NodeMetadata> nodeList = new ArrayList<>();
            int i = 0;
            for (i = 0; i < serverCount; i++) {
                nodeList.add(new NodeMetadata(i + 1, ipPrefix + (i + 1), apiPort));
            }
            try {
                Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
                while (interfaces.hasMoreElements()) {
                    NetworkInterface networkInterface = interfaces.nextElement();
                    Enumeration<InetAddress> addresses = networkInterface.getInetAddresses();
                    while (addresses.hasMoreElements()) {
                        InetAddress address = addresses.nextElement();
                        if (!address.isLoopbackAddress() && !address.isLinkLocalAddress() && address.isSiteLocalAddress()) {
                            String ipAddress = address.getHostAddress();
                            System.out.println("[Main] Found IP address: " + ipAddress);
                            if (ipAddress.startsWith(ipPrefix)) {
                                System.out.println("[Main] Select IP address: " + ipAddress);
                                selectAddress = ipAddress;
                                myId = Integer.parseInt(selectAddress.substring(ipPrefix.length()));
                                break;
                            }
                        }
                    }
                    if (selectAddress != null) {
                        break;
                    }
                }
                if (selectAddress == null || myId == -1) {
                    LOGGER.error("[Main] Fail to find my id and IP address. Exiting.");
                    return;
                }
                System.out.println("[Main] id = " + myId + ", address = " + selectAddress);
            } catch (SocketException exception) {
                LOGGER.error("[Main] Fail to get IP address: {}. Exiting.", exception.getLocalizedMessage());
                return;
            }
            RippleServer server = null;
            if (profile.equals("star")) {
                server = RippleServer.starProtocol(myId, storageLocation, selectAddress, apiPort, uiPort, language);
            } else if (profile.equals("tree")) {
                int branch = 3;
                server = RippleServer.treeProtocol(myId, storageLocation, selectAddress, apiPort, uiPort, branch, language);
            } else {
                LOGGER.error("[Main] Invalid profile. Exiting");
            }

            if (server != null) {
                doStartRippleServer(nodeList, server);
                System.out.println("[Main] Server " + myId + " started.");
            } else {
                LOGGER.error("[Main] Fail to start server.");
            }

        } else {
            LOGGER.info("[Main] Missing parameter: profile (-Dprofile). Switching to normal mode.");

            String storageLocation = System.getProperty("storageLocation");
            if (storageLocation == null) {
                String defaultStorageLocation = "database.sqlite";
                LOGGER.info("[Main] Missing parameter: storageLocation (-DstorageLocation). Using default: {}", defaultStorageLocation);
                storageLocation = defaultStorageLocation;
            }

            String apiPort = System.getProperty("apiPort");
            if (apiPort == null) {
                LOGGER.info("[Main] Missing parameter: API port (-DapiPort). Using random port.");
                apiPort = "0";
            }

            String uiPort = System.getProperty("uiPort");
            if (uiPort == null) {
                LOGGER.info("[Main] Missing parameter: UI port (-DuiPort). Using random port.");
                uiPort = "0";
            }

            String protocol = System.getProperty("protocol");
            if (protocol == null) {
                LOGGER.info("[Main] Missing parameter: protocol (-Dprotocol). Using STAR protocol.");
                protocol = "star";
            }

            String id = System.getProperty("id");
            if (id == null) {
                LOGGER.info("[Main] Missing parameter: id (-Did).");
                return;
            }

            String nodes = System.getProperty("nodes");
            if (nodes == null) {
                LOGGER.info("[Main] Missing parameter: nodes (-Dnodes).");
                return;
            }

            String address = System.getProperty("address");
            if (address == null) {
                LOGGER.info("[Main] Missing parameter: address (-Daddress).");
                return;
            }

            RippleServer server = null;
            if (protocol.equals("tree")) {
                String branch = System.getProperty("branch");
                if (branch == null) {
                    String defaultBranch = "3";
                    LOGGER.info("[Main] Missing parameter: branch for TREE protocol (-Dbranch). Using default: {}", defaultBranch);
                    branch = defaultBranch;
                }
                server = RippleServer.treeProtocol(Integer.parseInt(id), storageLocation, address, Integer.parseInt(apiPort), Integer.parseInt(uiPort), Integer.parseInt(branch));
            } else if (protocol.equals("star")) {
                server = RippleServer.starProtocol(Integer.parseInt(id), storageLocation, address, Integer.parseInt(apiPort), Integer.parseInt(uiPort));
            } else if (protocol.equals("hashing")) {
                String divisor = System.getProperty("divisor");
                String candidateCount = System.getProperty("candidateCount");
                if (divisor == null) {
                    String defaultDivisor = "200";
                    LOGGER.info("[Main] Missing parameter: divisor for HASHING protocol (-Ddivisor). Using default: {}", defaultDivisor);
                    divisor = defaultDivisor;
                }
                if (candidateCount == null) {
                    String defaultCandidateCount = "3";
                    LOGGER.info("[Main] Missing parameter: candidateCount for HASHING protocol (-DcandidateCount). Using default: {}", defaultCandidateCount);
                    candidateCount = defaultCandidateCount;
                }
                server = RippleServer.hashingBasedProtocol(Integer.parseInt(id), storageLocation, address, Integer.parseInt(apiPort)
                        , Integer.parseInt(uiPort), new ModHashing(Integer.parseInt(candidateCount), Integer.parseInt(divisor)));
            }

            if (server != null) {
                List<NodeMetadata> nodeList = new ArrayList<>();
                String[] nodeAddress = nodes.split(",");
                for (String item : nodeAddress) {
                    String[] result = item.split(":");
                    NodeMetadata metadata = new NodeMetadata(Integer.parseInt(result[0]), result[1], Integer.parseInt(result[2]));
                    nodeList.add(metadata);
                }
                doStartRippleServer(nodeList, server);
                System.out.println("[Main] Server " + id + " started.");
            } else {
                LOGGER.error("[Main] Fail to start server.");
            }
        }
    }

    private static void doStartRippleServer(List<NodeMetadata> nodeList, RippleServer server) {
        server.getNode().setNodeList(nodeList);
        server.start();
        LOGGER.info("[Main] Ripple Server started. The API port is {}. The UI port is {}. The storage location is {}."
                , server.getApiPort(), server.getUiPort(), server.getNode().getStorage().getLocation());
        LOGGER.info("[Main] Nodes in the cluster:");
        for (NodeMetadata nodeMetadata : server.getNode().getNodeList()) {
            LOGGER.info("[Main] --> Id = {}, Address = {}, API port = {}", nodeMetadata.getId(), nodeMetadata.getAddress(), nodeMetadata.getPort());
        }
        server.initCluster(server.getNode().getNodeList());
    }
}
