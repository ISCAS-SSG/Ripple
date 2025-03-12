// Copyright (c) 2024 Institute of Software, Chinese Academy of Sciences
// Ripple is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

package ripple.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ripple.client.core.DefaultSelector;
import ripple.client.core.HashingBasedSelector;
import ripple.client.core.LoadBalancedSelector;
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
            LOGGER.info("[Main] Profile is {}", profile);
            String ipPrefix = "192.168.101.";
            int serverApiPort = 20000;
            int serverCount = 50;
            String selectAddress = null;
            List<NodeMetadata> nodeList = new ArrayList<>();
            List<RippleClient> clients = new ArrayList<>();
            int i = 0;
            for (i = 0; i < serverCount; i++) {
                nodeList.add(new NodeMetadata(i + 1, ipPrefix + (i + 1), serverApiPort));
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
                                break;
                            }
                        }
                    }
                    if (selectAddress != null) {
                        break;
                    }
                }
                if (selectAddress == null) {
                    LOGGER.error("[Main] Fail to find my IP address. Exiting.");
                    return;
                }
                System.out.println("[Main] address = " + selectAddress);
            } catch (SocketException exception) {
                LOGGER.error("[Main] Fail to get IP address: {}. Exiting.", exception.getLocalizedMessage());
                return;
            }

            int clientCount = 0;
            int subscriptionPerClient = 0;
            if (profile.contains("connect")) {
                clientCount = 100;
                subscriptionPerClient = 200;
            } else if (profile.contains("push")) {
                clientCount = 100;
                subscriptionPerClient = 20;
            } else {
                LOGGER.info("[Main] Invalid profile");
                return;
            }
            if (profile.contains("star")) {
                doStartClients("star", clientCount, subscriptionPerClient, serverCount, nodeList, selectAddress, clients);
            } else if (profile.contains("tree")) {
                doStartClients("tree", clientCount, subscriptionPerClient, serverCount, nodeList, selectAddress, clients);
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

            String nodeSelector = System.getProperty("nodeSelector");
            if (nodeSelector == null) {
                LOGGER.info("[Main] Missing parameter: nodeSelector (-DnodeSelector). Using HASHING selector.");
                nodeSelector = "hashing";
            }
            if (!nodeSelector.equals("hashing") && !nodeSelector.equals("loadbalance")) {
                LOGGER.info("[Main] Incorrect parameter value: nodeSelector (-DnodeSelector). Allowed: hashing, loadbalance. Using HASHING selector.");
                nodeSelector = "hashing";
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

            List<NodeMetadata> nodeList = new ArrayList<>();
            String[] nodeAddress = nodes.split(",");
            for (String item : nodeAddress) {
                String[] result = item.split(":");
                NodeMetadata metadata = new NodeMetadata(Integer.parseInt(result[0]), result[1], Integer.parseInt(result[2]));
                nodeList.add(metadata);
            }

            RippleClient client = null;
            if (nodeSelector.equals("hashing")) {
                String divisor = System.getProperty("divisor");
                String candidateCount = System.getProperty("candidateCount");
                if (divisor == null) {
                    String defaultDivisor = "200";
                    LOGGER.info("[Main] Missing parameter: divisor for HASHING selector (-Ddivisor). Using default: " + defaultDivisor);
                    divisor = defaultDivisor;
                }
                if (candidateCount == null) {
                    String defaultCandidateCount = "3";
                    LOGGER.info("[Main] Missing parameter: candidateCount for HASHING selector (-DcandidateCount). Using default: " + defaultCandidateCount);
                    candidateCount = defaultCandidateCount;
                }
                client = new RippleClient(nodeList, new HashingBasedSelector(new ModHashing(Integer.parseInt(candidateCount), Integer.parseInt(divisor))), storageLocation, address);
            } else if (nodeSelector.equals("loadbalance")) {
                String divisor = System.getProperty("divisor");
                String candidateCount = System.getProperty("candidateCount");
                String backupRatio = System.getProperty("backupRatio");
                String cpuThreshold = System.getProperty("cpuThreshold");
                if (divisor == null) {
                    String defaultDivisor = "200";
                    LOGGER.info("[Main] Missing parameter: divisor for LOAD_BALANCED selector (-Ddivisor). Using default: " + defaultDivisor);
                    divisor = defaultDivisor;
                }
                if (candidateCount == null) {
                    String defaultCandidateCount = "3";
                    LOGGER.info("[Main] Missing parameter: candidateCount for LOAD_BALANCED selector (-DcandidateCount). Using default: " + defaultCandidateCount);
                    candidateCount = defaultCandidateCount;
                }
                if (backupRatio == null) {
                    String defaultBackupRatio = "0.5";
                    LOGGER.info("[Main] Missing parameter: backupRatio for LOAD_BALANCED selector (-DbackupRatio). Using default: " + defaultBackupRatio);
                    backupRatio = defaultBackupRatio;
                }
                if (cpuThreshold == null) {
                    String defaultCpuThreshold = "0.8";
                    LOGGER.info("[Main] Missing parameter: cpuThreshold for LOAD_BALANCED selector (-DcpuThreshold). Using default: " + defaultCpuThreshold);
                    cpuThreshold = defaultCpuThreshold;
                }
                LoadBalancedSelector selector = new LoadBalancedSelector(new ModHashing(Integer.parseInt(candidateCount), Integer.parseInt(divisor))
                        , Double.parseDouble(backupRatio), Double.parseDouble(cpuThreshold));
                client = new RippleClient(nodeList, selector, storageLocation, address);
                selector.setRippleClient(client);
            }
            client.setUiPort(Integer.parseInt(uiPort));
            client.setApiPort(Integer.parseInt(apiPort));
            client.start();
            LOGGER.info("[Main] Ripple Client started. The API port is {}. The UI port is {}. The storage location is {}."
                    , client.getApiPort(), client.getUiPort(), client.getStorage().getLocation());
            LOGGER.info("[Main] Nodes in the server cluster:");
            for (NodeMetadata nodeMetadata : client.getNodeList()) {
                LOGGER.info("[Main] --> Id = {}, Address = {}, API port = {}", nodeMetadata.getId(), nodeMetadata.getAddress(), nodeMetadata.getPort());
            }
        }
    }

    private static void doStartClients(String protocol, int clientCount, int subscriptionCount, int serverCount, List<NodeMetadata> nodeList, String selectAddress, List<RippleClient> clients) {
        int i;
        List<List<NodeMetadata>> subList = new ArrayList<>();
        for (i = 0; i < serverCount; i++) {
            subList.add(nodeList.subList(i, i + 1));
        }
        System.out.println("[Main] [" + selectAddress + "] Creating clients");
        for (i = 0; i < clientCount; i++) {
            int listIndex = i % serverCount;
            String storageLocation = "client-" + (i + 1) + "-" + protocol + ".sqlite";
            RippleClient client = null;
            List<NodeMetadata> list = subList.get(listIndex);
            client = new RippleClient(list, new DefaultSelector(), storageLocation, selectAddress, "chinese");
            clients.add(client);
            System.out.println("[Main] [" + selectAddress + "] " + (i + 1) + " / " + clientCount + " clients created.");
        }

        for (i = 0; i < clients.size(); i++) {
            clients.get(i).setUiPort(0);
            clients.get(i).setApiPort(0);
            clients.get(i).start();
        }

        System.out.println("[Main] [" + selectAddress + "] Subscribing...");
        String applicationName = "testApp";
        String key = "testKey";
        int j = 0;
        for (i = 0; i < clients.size(); i++) {
            for (j = 0; j < subscriptionCount; j++) {
                clients.get(i).findOrConnectToServer(applicationName + "-" + (j + 1), key + "-" + (j + 1));
                clients.get(i).subscribe(applicationName + "-" + (j + 1), key + "-" + (j + 1));
            }
        }
        System.out.println("[Main] [" + selectAddress + "] Done");
    }
}
