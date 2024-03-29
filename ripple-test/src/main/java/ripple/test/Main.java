// Copyright (c) 2023 Institute of Software, Chinese Academy of Sciences
// Ripple is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

package ripple.test;

import ripple.client.RippleClient;
import ripple.common.entity.NodeMetadata;
import ripple.common.hashing.ModHashing;
import ripple.server.RippleServer;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Scanner;

/**
 * @author Zhen Tang
 */
public class Main {
    private static final int SERVER_COUNT = 3;
    private static final int CLIENTS_PER_SERVER = 1;
    private static final String DATABASE_PATH = "D:\\ripple-test-dir";

    public static void main(String[] args) {
        try {
            Files.createDirectories(Paths.get(DATABASE_PATH));

            List<RippleServer> serverList = new ArrayList<>();
            List<RippleClient> clientList = new ArrayList<>();
            List<NodeMetadata> nodeList = new ArrayList<>();

            int branch = 3;
            int i = 0;
            for (i = 0; i < SERVER_COUNT; i++) {
                int serverId = i + 1;
                String storageLocation = DATABASE_PATH + "\\server-" + serverId + ".db";
                RippleServer rippleServer = RippleServer.hashingBasedProtocol(serverId, storageLocation, new ModHashing());
                rippleServer.start();
                serverList.add(rippleServer);
                System.out.println("[" + SimpleDateFormat.getDateTimeInstance().format(new Date(System.currentTimeMillis())) + "] "
                        + "Node " + rippleServer.getId() + ": " + rippleServer.getAddress() + ", API port = " + rippleServer.getApiPort() + ", UI port = " + rippleServer.getUiPort());
                nodeList.add(new NodeMetadata(serverList.get(i).getId(), serverList.get(i).getAddress(), serverList.get(i).getApiPort()));
            }
            for (i = 0; i < SERVER_COUNT; i++) {
                serverList.get(i).initCluster(nodeList);
            }

            Scanner scanner = new Scanner(System.in);
            System.out.println("Complete creating servers. Press any key to continue.");
            scanner.nextLine();

            int j = 0;
            for (i = 0; i < SERVER_COUNT; i++) {
                for (j = 0; j < CLIENTS_PER_SERVER; j++) {
                    RippleServer rippleServer = serverList.get(i);
                    String storageLocation = DATABASE_PATH + "\\server-" + rippleServer.getId() + "-client-" + (j + 1) + ".db";
                    RippleClient rippleClient = new RippleClient(nodeList, storageLocation);
                    rippleClient.start();
                    clientList.add(rippleClient);
                    System.out.println("Client " + (j + 1) + " for Server " + rippleServer.getId() + ":"
                            + rippleClient.getAddress() + ":" + rippleClient.getUiPort());
                }
            }

            for (RippleClient client : clientList) {
                client.subscribe("testApp", "test");
            }

//            Thread.sleep(1000);
//            UUID baseMessageUuid = UUID.randomUUID();
//            System.out.println("Test incremental update, uuid = " + baseMessageUuid);
//            clientList.get(0).incrementalUpdate("testApp", "test", baseMessageUuid, Constants.ATOMIC_OPERATION_ADD_ENTRY, "test");

//            Thread.sleep(2000);
//            GetRequest getRequest = new GetRequest();
//            getRequest.setUuid(UUID.randomUUID());
//            getRequest.setApplicationName("testApp");
//            getRequest.setKey("test");
//            serverList.get(0).getNode().getApiServer().sendMessage(serverList.get(1).getAddress(), serverList.get(1).getApiPort(), getRequest);

            System.out.println("Press any key to stop.");
            scanner.nextLine();

            for (RippleClient rippleClient : clientList) {
                rippleClient.stop();
            }

            for (RippleServer rippleServer : serverList) {
                rippleServer.stop();
            }
        } catch (Exception exception) {
            exception.printStackTrace();
        }
    }
}
