// Copyright (c) 2024 Institute of Software, Chinese Academy of Sciences
// Ripple is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

package ripple.server.core;

import io.netty.channel.Channel;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import oshi.SystemInfo;
import oshi.hardware.CentralProcessor;
import oshi.hardware.HardwareAbstractionLayer;
import ripple.common.ClientListCache;
import ripple.common.entity.AbstractMessage;
import ripple.common.entity.ClientMetadata;
import ripple.common.entity.DeleteMessage;
import ripple.common.entity.IncrementalUpdateMessage;
import ripple.common.entity.Item;
import ripple.common.entity.NodeMetadata;
import ripple.common.entity.UpdateMessage;
import ripple.common.helper.ChineseStringTable;
import ripple.common.helper.EnglishStringTable;
import ripple.common.helper.StringTable;
import ripple.common.resolver.LastWriteWinsResolver;
import ripple.common.resolver.MessageBasedResolver;
import ripple.common.storage.Storage;
import ripple.common.storage.sqlite.SqliteStorage;
import ripple.common.tcp.message.Result;
import ripple.server.core.dispatcher.ClientDispatcher;
import ripple.server.core.dispatcher.DefaultClientDispatcher;
import ripple.server.core.dispatcher.EqualDivisionClientDispatcher;
import ripple.server.core.overlay.Overlay;
import ripple.server.helper.Api;
import ripple.server.tcp.NettyServer;
import ripple.server.ui.AddConfigServlet;
import ripple.server.ui.ClientClusterServlet;
import ripple.server.ui.Endpoint;
import ripple.server.ui.GetConfigServlet;
import ripple.server.ui.GetSubscriptionServlet;
import ripple.server.ui.HomeServlet;
import ripple.server.ui.IncrementalUpdateServlet;
import ripple.server.ui.ModifyConfigServlet;
import ripple.server.ui.RemoveConfigServlet;
import ripple.server.ui.ServerClusterServlet;
import ripple.server.ui.StyleServlet;

import javax.servlet.Servlet;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * @author Zhen Tang
 */
public class Node {
    private static final Logger LOGGER = LoggerFactory.getLogger(Node.class);

    private ExecutorService executorService;
    private Worker worker;
    private Thread workingThread;

    private int id;
    private Overlay overlay;
    private Storage storage;
    private List<NodeMetadata> nodeList;
    private ConcurrentHashMap<Item, Set<ClientMetadata>> subscription;
    private Set<ClientMetadata> connectedClients;

    private Tracker tracker;
    private HealthManager healthManager;
    private ClientListCache clientListCache;
    private ClientDispatcher clientDispatcher;

    private String address;
    private int uiPort;
    private int apiPort;
    private Server uiServer;
    private NettyServer apiServer;
    private boolean running;

    private double currentCpuLoad;

    private StringTable stringTable;
    private MessageBasedResolver resolver;

    public ExecutorService getExecutorService() {
        return executorService;
    }

    public void setExecutorService(ExecutorService executorService) {
        this.executorService = executorService;
    }

    public Worker getWorker() {
        return worker;
    }

    private void setWorker(Worker worker) {
        this.worker = worker;
    }

    public Thread getWorkingThread() {
        return workingThread;
    }

    private void setWorkingThread(Thread workingThread) {
        this.workingThread = workingThread;
    }

    public int getId() {
        return id;
    }

    private void setId(int id) {
        this.id = id;
    }

    public Overlay getOverlay() {
        return overlay;
    }

    public void setOverlay(Overlay overlay) {
        this.overlay = overlay;
    }

    public Storage getStorage() {
        return storage;
    }

    private void setStorage(Storage storage) {
        this.storage = storage;
    }

    public List<NodeMetadata> getNodeList() {
        return nodeList;
    }

    public void setNodeList(List<NodeMetadata> nodeList) {
        this.nodeList = nodeList;
    }

    public ConcurrentHashMap<Item, Set<ClientMetadata>> getSubscription() {
        return subscription;
    }

    public void setSubscription(ConcurrentHashMap<Item, Set<ClientMetadata>> subscription) {
        this.subscription = subscription;
    }

    public Set<ClientMetadata> getConnectedClients() {
        return connectedClients;
    }

    public void setConnectedClients(Set<ClientMetadata> connectedClients) {
        this.connectedClients = connectedClients;
    }

    public Tracker getTracker() {
        return tracker;
    }

    private void setTracker(Tracker tracker) {
        this.tracker = tracker;
    }

    public HealthManager getHealthManager() {
        return healthManager;
    }

    private void setHealthManager(HealthManager healthManager) {
        this.healthManager = healthManager;
    }

    public ClientListCache getClientListCache() {
        return clientListCache;
    }

    public void setClientListCache(ClientListCache clientListCache) {
        this.clientListCache = clientListCache;
    }

    public ClientDispatcher getClientDispatcher() {
        return clientDispatcher;
    }

    public void setClientDispatcher(ClientDispatcher clientDispatcher) {
        this.clientDispatcher = clientDispatcher;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public int getUiPort() {
        return uiPort;
    }

    public void setUiPort(int uiPort) {
        this.uiPort = uiPort;
    }

    public int getApiPort() {
        return apiPort;
    }

    public void setApiPort(int apiPort) {
        this.apiPort = apiPort;
    }

    public Server getUiServer() {
        return uiServer;
    }

    public void setUiServer(Server uiServer) {
        this.uiServer = uiServer;
    }

    public NettyServer getApiServer() {
        return apiServer;
    }

    public void setApiServer(NettyServer apiServer) {
        this.apiServer = apiServer;
    }

    public boolean isRunning() {
        return running;
    }

    public void setRunning(boolean running) {
        this.running = running;
    }

    public double getCurrentCpuLoad() {
        return currentCpuLoad;
    }

    public void setCurrentCpuLoad(double currentCpuLoad) {
        this.currentCpuLoad = currentCpuLoad;
    }

    public StringTable getStringTable() {
        return stringTable;
    }

    private void setStringTable(StringTable stringTable) {
        this.stringTable = stringTable;
    }

    public MessageBasedResolver getResolver() {
        return resolver;
    }

    public void setResolver(MessageBasedResolver resolver) {
        this.resolver = resolver;
    }

    public Node(int id, Overlay overlay, String storageLocation, String address) {
        this(id, overlay, storageLocation, address, 0, 0);
    }

    public Node(int id, Overlay overlay, String storageLocation, String address, int apiPort, int uiPort) {
        this(id, overlay, storageLocation, address, apiPort, uiPort, "english");
    }

    public Node(int id, Overlay overlay, String storageLocation, String address, int apiPort, int uiPort, String language) {
        this.setExecutorService(Executors.newCachedThreadPool());
        this.setId(id);
        this.setOverlay(overlay);
        this.setTracker(new Tracker(this, overlay));
        this.setStorage(new SqliteStorage(storageLocation, 3));
        this.setHealthManager(new HealthManager(this));
        this.setWorker(new Worker(this));

        this.setNodeList(new ArrayList<>());
        this.setSubscription(new ConcurrentHashMap<>());
        this.setAddress(address);
        this.setApiPort(apiPort);
        this.setUiPort(uiPort);
        this.setConnectedClients(new HashSet<>());
        this.setClientListCache(new ClientListCache());
        // this.setClientDispatcher(new EqualDivisionClientDispatcher(this));
        this.setClientDispatcher(new DefaultClientDispatcher(this));
        this.updateCpuLoad(1000);

        if (language.equalsIgnoreCase("chinese")) {
            this.setStringTable(new ChineseStringTable());
        } else {
            this.setStringTable(new EnglishStringTable());
        }

        this.setResolver(new LastWriteWinsResolver());
    }

    private void registerServlet(ServletContextHandler servletContextHandler, Servlet servlet, String endpoint) {
        servletContextHandler.addServlet(new ServletHolder(servlet), endpoint);
    }

    public void registerHandlers(ServletContextHandler servletContextHandler) {
        // UI
        this.registerServlet(servletContextHandler, new HomeServlet(this), Endpoint.UI_HOME);
        this.registerServlet(servletContextHandler, new StyleServlet(this), Endpoint.UI_STYLE);
        this.registerServlet(servletContextHandler, new GetConfigServlet(this), Endpoint.UI_GET_CONFIG);
        this.registerServlet(servletContextHandler, new AddConfigServlet(this), Endpoint.UI_ADD_CONFIG);
        this.registerServlet(servletContextHandler, new ModifyConfigServlet(this), Endpoint.UI_MODIFY_CONFIG);
        this.registerServlet(servletContextHandler, new IncrementalUpdateServlet(this), Endpoint.UI_INCREMENTAL_UPDATE);
        this.registerServlet(servletContextHandler, new RemoveConfigServlet(this), Endpoint.UI_REMOVE_CONFIG);
        this.registerServlet(servletContextHandler, new GetSubscriptionServlet(this), Endpoint.UI_GET_SUBSCRIPTION);
        this.registerServlet(servletContextHandler, new ServerClusterServlet(this), Endpoint.UI_SERVER_CLUSTER);
        this.registerServlet(servletContextHandler, new ClientClusterServlet(this), Endpoint.UI_CLIENT_CLUSTER);
    }

    public Item get(String applicationName, String key) {
        return this.getStorage().getItemService().getItem(applicationName, key);
    }

    public Item getWithValue(String applicationName, String key) {
        Item item = this.get(applicationName, key);
        this.getResolver().merge(item, this.getStorage().getMessageService().findMessages(item.getApplicationName(), item.getKey()));
        return item;
    }

    public List<Item> getAll() {
        return this.getStorage().getItemService().getAllItems();
    }

    public boolean put(String applicationName, String key, String value) {
        Date lastUpdate = new Date(System.currentTimeMillis());
        int lastUpdateServerId = this.getId();

        UpdateMessage message = new UpdateMessage(applicationName, key, value, lastUpdate, lastUpdateServerId);
        message.setFromId(this.getId());
        Result result = this.propagateMessage(message);

        return (result == Result.SUCCESS);
    }

    public boolean incrementalUpdate(String applicationName, String key, UUID baseMessageUuid, String atomicOperation, String value) {
        Date lastUpdate = new Date(System.currentTimeMillis());
        int lastUpdateServerId = this.getId();

        IncrementalUpdateMessage message = new IncrementalUpdateMessage(applicationName, key
                , baseMessageUuid, atomicOperation, value, lastUpdate, lastUpdateServerId);
        message.setFromId(this.getId());
        Result result = this.propagateMessage(message);

        return (result == Result.SUCCESS);
    }

    public boolean delete(String applicationName, String key) {
        Date lastUpdate = new Date(System.currentTimeMillis());
        int lastUpdateServerId = this.getId();

        DeleteMessage message = new DeleteMessage(applicationName, key, lastUpdate, lastUpdateServerId);
        message.setFromId(this.getId());
        Result result = this.propagateMessage(message);

        return (result == Result.SUCCESS);
    }

    public Result propagateMessage(final AbstractMessage message) {
        // Check duplicated messages
        if (this.getStorage().getMessageService().exist(message.getUuid())) {
            return Result.DUPLICATED;
        }

        // Update local storage
        this.applyMessageToStorage(message);

        // Init ACK if it is the message source
        if (message.getLastUpdateServerId() == this.getId()) {
            this.getTracker().initProgress(message);
        }

        // Notify clients
        this.doNotifyClients(message);

        final int sourceId = message.getLastUpdateServerId();
        final int fromId = message.getFromId();
        final int currentId = this.getId();

        // this.doSyncWithServer(message, sourceId, currentId);

        this.getExecutorService().submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                try {
                    doSyncWithServer(message, sourceId, fromId, currentId);
                    return null;
                } catch (Exception exception) {
                    exception.printStackTrace();
                    return null;
                }
            }
        });

        return Result.SUCCESS;
    }

    public NodeMetadata findServerById(int serverId) {
        for (NodeMetadata nodeMetadata : this.getNodeList()) {
            if (nodeMetadata.getId() == serverId) {
                return nodeMetadata;
            }
        }
        return null;
    }

    public NodeMetadata findServerByAddress(String address, int port) {
        for (NodeMetadata nodeMetadata : this.getNodeList()) {
            if (nodeMetadata.getAddress().equals(address)
                    && nodeMetadata.getPort() == port) {
                return nodeMetadata;
            }
        }
        return null;
    }

    private void doSyncWithServer(AbstractMessage message, int sourceId, int fromId, int currentId) {
        NodeMetadata source = this.findServerById(sourceId);
        NodeMetadata from = this.findServerById(fromId);
        NodeMetadata current = this.findServerById(currentId);
        List<NodeMetadata> initialList = this.getOverlay().calculateNodesToSync(message, source, from, current);
        Queue<NodeMetadata> sendQueue = new LinkedBlockingDeque<>(initialList);

        // Sync to servers following the overlay
        while (!sendQueue.isEmpty()) {
            // Fault tolerant
            NodeMetadata nodeMetadata = sendQueue.poll();
            if (this.getHealthManager().isAlive(nodeMetadata)) {
                LOGGER.info("[Node-{}] Sync {} with server {}:{}.", this.getId(), message.getType(), nodeMetadata.getAddress(), nodeMetadata.getPort());
                Channel channel = this.getApiServer().findChannel(nodeMetadata.getAddress(), nodeMetadata.getPort());
                if (channel == null) {
                    LOGGER.info("[Node-{}] Node-{} ({}:{}) is unreachable, skipping."
                            , this.getId(), nodeMetadata.getId(), nodeMetadata.getAddress(), nodeMetadata.getPort());
                } else {
                    // Replace fromId
                    message.setFromId(this.getId());
                    Api.sync(channel, message);
                    LOGGER.info("[Node-{}] Record ACK of message {} from server {}.", this.getId(), message.getUuid(), nodeMetadata.getId());
                    this.getTracker().recordAck(message.getUuid(), message.getLastUpdateServerId(), nodeMetadata.getId());
                }
            } else {
                LOGGER.info("[Node-{}] Server {}:{} (id = {}) is unreachable, attempting to send to its children."
                        , this.getId(), nodeMetadata.getAddress(), nodeMetadata.getPort(), nodeMetadata.getId());
                List<NodeMetadata> list = this.getOverlay().calculateNodesToSync(message, source, from, nodeMetadata);
                sendQueue.addAll(list);
            }
        }
    }

    private void doNotifyClients(AbstractMessage message) {
        Item item = new Item(message.getApplicationName(), message.getKey());
        if (this.getSubscription().containsKey(item)) {
            Set<ClientMetadata> clients = this.getSubscription().get(item);
            this.getClientDispatcher().notifyClients(clients, message);
        }
    }

    private void applyMessageToStorage(AbstractMessage message) {
        String applicationName = message.getApplicationName();
        String key = message.getKey();
        Item item = this.getStorage().getItemService().getItem(applicationName, key);
        if (item == null) {
            this.getStorage().getItemService().newItem(applicationName, key);
        }
        this.getStorage().getMessageService().newMessage(message);
    }


    public synchronized boolean start() {
        if (this.isRunning()) {
            return true;
        }
        try {
            this.setApiServer(new NettyServer(this, this.getAddress(), this.getApiPort()));
            this.getApiServer().start();
            this.setApiPort(this.getApiServer().getPort());

            this.setUiServer(new Server());
            ServerConnector serverConnector = new ServerConnector(this.getUiServer());
            serverConnector.setPort(this.getUiPort());
            this.getUiServer().setConnectors(new Connector[]{serverConnector});
            ServletContextHandler servletContextHandler = new ServletContextHandler();
            this.registerHandlers(servletContextHandler);
            this.getUiServer().setHandler(servletContextHandler);
            this.getUiServer().start();
            this.setUiPort(serverConnector.getLocalPort());
            if (this.getExecutorService() == null || this.getExecutorService().isShutdown()) {
                this.setExecutorService(Executors.newCachedThreadPool());
            }

            this.setRunning(true);
            return true;
        } catch (Exception exception) {
            exception.printStackTrace();
            return false;
        }
    }

    public synchronized boolean stop() {
        if (!this.isRunning()) {
            return true;
        }

        try {
            this.getApiServer().stop();
            this.getUiServer().stop();
            this.getWorkingThread().interrupt();

            this.executorService.shutdown();

            this.setRunning(false);
            return true;
        } catch (Exception exception) {
            exception.printStackTrace();
            return false;
        }
    }

    public synchronized void subscribe(String callbackAddress, int callbackPort, String applicationName, String key) {
        LOGGER.info("[Node-{}] subscribe() called: Callback Address = {}, Callback Port = {}, Application Name = {}, Key = {}."
                , this.getId(), callbackAddress, callbackPort, applicationName, key);
        Item item = new Item(applicationName, key);
        if (this.getSubscription().get(item) == null) {
            this.getSubscription().put(item, new HashSet<>());
        }
        Set<ClientMetadata> subscribers = this.getSubscription().get(item);
        ClientMetadata clientMetadata = new ClientMetadata(callbackAddress, callbackPort);
        if (!subscribers.contains(clientMetadata)) {
            subscribers.add(clientMetadata);
        }
        this.getApiServer().connect(clientMetadata.getAddress(), clientMetadata.getPort());
        this.getConnectedClients().add(clientMetadata);
    }

    public synchronized void unsubscribe(String callbackAddress, int callbackPort, String applicationName, String key) {
        LOGGER.info("[Node-{}] unsubscribe() called: Callback Address = {}, Callback Port = {}, Application Name = {}, Key = {}."
                , this.getId(), callbackAddress, callbackPort, applicationName, key);
        Item item = new Item(applicationName, key);
        if (this.getSubscription().get(item) == null) {
            return;
        }
        Set<ClientMetadata> subscribers = this.getSubscription().get(item);
        ClientMetadata clientMetadata = new ClientMetadata(callbackAddress, callbackPort);
        if (subscribers.contains(clientMetadata)) {
            subscribers.remove(clientMetadata);
        }

        this.cleanUpConnectedClients(clientMetadata);
    }

    private void cleanUpConnectedClients(ClientMetadata clientMetadata) {
        boolean exist = false;
        for (Set<ClientMetadata> clients : this.getSubscription().values()) {
            if (clients.contains(clientMetadata)) {
                exist = true;
                break;
            }
        }
        if (!exist) {
            this.getConnectedClients().remove(clientMetadata);
        }
    }

    public void initCluster(List<NodeMetadata> nodeList) {
        this.setNodeList(nodeList);
        this.getHealthManager().init();
        this.getOverlay().buildOverlay(this.getNodeList());
        this.setWorkingThread(new Thread(this.getWorker()));
        this.getWorkingThread().start();
        this.reconnect(nodeList);
    }

    public synchronized void reconnect(List<NodeMetadata> nodeList) {
        for (NodeMetadata metadata : nodeList) {
            // if (metadata.getId() > this.getId()) {
            Channel channel = this.getApiServer().findChannel(metadata.getAddress(), metadata.getPort());
            if (channel == null) {
                try {
                    LOGGER.trace("[Node-{}] Reconnecting to Node-{} ({}:{})."
                            , this.getId(), metadata.getId(), metadata.getAddress(), metadata.getPort());
                    this.getApiServer().connect(metadata.getAddress(), metadata.getPort());
                } catch (Exception exception) {
                    exception.printStackTrace();
                }
            }
            // }
        }
    }

    public void updateCpuLoad(long delay) {
        SystemInfo si = new SystemInfo();
        HardwareAbstractionLayer hal = si.getHardware();
        CentralProcessor cpu = hal.getProcessor();
        double load = cpu.getSystemCpuLoad(delay);
        this.setCurrentCpuLoad(load);
    }

}
