// Copyright (c) 2023 Institute of Software, Chinese Academy of Sciences
// Ripple is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

package ripple.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ripple.client.core.HashingBasedSelector;
import ripple.client.core.LoadBalancedSelector;
import ripple.client.core.NodeSelector;
import ripple.client.core.Worker;
import ripple.client.core.tcp.ClientChannelInitializer;
import ripple.client.core.ui.AddConfigServlet;
import ripple.client.core.ui.AddSubscriptionServlet;
import ripple.client.core.ui.Endpoint;
import ripple.client.core.ui.GetConfigServlet;
import ripple.client.core.ui.GetSubscriptionServlet;
import ripple.client.core.ui.HomeServlet;
import ripple.client.core.ui.IncrementalUpdateServlet;
import ripple.client.core.ui.ModifyConfigServlet;
import ripple.client.core.ui.RemoveConfigServlet;
import ripple.client.core.ui.RemoveSubscriptionServlet;
import ripple.client.core.ui.ServerInfoServlet;
import ripple.client.core.ui.StyleServlet;
import ripple.client.helper.Api;
import ripple.common.ClientListCache;
import ripple.common.entity.AbstractMessage;
import ripple.common.entity.ClientMetadata;
import ripple.common.entity.Item;
import ripple.common.entity.NodeMetadata;
import ripple.common.hashing.ModHashing;
import ripple.common.helper.ChineseStringTable;
import ripple.common.helper.EnglishStringTable;
import ripple.common.helper.StringTable;
import ripple.common.resolver.LastWriteWinsResolver;
import ripple.common.resolver.MessageBasedResolver;
import ripple.common.storage.Storage;
import ripple.common.storage.sqlite.SqliteStorage;

import javax.servlet.Servlet;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

/**
 * @author Zhen Tang
 */
public class RippleClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(RippleClient.class);
    private Worker worker;
    private Thread workingThread;
    private Storage storage;
    private String address;
    private int uiPort;
    private int apiPort;

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel serverChannel;
    private List<Channel> connectedNodes;

    private Server server;
    private boolean running;
    private List<NodeMetadata> nodeList;
    private Map<Item, NodeMetadata> mappingCache;
    private Map<Item, NodeMetadata> subscriptions;
    private Map<NodeMetadata, Channel> serverConnections;
    private Map<ClientMetadata, Channel> clientConnections;
    private NodeSelector nodeSelector;
    private ClientListCache clientListCache;
    private Map<String, Queue<AbstractMessage>> pendingMessages;
    private Map<NodeMetadata, Double> serverCpuUsage;
    private StringTable stringTable;
    private MessageBasedResolver resolver;

    public RippleClient(List<NodeMetadata> nodeList, NodeSelector nodeSelector, String storageLocation, String address, String language) {
        this.setStorage(new SqliteStorage(storageLocation, 3));
        this.setRunning(false);
        this.setNodeList(nodeList);
        this.setMappingCache(new ConcurrentHashMap<>());
        this.setSubscriptions(new ConcurrentHashMap<>());
        this.setServerConnections(new ConcurrentHashMap<>());
        this.setClientConnections(new ConcurrentHashMap<>());
        this.setNodeSelector(nodeSelector);
        this.setClientListCache(new ClientListCache());
        this.setPendingMessages(new ConcurrentHashMap<>());
        this.setWorker(new Worker(this));
        this.setServerCpuUsage(new ConcurrentHashMap<>());
        this.setAddress(address);
        this.setApiPort(0);
        this.setUiPort(0);
        if (language.equalsIgnoreCase("chinese")) {
            this.setStringTable(new ChineseStringTable());
        } else {
            this.setStringTable(new EnglishStringTable());
        }
        this.setResolver(new LastWriteWinsResolver());
    }

    public RippleClient(List<NodeMetadata> nodeList, NodeSelector nodeSelector, String storageLocation, String address) {
        this(nodeList, nodeSelector, storageLocation, address, "english");
    }

    public RippleClient(List<NodeMetadata> nodeList, String storageLocation, String address) {
        this(nodeList, new HashingBasedSelector(new ModHashing()), storageLocation, address);
    }

    public Storage getStorage() {
        return storage;
    }

    public void setStorage(Storage storage) {
        this.storage = storage;
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

    public EventLoopGroup getBossGroup() {
        return bossGroup;
    }

    public void setBossGroup(EventLoopGroup bossGroup) {
        this.bossGroup = bossGroup;
    }

    public EventLoopGroup getWorkerGroup() {
        return workerGroup;
    }

    public void setWorkerGroup(EventLoopGroup workerGroup) {
        this.workerGroup = workerGroup;
    }

    public Channel getServerChannel() {
        return serverChannel;
    }

    public void setServerChannel(Channel serverChannel) {
        this.serverChannel = serverChannel;
    }

    public List<Channel> getConnectedNodes() {
        return connectedNodes;
    }

    public void setConnectedNodes(List<Channel> connectedNodes) {
        this.connectedNodes = connectedNodes;
    }

    public Server getServer() {
        return server;
    }

    public void setServer(Server server) {
        this.server = server;
    }

    public boolean isRunning() {
        return running;
    }

    public void setRunning(boolean running) {
        this.running = running;
    }

    public List<NodeMetadata> getNodeList() {
        return nodeList;
    }

    public void setNodeList(List<NodeMetadata> nodeList) {
        this.nodeList = nodeList;
    }

    public Map<Item, NodeMetadata> getMappingCache() {
        return mappingCache;
    }

    public void setMappingCache(Map<Item, NodeMetadata> mappingCache) {
        this.mappingCache = mappingCache;
    }

    public Map<Item, NodeMetadata> getSubscriptions() {
        return subscriptions;
    }

    public void setSubscriptions(Map<Item, NodeMetadata> subscriptions) {
        this.subscriptions = subscriptions;
    }

    public Map<NodeMetadata, Channel> getServerConnections() {
        return serverConnections;
    }

    public void setServerConnections(Map<NodeMetadata, Channel> serverConnections) {
        this.serverConnections = serverConnections;
    }

    public Map<ClientMetadata, Channel> getClientConnections() {
        return clientConnections;
    }

    public void setClientConnections(Map<ClientMetadata, Channel> clientConnections) {
        this.clientConnections = clientConnections;
    }

    public NodeSelector getNodeSelector() {
        return nodeSelector;
    }

    public void setNodeSelector(NodeSelector nodeSelector) {
        this.nodeSelector = nodeSelector;
    }

    public ClientListCache getClientListCache() {
        return clientListCache;
    }

    public void setClientListCache(ClientListCache clientListCache) {
        this.clientListCache = clientListCache;
    }

    public Map<String, Queue<AbstractMessage>> getPendingMessages() {
        return pendingMessages;
    }

    public void setPendingMessages(Map<String, Queue<AbstractMessage>> pendingMessages) {
        this.pendingMessages = pendingMessages;
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

    public Map<NodeMetadata, Double> getServerCpuUsage() {
        return serverCpuUsage;
    }

    private void setServerCpuUsage(Map<NodeMetadata, Double> serverCpuUsage) {
        this.serverCpuUsage = serverCpuUsage;
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

    public Item get(String applicationName, String key) {
        if (!this.isRunning()) {
            this.start();
        }
        return this.refreshItem(applicationName, key);
    }

    public Item getWithValue(String applicationName, String key) {
        Item item = this.get(applicationName, key);
        this.getResolver().merge(item, this.getStorage().getMessageService().findMessages(item.getApplicationName(), item.getKey()));
        return item;
    }


    public void fullyGet(String applicationName, String key) {
        if (!this.isRunning()) {
            this.start();
        }
        Channel channel = this.findOrConnectToServer(applicationName, key);
        Api.getAsync(channel, applicationName, key);
    }

    private Item refreshItem(String applicationName, String key) {
        if (!this.isRunning()) {
            this.start();
        }
        Channel channel = this.findOrConnectToServer(applicationName, key);
        Item item = this.getStorage().getItemService().getItem(applicationName, key);
        if (item == null) {
            Api.getAsync(channel, applicationName, key);
        }
        return item;
    }

    public void put(String applicationName, String key, String value) {
        if (!this.isRunning()) {
            this.start();
        }
        Channel channel = this.findOrConnectToServer(applicationName, key);
        Api.putAsync(channel, applicationName, key, value);
        // this.refreshItem(applicationName, key);
    }

    public void delete(String applicationName, String key) {
        if (!this.isRunning()) {
            this.start();
        }
        Channel channel = this.findOrConnectToServer(applicationName, key);
        Api.deleteAsync(channel, applicationName, key);
        // this.refreshItem(applicationName, key);
    }

    public void incrementalUpdate(String applicationName, String key, UUID baseMessageUuid
            , String atomicOperation, String value) {
        if (!this.isRunning()) {
            this.start();
        }
        Channel channel = this.findOrConnectToServer(applicationName, key);
        Api.incrementalUpdateAsync(channel, applicationName, key, baseMessageUuid, atomicOperation, value);
        // this.refreshItem(applicationName, key);
    }

    public void subscribe(String applicationName, String key) {
        if (!this.isRunning()) {
            this.start();
        }
        Channel channel = this.findOrConnectToServer(applicationName, key);
        Item item = new Item(applicationName, key);
        this.getSubscriptions().put(item, this.getMappingCache().get(item));
        Api.subscribeAsync(channel, applicationName, key, this.getAddress(), this.getApiPort());
    }

    public void unsubscribe(String applicationName, String key) {
        if (!this.isRunning()) {
            this.start();
        }
        Channel channel = this.findOrConnectToServer(applicationName, key);
        this.getSubscriptions().remove(new Item(applicationName, key));
        Api.unsubscribeAsync(channel, applicationName, key, this.getAddress(), this.getApiPort());
    }

    public void systemInfo(int serverId) {
        if (!this.isRunning()) {
            this.start();
        }
        Channel channel = this.findOrConnectToServer(serverId);
        Api.systemInfoAsync(channel);
    }

    public NodeMetadata findServerByAddress(Channel channel) {
        for (NodeMetadata metadata : this.getServerConnections().keySet()) {
            if (this.getServerConnections().get(metadata) == channel) {
                return metadata;
            }
        }
        return null;
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
        this.registerServlet(servletContextHandler, new AddSubscriptionServlet(this), Endpoint.UI_ADD_SUBSCRIPTION);
        this.registerServlet(servletContextHandler, new RemoveSubscriptionServlet(this), Endpoint.UI_REMOVE_SUBSCRIPTION);
        this.registerServlet(servletContextHandler, new ServerInfoServlet(this), Endpoint.UI_SERVER_INFO);
    }

    public Channel findOrConnectToServer(String applicationName, String key) {
        Item item = new Item(applicationName, key);

        // Double check
        if (this.getMappingCache().get(item) == null) {
            synchronized (this) {
                if (this.getMappingCache().get(item) == null) {
                    this.getMappingCache().put(item
                            , this.getNodeSelector().selectNodeToConnect(applicationName, key, this.getNodeList()));
                }
            }
        }
        NodeMetadata nodeMetadata = this.getMappingCache().get(item);

        // TODO: Need refactoring
        // For load balance
        if (this.getNodeSelector() instanceof LoadBalancedSelector) {
            double threshold = ((LoadBalancedSelector) this.getNodeSelector()).getCpuThreshold();
            if (this.getServerCpuUsage().get(nodeMetadata) > threshold) {
                synchronized (this) {
                    this.getMappingCache().put(item
                            , this.getNodeSelector().selectNodeToConnect(applicationName, key, this.getNodeList()));
                }
            }
        }

        Channel channel = this.getServerConnections().get(nodeMetadata);
        if (channel == null) {
            this.getServerConnections().put(nodeMetadata, this.doConnect(nodeMetadata.getAddress(), nodeMetadata.getPort()));
        }
        return this.getServerConnections().get(nodeMetadata);
    }

    public Channel findOrConnectToServer(int serverId) {
        NodeMetadata nodeMetadata = null;
        for (NodeMetadata metadata : this.getNodeList()) {
            if (metadata.getId() == serverId) {
                nodeMetadata = metadata;
                break;
            }
        }
        if (nodeMetadata == null) {
            return null;
        }
        Channel channel = this.getServerConnections().get(nodeMetadata);
        if (channel == null) {
            this.getServerConnections().put(nodeMetadata, this.doConnect(nodeMetadata.getAddress(), nodeMetadata.getPort()));
        }
        return this.getServerConnections().get(nodeMetadata);
    }

    public Channel findOrConnectToClient(ClientMetadata clientMetadata) {
        Channel channel = this.getClientConnections().get(clientMetadata);
        if (channel == null) {
            this.getClientConnections().put(clientMetadata, this.doConnect(clientMetadata.getAddress(), clientMetadata.getPort()));
        }
        return this.getClientConnections().get(clientMetadata);
    }

    private Channel doConnect(String address, int port) {
        try {
            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(this.getWorkerGroup())
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.TCP_NODELAY, Boolean.TRUE)
                    .option(ChannelOption.SO_REUSEADDR, true) // TODO: Trick
                    .handler(new ClientChannelInitializer(this));

            CountDownLatch latch = new CountDownLatch(1);
            ChannelFuture future = bootstrap.connect(address, port).addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture channelFuture) throws Exception {
                    LOGGER.info("[RippleClient] doConnect: Completed.");
                    latch.countDown();
                }
            });
            latch.await();
            return future.channel();
        } catch (Exception exception) {
            exception.printStackTrace();
            return null;
        }
    }

    public synchronized boolean start() {
        if (this.isRunning()) {
            return true;
        }
        try {
            this.setConnectedNodes(new ArrayList<>());
            this.setBossGroup(new NioEventLoopGroup());
            this.setWorkerGroup(new NioEventLoopGroup());

            ServerBootstrap serverBootstrap = new ServerBootstrap();
            serverBootstrap.group(this.getBossGroup(), this.getWorkerGroup())
                    .channel(NioServerSocketChannel.class)
                    .option(ChannelOption.SO_BACKLOG, 1024)
                    .option(ChannelOption.SO_REUSEADDR, true) // Trick
                    .handler(new LoggingHandler(LogLevel.INFO))
                    .childHandler(new ClientChannelInitializer(this));
            CountDownLatch latch = new CountDownLatch(1);

            ChannelFuture future = serverBootstrap.bind(this.getAddress(), this.getApiPort()).addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture channelFuture) throws Exception {
                    LOGGER.info("[RippleClient] Started.");
                    latch.countDown();
                }
            });
            latch.await();
            if (this.getApiPort() == 0) {
                this.setApiPort(((NioServerSocketChannel) future.channel()).localAddress().getPort());
            }
            this.setServerChannel(future.channel());

            this.setServer(new Server());
            ServerConnector serverConnector = new ServerConnector(this.getServer());
            serverConnector.setPort(this.getUiPort());
            this.getServer().setConnectors(new Connector[]{serverConnector});
            ServletContextHandler servletContextHandler = new ServletContextHandler();
            this.registerHandlers(servletContextHandler);
            this.getServer().setHandler(servletContextHandler);
            this.getServer().start();

            this.setUiPort(serverConnector.getLocalPort());

            this.setWorkingThread(new Thread(this.getWorker()));
            this.getWorkingThread().start();

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
            this.getWorkingThread().interrupt();
            this.getServer().stop();
            CountDownLatch lock = new CountDownLatch(2);
            this.getBossGroup().shutdownGracefully().addListener(e -> {
                lock.countDown();
            });
            this.getWorkerGroup().shutdownGracefully().addListener(e -> {
                lock.countDown();
            });
            lock.await();
            this.setRunning(false);
            return true;
        } catch (Exception exception) {
            exception.printStackTrace();
            return false;
        }
    }
}
