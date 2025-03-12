// Copyright (c) 2023 Institute of Software, Chinese Academy of Sciences
// Ripple is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

package ripple.server.tcp;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ripple.common.tcp.Message;
import ripple.server.core.Node;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * @author Zhen Tang
 */
public class NettyServer {
    private static final Logger LOGGER = LoggerFactory.getLogger(NettyServer.class);
    private Node node;

    private boolean running;
    private String address;
    private int port;
    private Channel serverChannel;
    private List<Channel> connectedNodes;

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;

    public Node getNode() {
        return node;
    }

    private void setNode(Node node) {
        this.node = node;
    }

    public boolean isRunning() {
        return running;
    }

    private void setRunning(boolean running) {
        this.running = running;
    }

    public String getAddress() {
        return address;
    }

    private void setAddress(String address) {
        this.address = address;
    }

    public int getPort() {
        return port;
    }

    private void setPort(int port) {
        this.port = port;
    }

    private Channel getServerChannel() {
        return serverChannel;
    }

    private void setServerChannel(Channel serverChannel) {
        this.serverChannel = serverChannel;
    }

    public List<Channel> getConnectedNodes() {
        return connectedNodes;
    }

    private void setConnectedNodes(List<Channel> connectedNodes) {
        this.connectedNodes = connectedNodes;
    }

    private EventLoopGroup getBossGroup() {
        return bossGroup;
    }

    private void setBossGroup(EventLoopGroup bossGroup) {
        this.bossGroup = bossGroup;
    }

    private EventLoopGroup getWorkerGroup() {
        return workerGroup;
    }

    private void setWorkerGroup(EventLoopGroup workerGroup) {
        this.workerGroup = workerGroup;
    }

    public NettyServer(Node node, String address, int port) {
        this.setNode(node);
        this.setAddress(address);
        this.setPort(port);
    }

    public synchronized boolean start() {
        try {
            if (this.isRunning()) {
                return true;
            }

            this.setConnectedNodes(new ArrayList<>());
            this.setBossGroup(new NioEventLoopGroup());
            this.setWorkerGroup(new NioEventLoopGroup());

            ServerBootstrap serverBootstrap = new ServerBootstrap();
            serverBootstrap.group(this.getBossGroup(), this.getWorkerGroup())
                    .channel(NioServerSocketChannel.class)
                    .option(ChannelOption.SO_BACKLOG, 1024)
                    .option(ChannelOption.SO_REUSEADDR, true) // Trick
                    .handler(new LoggingHandler(LogLevel.INFO))
                    .childHandler(new ServerChannelInitializer(this));

            ChannelFuture future = serverBootstrap.bind(this.getAddress(), this.getPort()).addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture channelFuture) throws Exception {
                    LOGGER.trace("[NettyServer] [Server-{}] start(): Completed.", getNode().getId());
                }
            }).sync();
            if (this.getPort() == 0) {
                this.setPort(((NioServerSocketChannel) future.channel()).localAddress().getPort());
            }
            this.setServerChannel(future.channel());
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

    public synchronized Channel connect(String address, int port) {
        try {
            LOGGER.trace("[NettyServer] [Server-{}] Connecting to {}:{}.", getNode().getId(), address, port);
            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(this.getWorkerGroup())
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.TCP_NODELAY, Boolean.TRUE)
                    .option(ChannelOption.SO_REUSEADDR, true) // Trick
                    .handler(new ServerChannelInitializer(this));

            // Do not wait for connecting as the client may not be ready.
            ChannelFuture future = bootstrap.connect(address, port).addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture channelFuture) throws Exception {
                    LOGGER.trace("[NettyServer] [Server-{}] connect: Connected.", getNode().getId());
                }
            });
            return future.channel();
        } catch (Exception exception) {
            exception.printStackTrace();
            return null;
        }
    }

    public Channel findChannel(String address, int port) {
        List<Channel> toIter = new ArrayList<>(this.getConnectedNodes());
        for (Channel channel : toIter) {
            InetSocketAddress remoteAddress = ((NioSocketChannel) channel).remoteAddress();
            if (remoteAddress.getHostString().equals(address) && remoteAddress.getPort() == port) {
                return channel;
            }
        }
        LOGGER.trace("[NettyServer] [Server-{}] findChannel(): Cannot find the channel for {}:{}."
                , this.getNode().getId(), address, port);
        return null;
    }

    public boolean sendMessage(String address, int port, Message message) {
        Channel channel = this.findChannel(address, port);
        if (channel == null) {
            LOGGER.trace("[NettyServer] [Server-{}] sendMessage(): Channel is null.", this.getNode().getId());
            return false;
        }
        channel.writeAndFlush(message);
        return true;
    }
}
