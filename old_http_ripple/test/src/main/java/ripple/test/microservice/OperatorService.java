package ripple.test.microservice;

import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import ripple.client.RippleClient;

import java.net.InetAddress;

public class OperatorService {
    private RippleClient client;
    private String address;
    private int port;
    private Server server;
    private boolean running;

    public RippleClient getClient() {
        return client;
    }

    public void setClient(RippleClient client) {
        this.client = client;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    private Server getServer() {
        return server;
    }

    private void setServer(Server server) {
        this.server = server;
    }

    public boolean isRunning() {
        return running;
    }

    public void setRunning(boolean running) {
        this.running = running;
    }

    public OperatorService(String serverAddress, int serverPort, String storageLocation) {
        this.setClient(new RippleClient(serverAddress, serverPort, storageLocation));
        this.setPort(0);
    }

    public synchronized boolean start() {
        if (this.isRunning()) {
            return true;
        }
        try {
            this.getClient().start();

            this.getClient().subscribe("testApp", "oneAddress");
            this.getClient().subscribe("testApp", "twoAddress");
            this.getClient().subscribe("testApp", "function");

            this.setServer(new Server());
            ServerConnector serverConnector = new ServerConnector(this.getServer());
            serverConnector.setPort(this.getPort());
            this.getServer().setConnectors(new Connector[]{serverConnector});

            ServletContextHandler servletContextHandler = new ServletContextHandler();
            servletContextHandler.addServlet(new ServletHolder(new OperatorServlet(this)), "/");
            this.getServer().setHandler(servletContextHandler);
            this.getServer().start();
            this.setAddress(InetAddress.getLocalHost().getHostAddress());
            this.setPort(serverConnector.getLocalPort());

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
            this.getServer().stop();
            this.getClient().stop();
            this.setRunning(false);
            return true;
        } catch (Exception exception) {
            exception.printStackTrace();
            return false;
        }
    }
}