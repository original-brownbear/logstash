package org.logstash.cluster.server;

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.List;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.ArgumentType;
import net.sourceforge.argparse4j.inf.Namespace;
import org.logstash.cluster.LogstashCluster;
import org.logstash.cluster.cluster.Node;
import org.logstash.cluster.cluster.NodeId;
import org.logstash.cluster.messaging.Endpoint;
import org.logstash.cluster.messaging.netty.NettyMessagingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class LsClusterServer {
    private static final Logger LOGGER = LoggerFactory.getLogger(LsClusterServer.class);

    public static void main(final String... args) throws Exception {
        ArgumentType<Node> nodeType = (argumentParser, argument, value) -> {
            String[] address = parseAddress(value);
            return Node.builder()
                .withId(parseNodeId(address))
                .withEndpoint(parseEndpoint(address))
                .build();
        };

        ArgumentType<File> fileType = (argumentParser, argument, value) -> new File(value);

        ArgumentParser parser = ArgumentParsers.newFor("AtomixServer").build()
            .defaultHelp(true)
            .description("Atomix server");
        parser.addArgument("node")
            .type(nodeType)
            .nargs("?")
            .metavar("NAME:HOST:PORT")
            .setDefault(Node.builder()
                .withId(NodeId.from("local"))
                .withEndpoint(new Endpoint(InetAddress.getByName("127.0.0.1"), NettyMessagingService.DEFAULT_PORT))
                .build())
            .help("The local node info");
        parser.addArgument("--bootstrap", "-b")
            .nargs("*")
            .type(nodeType)
            .metavar("NAME:HOST:PORT")
            .required(false)
            .help("Bootstraps a new cluster");
        parser.addArgument("--data-dir", "-d")
            .type(fileType)
            .metavar("FILE")
            .required(true)
            .help("The server data directory");

        Namespace namespace = null;
        try {
            namespace = parser.parseArgs(args);
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            System.exit(1);
        }

        Node localNode = namespace.get("node");
        List<Node> bootstrap = namespace.getList("bootstrap");
        if (bootstrap == null) {
            bootstrap = Collections.singletonList(localNode);
        }

        File dataDir = namespace.get("data_dir");

        LOGGER.info("Node: {}", localNode);
        LOGGER.info("Bootstrap: {}", bootstrap);
        LOGGER.info("Data: {}", dataDir);

        LogstashCluster server = LogstashCluster.builder()
            .withLocalNode(localNode)
            .withBootstrapNodes(bootstrap)
            .withDataDir(dataDir)
            .build();

        server.open().join();
        synchronized (LogstashCluster.class) {
            while (server.isOpen()) {
                LogstashCluster.class.wait();
            }
        }
    }

    static String[] parseAddress(String address) {
        String[] parsed = address.split(":");
        if (parsed.length > 3) {
            throw new IllegalArgumentException("Malformed address " + address);
        }
        return parsed;
    }

    static NodeId parseNodeId(String[] address) {
        if (address.length == 3) {
            return NodeId.from(address[0]);
        } else if (address.length == 2) {
            try {
                InetAddress.getByName(address[0]);
            } catch (UnknownHostException e) {
                return NodeId.from(address[0]);
            }
            return NodeId.from(parseEndpoint(address).toString());
        } else {
            try {
                InetAddress.getByName(address[0]);
                return NodeId.from(parseEndpoint(address).toString());
            } catch (UnknownHostException e) {
                return NodeId.from(address[0]);
            }
        }
    }

    static Endpoint parseEndpoint(String[] address) {
        String host;
        int port;
        if (address.length == 3) {
            host = address[1];
            port = Integer.parseInt(address[2]);
        } else if (address.length == 2) {
            try {
                host = address[0];
                port = Integer.parseInt(address[1]);
            } catch (NumberFormatException e) {
                host = address[1];
                port = NettyMessagingService.DEFAULT_PORT;
            }
        } else {
            try {
                InetAddress.getByName(address[0]);
                host = address[0];
            } catch (UnknownHostException e) {
                host = "127.0.0.1";
            }
            port = NettyMessagingService.DEFAULT_PORT;
        }

        try {
            return new Endpoint(InetAddress.getByName(host), port);
        } catch (UnknownHostException e) {
            throw new IllegalArgumentException("Failed to resolve host", e);
        }
    }
}
