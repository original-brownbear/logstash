package org.logstash.cluster;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.net.Socket;

final class DistributedPipelineContext implements Serializable {

    private final DistributedClusterContext clusterContext;

    private final InetSocketAddress masterAddress;

    DistributedPipelineContext(final InetSocketAddress masterAddress,
        final DistributedClusterContext clusterContext) {
        this.clusterContext = clusterContext;
        this.masterAddress = masterAddress;
    }

    void report(String state) {
        try (Socket master = new Socket(masterAddress.getAddress(), masterAddress.getPort())) {
            System.out.println("Reporting: " + state);
            final DataOutputStream dataOut = new DataOutputStream(master.getOutputStream());
            dataOut.writeUTF(state);
            dataOut.flush();
            while (master.getInputStream().read() > 0) {
                System.out.println("Waiting for master to close");
            }
            System.out.println("Master closed connection");
        } catch (final IOException ex) {
            System.err.println(ex);
            throw new IllegalStateException(ex);
        }
    }
}
