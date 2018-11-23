package rpc;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;

public class Starter {
    public static void main(String[] args) throws IOException {
        RPC.Builder builder = new RPC.Builder(new Configuration());

        builder.setBindAddress("leon").setPort(4000).setProtocol(LoginServiceInterface.class)
                .setInstance(new LoginServiceImpl());

        RPC.Server server = builder.build();

        System.out.println("running...");

        server.start();
    }
}
