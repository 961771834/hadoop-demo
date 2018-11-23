
package rpc;
import org.apache.hadoop.ipc.ProtocolSignature;

import java.io.IOException;

public class LoginServiceImpl implements LoginServiceInterface {
    @Override
    public String login(String username, String password) {
        return username + "  logged in successfully";
    }

    @Override
    public long getProtocolVersion(String s, long l) throws IOException {
        return versionID;
    }

    @Override
    public ProtocolSignature getProtocolSignature(String s, long l, int i) throws IOException {
        return new ProtocolSignature();
    }
}
