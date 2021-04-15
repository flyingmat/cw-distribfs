import java.net.*;

public class DstoreConnection extends Connection {

    public DstoreConnection(Socket ins, Integer port) throws Exception {
        super(ins, new Socket(ins.getInetAddress(), port));
    }

    protected void processMessage(String msg) {
        System.out.println("[DSTORE] Received: " + msg);
    }
}
