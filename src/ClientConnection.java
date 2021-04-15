import java.net.*;

public class ClientConnection extends Connection {

    private String fmsg = "";

    public ClientConnection(Socket ins, String msg) throws Exception {
        super(ins, ins);
        this.fmsg = msg;
    }

    protected void processMessage(String msg) {
        System.out.println("[CLIENT] Received: " + msg);
    }

    @Override
    public void run() {
        processMessage(this.fmsg);
        super.run();
    }
}
