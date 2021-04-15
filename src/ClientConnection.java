import java.net.*;

public class ClientConnection extends Connection {

    private String fmsg = "";

    public ClientConnection(Socket ins, String msg) throws Exception {
        super(ins, ins);
        this.fmsg = msg;
    }

    protected void processMessage(String msg) {
        System.out.println("[CLIENT] Received: " + msg);
        dispatch("HEMLO");
        System.out.println("[CLIENT] Sent: HEMLO");
    }

    @Override
    public void run() {
        processMessage(this.fmsg);
        try {
            String msg;
            while ((msg = await()) != null) {
                processMessage(msg);
            }

            this.ins.close();
        } catch (Exception e) {
            // when client disconnects ?
            e.printStackTrace();
        }
    }
}
