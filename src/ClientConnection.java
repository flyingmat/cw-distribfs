import java.net.*;

public class ClientConnection extends Connection {

    private Controller controller;
    private String fmsg = "";

    public ClientConnection(Controller controller, Socket ins, String msg) throws Exception {
        super(ins, ins);
        this.controller = controller;
        this.fmsg = msg;
    }

    protected void processMessage(String msg) {
        System.out.println("[CLIENT] Received: " + msg);

        String[] ws = msg.split(" ");
        if (ws.length > 0) {
            switch (ws[0]) {
                case "STORE":
                    if (ws.length == 3) {
                        try {
                            controller.store(ws[1], Integer.parseInt(ws[2]));
                        } catch (NumberFormatException e) {
                            // log
                        }
                    } else {
                        // log
                    }
                    break;
            }
        } else {
            // log
        }
    }

    @Override
    public void run() {
        processMessage(this.fmsg);
        super.run();
    }
}
