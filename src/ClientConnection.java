import java.net.*;

public class ClientConnection extends Connection {

    private Controller controller;
    private String fmsg = "";

    public ClientConnection(Controller controller, Socket ins, String msg) throws Exception {
        super(ins, ins);
        this.controller = controller;
        this.fmsg = msg;
    }

    @Override
    protected void processMessage(String msg) {
        System.out.println("[CLIENT] Received: " + msg);

        String[] ws = msg.split(" ");
        if (ws.length > 0) {
            switch (ws[0]) {
                case "STORE":
                    if (ws.length == 3) {
                        try {
                            controller.store(this, ws[1], Integer.parseInt(ws[2]));
                        } catch (NumberFormatException e) {
                            // log, message malformed, int not parsed
                            System.out.println("STORE file_size malformed ???");
                        }
                    } else {
                        // log, message malformed
                        System.out.println("STORE malformed ???");
                    }
                    break;
                case "LOAD":
                    if (ws.length == 2) {
                        controller.load(this, ws[1], 0);
                    } else {

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
