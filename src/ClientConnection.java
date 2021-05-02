import java.util.*;
import java.net.*;

public class ClientConnection extends Connection {

    private Controller controller;
    private String fmsg = "";
    private Map<String, Integer> reload = new HashMap<>();

    public ClientConnection(Controller controller, Socket ins, String msg) throws Exception {
        super(ins, ins);
        this.controller = controller;
        this.fmsg = msg;
    }

    @Override
    protected void processMessage(String msg) {
        System.out.println(" [CLIENT] :: " + msg);

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
                        reload.put(ws[1], 0);
                        controller.load(this, ws[1], 0);
                    } else {

                    }
                    break;
                case "RELOAD":
                    if (ws.length == 2) {
                        if (reload.containsKey(ws[1])) {
                            reload.put(ws[1], reload.get(ws[1]) + 1);
                            controller.load(this, ws[1], reload.get(ws[1]));
                        } else {

                        }
                    } else {

                    }
                    break;
            }
        } else {
            // log
        }
    }

    @Override
    protected void onDisconnect() {
        this.controller.removeClient(this);
    }

    @Override
    public void run() {
        processMessage(this.fmsg);
        super.run();
    }
}
