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
        String[] ws = msg.split(" ");
        switch (ws[0]) {
            case Protocol.STORE_TOKEN:
                if (ws.length == 3) {
                    try {
                        controller.store(this, ws[1], Integer.parseInt(ws[2]));
                    } catch (NumberFormatException e) {
                        // log, message malformed, int not parsed
                        System.out.println("(?) " + Protocol.STORE_TOKEN + " file_size IS NOT A NUMBER");
                    }
                } else {
                    // log, message malformed
                    System.out.println("(?) " + Protocol.STORE_TOKEN + " MALFORMED");
                }
                break;
            case Protocol.LOAD_TOKEN:
                if (ws.length == 2) {
                    reload.put(ws[1], 0);
                    controller.load(this, ws[1], 0);
                } else {
                    System.out.println("(?) " + Protocol.LOAD_TOKEN + " MALFORMED");
                }
                break;
            case Protocol.RELOAD_TOKEN:
                if (ws.length == 2) {
                    if (reload.containsKey(ws[1])) {
                        reload.put(ws[1], reload.get(ws[1]) + 1);
                        controller.load(this, ws[1], reload.get(ws[1]));
                    } else {
                        System.out.println("(?) " + Protocol.RELOAD_TOKEN + " NOT EXPECTED FOR FILENAME " + ws[1]);
                    }
                } else {
                    System.out.println("(?) " + Protocol.RELOAD_TOKEN + " MALFORMED");
                }
                break;
            case Protocol.REMOVE_TOKEN:
                if (ws.length == 2) {
                    controller.remove(this, ws[1]);
                } else {
                    System.out.println("(?) " + Protocol.REMOVE_TOKEN + " MALFORMED");
                }
                break;
            case Protocol.LIST_TOKEN:
                if (ws.length == 1) {
                    controller.list(this);
                } else {
                    System.out.println("(?) " + Protocol.LIST_TOKEN + " MALFORMED");
                }
                break;
        }
    }

    @Override
    protected void onDisconnect() {
        this.controller.removeClient(this);
    }

    @Override
    public String await() throws Exception {
        String msg = super.await();
        if (msg != null)
            ControllerLogger.getInstance().messageReceived(this.ins, msg);
        return msg;
    }

    @Override
    public void dispatch(String msg) {
        super.dispatch(msg);
        ControllerLogger.getInstance().messageSent(this.outs, msg);
    }

    @Override
    public void run() {
        processMessage(this.fmsg);
        super.run();
    }
}
