import java.net.*;

public class DataConnection extends Connection {

    private Dstore dstore;

    public DataConnection(Dstore dstore, Socket ins, Socket outs) throws Exception {
        super(ins, outs);
        this.dstore = dstore;
    }

    protected void processMessage(String msg) {
        System.out.println("[CONTROLLER] Received: " + msg);
        String[] ws = msg.split(" ");
        if (ws.length > 0) {
            switch (ws[0]) {
                case "LIST":
                    if (ws.length == 1) {
                        dispatch(dstore.list());
                    } else {
                        // log
                    }
                    break;
            }
        } else {
            // log
        }
    }
}
