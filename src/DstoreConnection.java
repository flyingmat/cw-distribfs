import java.util.*;
import java.util.concurrent.*;
import java.net.*;

public class DstoreConnection extends Connection {

    private final Controller controller;

    private final Integer port;
    private final String fmsg;

    private final Set<String> files;

    public DstoreConnection(Controller controller, Socket ins, Integer port, String msg) throws Exception {
        super(ins, new Socket(ins.getInetAddress(), port));

        this.controller = controller;

        this.port = port;
        this.fmsg = msg;

        this.files = ConcurrentHashMap.newKeySet();
    }

    @Override
    protected void processMessage(String msg) {
        System.out.println(" [DSTORE] :: " + msg);
        String[] ws = msg.split(" ");
        switch (ws[0]) {
            case "STORE_ACK":
                if (ws.length == 2)
                    this.controller.getIndex().addStoreAck(ws[1]);
                break;
            case "REMOVE_ACK":
                if (ws.length == 2)
                    this.controller.getIndex().addRemoveAck(ws[1]);
                break;
        }
    }

    @Override
    protected void onDisconnect() {
        this.controller.removeDstore(this);
    }

    protected List<String> linesUntil(String exclusive) {
        List<String> lines = new ArrayList<String>();
        try {
            String line;
            while (!(line = await()).equals(exclusive))
                lines.add(line);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return lines;
    }

    public List<String> getList() {
        hold();
        List<String> list = null;
        dispatch("LIST");
        String ack = await(this.controller.getTimeout());
        if (ack != null && ack.equals("LIST BEGIN")) {
            list = linesUntil("LIST END");
        } else {
            // log
        }

        resume();
        return list;
    }

    public void store(String filename) {
        this.files.add(filename);
    }

    public void remove(String filename) {
        this.files.remove(filename);
    }

    public Integer getPort() {
        return this.port;
    }

    public Integer getFileAmount() {
        return this.files.size();
    }

    @Override
    public void run() {
        if (this.fmsg != null)
            processMessage(this.fmsg);
        super.run();
    }
}
