import java.util.*;
import java.net.*;

public class DstoreConnection extends Connection {

    private Controller controller;
    private Integer port;
    private String fmsg = "";

    public DstoreConnection(Controller controller, Socket ins, Integer port, String msg) throws Exception {
        super(ins, new Socket(ins.getInetAddress(), port));
        this.controller = controller;
        this.port = port;
        this.fmsg = msg;
    }

    @Override
    protected void processMessage(String msg) {
        System.out.println("[DSTORE] Received: " + msg);
        String[] ws = msg.split(" ");
        if (ws.length > 0) {

        } else {
            // log
        }
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

    public Integer getPort() {
        return this.port;
    }

    @Override
    public void run() {
        processMessage(this.fmsg);
        super.run();
    }
}
