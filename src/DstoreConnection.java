import java.util.*;
import java.util.concurrent.*;
import java.net.*;

public class DstoreConnection extends Connection {

    private Controller controller;
    private String fmsg = "";

    public DstoreConnection(Controller controller, Socket ins, Integer port, String msg) throws Exception {
        super(ins, new Socket(ins.getInetAddress(), port));
        this.controller = controller;
        this.fmsg = msg;
    }

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

    protected String waitForAck(String msg) {
        Callable<String> task = new Callable<String>() {
            public String call() {
                try {
                    return in.readLine();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return null;
            }
        };
        ExecutorService executor = Executors.newFixedThreadPool(1);
        dispatch(msg);
        Future<String> ack = executor.submit(task);
        try {
            String out = ack.get(this.controller.getTimeout(), TimeUnit.MILLISECONDS);
            System.out.println("* ACK received!");
            return out;
        } catch (Exception e) {
            return null;
        }
    }

    public List<String> getList() {
        hold();
        List<String> list = null;
        String ack = waitForAck("LIST");
        if (ack != null) {
            list = linesUntil("LIST END");
        } else {
            // log
        }

        resume();
        return list;
    }

    @Override
    public void run() {
        processMessage(this.fmsg);
        super.run();
    }
}
