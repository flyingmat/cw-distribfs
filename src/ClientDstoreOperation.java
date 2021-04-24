import java.io.*;

class ClientDstoreOperation extends TCPClient implements Runnable {

    public enum Operation {
        STORE
    }

    private Integer timeout;
    private Operation op;
    private String msg;

    public ClientDstoreOperation(Integer port, Integer timeout, Operation op, String msg) throws Exception {
        super("127.0.0.1", port);
        this.timeout = timeout;
        this.op = op;
        this.msg = msg;
    }

    @Override
    public void run() {
        switch (this.op) {
            case STORE:
                dispatch(this.msg);
                String ack = await(this.timeout);
                if (ack != null && ack.equals("ACK")) {
                    String[] ws = msg.split(" ");
                    // send data
                    try {
                        File inputFile = new File(ws[1]);
                        FileInputStream inf = new FileInputStream(inputFile);
                        byte[] buf = new byte[1000];
                        int buflen;
                        while ((buflen = inf.read(buf)) != -1) {
                            outs.getOutputStream().write(buf,0,buflen);
                        }

                        close();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                } else {
                    // log ???
                    System.out.println("(!OP) No ACK or malformed ACK received");
                }
                break;
        }

    }
}
