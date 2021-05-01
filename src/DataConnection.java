import java.io.*;
import java.net.*;

public class DataConnection extends Connection {

    private Dstore dstore;

    public DataConnection(Dstore dstore, Socket ins, Socket outs) throws Exception {
        super(ins, outs);
        this.dstore = dstore;
    }

    @Override
    protected void processMessage(String msg) {
        System.out.println("[CONTROLLER/CLIENT] Received: " + msg);
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
                case "STORE":
                    if (ws.length == 3) {
                        try {
                            Integer fs = Integer.parseInt(ws[2]);
                            File outputFile = new File(this.dstore.getFileFolder() + "/" + ws[1]);

                            hold();
                            System.out.println("Sending ACK..");
                            dispatch("ACK");
                            System.out.println("Sent - Reading bytes...");

                            // receive file contents
                            byte[] contents = ins.getInputStream().readNBytes(fs);
                            FileOutputStream outf = new FileOutputStream(outputFile);
                            outf.write(contents);
                            outf.close();

                            this.dstore.store(ws[1]);
                            this.dstore.getController().dispatch("STORE_ACK " + ws[1]);

                            resume();
                        } catch (NumberFormatException e) {
                            // malformed message ?
                            e.printStackTrace();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    } else {
                        // log
                        System.out.println("(!) Malformed STORE");
                    }
                    break;
                case "LOAD_DATA":
                    if (ws.length == 2) {
                        try {
                            File inputFile = new File(this.dstore.getFileFolder() + "/" + ws[1]);
                            if (!inputFile.isFile()) {
                                close();
                            } else {
                                FileInputStream inf = new FileInputStream(inputFile);
                                byte[] buf = new byte[1000];
                                int buflen;
                                while ((buflen = inf.read(buf)) != -1) {
                                    outs.getOutputStream().write(buf,0,buflen);
                                }
                                inf.close();
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    } else {

                    }
            }
        } else {
            // log
        }
    }
}
