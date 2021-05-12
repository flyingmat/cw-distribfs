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
        String[] ws = msg.split(" ");
        switch (ws[0]) {

            case Protocol.LIST_TOKEN:
                if (ws.length == 1) {
                    dispatch(dstore.list());
                } else {
                    System.out.println("(?) " + Protocol.LIST_TOKEN + " MALFORMED");
                }
                break;

            case Protocol.STORE_TOKEN:
                if (ws.length == 3) {
                    try {
                        Integer fs = Integer.parseInt(ws[2]);
                        File outputFile = new File(this.dstore.getFileFolder() + "/" + ws[1]);

                        dispatch(Protocol.ACK_TOKEN);

                        // receive file contents
                        byte[] contents = ins.getInputStream().readNBytes(fs);
                        FileOutputStream outf = new FileOutputStream(outputFile);
                        outf.write(contents);
                        outf.close();

                        this.dstore.store(ws[1]);
                        this.dstore.getController().dispatch(Protocol.STORE_ACK_TOKEN + " " + ws[1]);
                    } catch (NumberFormatException e) {
                        // malformed message ?
                        System.out.println("(?) " + Protocol.STORE_TOKEN + " file_size IS NOT A NUMBER");
                    } catch (Exception e) {
                        System.out.println("Error while reading file contents\n    (!) " + e.getMessage());
                    }
                } else {
                    // log
                    System.out.println("(?) " + Protocol.STORE_TOKEN + " MALFORMED");
                }
                break;

            case Protocol.LOAD_DATA_TOKEN:
                if (ws.length == 2) {
                    try {
                        File inputFile = new File(this.dstore.getFileFolder() + "/" + ws[1]);
                        if (!inputFile.isFile()) {
                            System.out.println("(?) " + Protocol.LOAD_DATA_TOKEN + " RECEIVED, BUT " + ws[1] + " NOT FOUND");
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
                        System.out.println("Error while sending file contents\n    (!) " + e.getMessage());
                    }
                } else {
                    System.out.println("(?) " + Protocol.LOAD_DATA_TOKEN + " MALFORMED");
                }
                break;

            case Protocol.REMOVE_TOKEN:
                if (ws.length == 2) {
                    File file = new File(this.dstore.getFileFolder() + "/" + ws[1]);
                    if (file.isFile()) {
                        file.delete();
                        this.dstore.getController().dispatch(Protocol.REMOVE_ACK_TOKEN + " " + ws[1]);
                    } else {
                        this.dstore.getController().dispatch(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN + " " + ws[1]);
                    }
                } else {
                    System.out.println("(?) " + Protocol.REMOVE_TOKEN + " MALFORMED");
                }
                break;
        }
    }

    @Override
    protected void onDisconnect() {}

    @Override
    public String await() throws Exception {
        String msg = super.await();
        if (msg != null)
            DstoreLogger.getInstance().messageReceived(this.ins, msg);
        return msg;
    }

    public void dispatch(String msg) {
        super.dispatch(msg);
        DstoreLogger.getInstance().messageSent(this.outs, msg);
    }
}
