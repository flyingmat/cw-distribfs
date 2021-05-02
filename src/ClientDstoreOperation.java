import java.util.*;
import java.util.concurrent.*;
import java.io.*;

class ClientDstoreOperation {

    public static void store(Integer port, Integer timeout, String msg) {
        try {
            TCPClient c = new TCPClient("127.0.0.1", port);

            c.dispatch(msg);
            String ack = c.await(timeout);

            if (ack != null && ack.equals("ACK")) {
                System.out.println(" [DSTORE] :: " + ack);

                String[] ws = msg.split(" ");
                // send data
                File inputFile = new File(ws[1]);
                FileInputStream inf = new FileInputStream(inputFile);
                byte[] buf = new byte[1000];
                int buflen;
                while ((buflen = inf.read(buf)) != -1) {
                    c.getSocketOut().getOutputStream().write(buf,0,buflen);
                }

                inf.close();
                c.close();
            } else {
                // log ???
                System.out.println("(!OP) No ACK or malformed ACK received");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void reload(Client cc, Integer timeout, String filename, Integer size) {
        cc.dispatch("RELOAD " + filename);
        String where = cc.await(timeout);

        if (where != null) {
            String[] ps = where.split(" ");
            System.out.println(" [CONTROLLER] :: " + where);
            if (ps.length > 0 && ps[0].equals("LOAD_FROM")) {
                try {
                    load(cc, Integer.parseInt(ps[1]), timeout, filename, size);
                } catch (NumberFormatException e ) {
                    reload(cc, timeout, filename, size);
                }
            } else if (ps.length == 1 && ps[0].equals("ERROR_LOAD")) {
                return;
            }
        }
    }

    public static void load(Client cc, Integer port, Integer timeout, String filename, Integer size) {
        try {
            TCPClient c = new TCPClient("127.0.0.1", port);
            c.getSocketIn().setSoTimeout(timeout);

            c.dispatch("LOAD_DATA " + filename);

            byte[] contents = c.getSocketIn().getInputStream().readNBytes(size);

            File outputFile = new File(filename);
            FileOutputStream outf = new FileOutputStream(outputFile);

            outf.write(contents);

            outf.close();
            c.close();
        } catch (Exception e) {
            reload(cc, timeout, filename, size);
        }
    }
}
