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
                String[] ws = msg.split(" ");
                // send data
                try {
                    File inputFile = new File(ws[1]);
                    FileInputStream inf = new FileInputStream(inputFile);
                    byte[] buf = new byte[1000];
                    int buflen;
                    while ((buflen = inf.read(buf)) != -1) {
                        c.getSocketOut().getOutputStream().write(buf,0,buflen);
                    }

                    c.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            } else {
                // log ???
                System.out.println("(!OP) No ACK or malformed ACK received");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void load(Integer port, Integer timeout, String filename, Integer size) {
        try {
            TCPClient c = new TCPClient("127.0.0.1", port);

            c.dispatch("LOAD_DATA " + filename);
            Callable<List<Byte>> task = new Callable<List<Byte>>() {
                public List<Byte> call() {
                    try {
                        List<Byte> out = new ArrayList<>();
                        byte[] contents = c.getSocketIn().getInputStream().readNBytes(size);
                        for (byte b : contents)
                            out.add(new Byte(b));
                        return out;
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    return null;
                }
            };
            ExecutorService executor = Executors.newFixedThreadPool(1);
            Future<List<Byte>> r = executor.submit(task);
            try {
                List<Byte> bs = r.get(timeout, TimeUnit.MILLISECONDS);
                byte[] contents = new byte[bs.size()];
                for (int i = 0; i < bs.size(); i++)
                    contents[i] = bs.get(i).byteValue();
                File outputFile = new File(filename);
                FileOutputStream outf = new FileOutputStream(outputFile);
                outf.write(contents);
                return;
            } catch (Exception e) {
                e.printStackTrace();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
