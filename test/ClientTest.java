import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import org.junit.*;
import org.junit.rules.*;
import org.junit.runner.*;
import org.junit.runners.MethodSorters;
import static org.junit.Assert.*;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ClientTest {

    private static final Integer cport = 2233;
    private static final Integer timeout = 200;

    private static final String dw = "test_download";
    private static final String up = "test_upload";

    private static File dwFolder;
    private static File upFolder;

    public List<String> listDwFiles(File dir) {
        return Arrays.asList(dir.listFiles()).stream().filter(t -> t.isFile()).map(t -> t.getName()).collect(Collectors.toList());
    }

    @Rule
    public TestRule watcher = new TestWatcher() {
        protected void starting(Description description) {
            System.out.println("Starting test: " + description.getMethodName());
        }
    };

    @BeforeClass
    public static void init() throws Exception {
        dwFolder = new File(dw);
		if (!dwFolder.exists())
			if (!dwFolder.mkdir()) throw new RuntimeException("Cannot create " + dw + " folder (folder absolute path: " + dwFolder.getAbsolutePath() + ")");

		upFolder = new File(up);
		if (!upFolder.exists())
			if (!upFolder.mkdir()) throw new RuntimeException("Cannot create " + up + " folder (folder absolute path: " + upFolder.getAbsolutePath() + ")");
    }

    @Before
    public void reset() {
        for (File f : upFolder.listFiles())
            f.delete();
        for (File f : dwFolder.listFiles())
            f.delete();
    }

    @Test
    public void a_test10x3MB_Seq() throws Exception {

        Client c = new Client(cport, timeout, Logger.LoggingType.ON_FILE_ONLY);
        c.connect();

        Set<String> upfiles = new HashSet<String>();
        for (int i = 0; i < 10; i++) {
            RandomAccessFile f = new RandomAccessFile(up + "/f" + i + ".tmp", "rw");
            f.setLength(3 * 1024 * 1024);
            upfiles.add("f" + i + ".tmp");
        }

        for (int i = 0; i < 10; i++) {
            c.store(new File(up + "/f" + i + ".tmp"));
        }

        Set<String> files = new HashSet<String>(Arrays.asList(c.list()));
        assertEquals(upfiles, files);
    }

    @Test
    public void b_testRemoveAll_Seq() throws Exception {
        Client c = new Client(cport, timeout, Logger.LoggingType.ON_FILE_ONLY);
        c.connect();

        for (int i = 0; i < 10; i++) {
            c.remove("f" + i + ".tmp");
        }

        Set<String> files = new HashSet<String>(Arrays.asList(c.list()));
        assertEquals(new HashSet<String>(), files);
    }

    public void storeFileThread(int i, CountDownLatch cl) {
        new Thread(
            new Runnable() {
                public void run() {
                    try {
                        Client c = new Client(cport, timeout, Logger.LoggingType.ON_FILE_ONLY);
                        c.connect();
                        c.store(new File(up + "/f" + i + ".tmp"));
                    } catch (Exception e) {}
                    cl.countDown();
                }
            }
        ).start();
    }

    @Test
    public void c_test10x3MB_Prl() throws Exception {

        Set<String> upfiles = new HashSet<String>();
        for (int i = 10; i < 20; i++) {
            RandomAccessFile f = new RandomAccessFile(up + "/f" + i + ".tmp", "rw");
            f.setLength(3 * 1024 * 1024);
            upfiles.add("f" + i + ".tmp");
        }

        CountDownLatch cl = new CountDownLatch(10);
        for (int i = 10; i < 20; i ++) {
            storeFileThread(i, cl);
        }

        cl.await();

        Client c = new Client(cport, timeout, Logger.LoggingType.ON_FILE_ONLY);
        c.connect();

        Set<String> files = new HashSet<String>(Arrays.asList(c.list()));
        assertEquals(upfiles, files);
    }

    public void removeFileThread(int i, CountDownLatch cl) {
        new Thread(
            new Runnable() {
                public void run() {
                    try {
                        Client c = new Client(cport, timeout, Logger.LoggingType.ON_FILE_ONLY);
                        c.connect();
                        c.remove("f" + i + ".tmp");
                    } catch (Exception e) {}
                    cl.countDown();
                }
            }
        ).start();
    }

    @Test
    public void d_testRemoveAll_Prl() throws Exception {

        CountDownLatch cl = new CountDownLatch(10);
        for (int i = 10; i < 20; i++) {
            removeFileThread(i, cl);
        }

        cl.await();

        Client c = new Client(cport, timeout, Logger.LoggingType.ON_FILE_ONLY);
        c.connect();

        Set<String> files = new HashSet<String>(Arrays.asList(c.list()));
        assertEquals(new HashSet<String>(), files);
    }

    public void loadFileThread(int i, CountDownLatch cl, File tf) {
        new Thread(
            new Runnable() {
                public void run() {
                    try {
                        Client c = new Client(cport, timeout, Logger.LoggingType.ON_FILE_ONLY);
                        c.connect();
                        c.load("f" + i + ".tmp", tf);
                    } catch (Exception e) {}
                    cl.countDown();
                }
            }
        ).start();
    }

    @Test
    public void e_test50x1KB_Prl() throws Exception {
        Set<String> upfiles = new HashSet<String>();
        for (int i = 20; i < 70; i++) {
            RandomAccessFile f = new RandomAccessFile(up + "/f" + i + ".tmp", "rw");
            f.setLength(1024);
            upfiles.add("f" + i + ".tmp");
        }

        CountDownLatch cl = new CountDownLatch(50);
        for (int i = 20; i < 70; i ++) {
            storeFileThread(i, cl);
        }

        cl.await();

        Client c = new Client(cport, timeout, Logger.LoggingType.ON_FILE_ONLY);
        c.connect();

        Set<String> files = new HashSet<String>(Arrays.asList(c.list()));
        assertEquals(upfiles, files);
    }

    @Test
    public void f_testLoad50x1KB_Prl() throws Exception {

        Set<String> upfiles = new HashSet<String>();
        CountDownLatch cl = new CountDownLatch(50);

        for (int i = 20; i < 70; i++) {
            loadFileThread(i, cl, dwFolder);
            upfiles.add("f" + i + ".tmp");
        }

        cl.await();

        Set<String> files = new HashSet<String>(listDwFiles(dwFolder));
        assertEquals(upfiles, files);
    }

    @Test
    public void g_testRemoveWhileLoad() throws Exception {
        Set<String> upfiles = new HashSet<String>();
        CountDownLatch cl = new CountDownLatch(100);

        for (int i = 20; i < 70; i++) {
            loadFileThread(i, cl, dwFolder);
            removeFileThread(i, cl);
            upfiles.add("f" + i + ".tmp");
        }

        cl.await();

        Client c = new Client(cport, timeout, Logger.LoggingType.ON_FILE_ONLY);
        c.connect();

        Set<String> files = new HashSet<String>(Arrays.asList(c.list()));
        assertEquals(new HashSet<String>(), files);
    }

    @Test
    public void h_testStress() throws Exception {

        Client c = new Client(cport, timeout, Logger.LoggingType.ON_FILE_ONLY);
        c.connect();

        Set<String> upfiles = new HashSet<String>();
        for (int i = 20; i < 70; i++) {
            RandomAccessFile f = new RandomAccessFile(up + "/f" + i + ".tmp", "rw");
            f.setLength(1024);
            upfiles.add("f" + i + ".tmp");
        }

        CountDownLatch cl = new CountDownLatch(250);
        for (int i = 20; i < 70; i ++) {
            c.store(new File(up + "/f" + i + ".tmp"));
            for (int k = 0; k < 5; k++) {
                File tf = new File(dw + "/" + i + "_" + k);
                if (!tf.exists())
                    tf.mkdir();
                loadFileThread(i, cl, tf);
            }
        }

        cl.await();

        for (int i = 20; i < 70; i ++) {
            for (int k = 0; k < 5; k++) {
                File tf = new File(dw + "/" + i + "_" + k);
                Set<String> t = new HashSet<String>();
                t.add("f" + i + ".tmp");
                assertEquals(t, new HashSet<String>(listDwFiles(tf)));
            }
        }
    }
}
