import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;

import org.vertx.java.core.Handler;
import org.vertx.java.core.MultiMap;
import org.vertx.java.core.http.HttpServer;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.http.RouteMatcher;
import org.vertx.java.platform.Verticle;

public class Coordinator extends Verticle {

    // This integer variable tells you what region you are in
    // 1 for US-E, 2 for US-W, 3 for Singapore
    private static int region = KeyValueLib.region;

    // Default mode: Strongly consistent
    // Options: strong, eventual
    private static String consistencyType = "strong";

    /**
     * TODO: Set the values of the following variables to the PRIVATE IP of your
     * three dataCenter instances. Be sure to match the regions with their PRIVATE IP!
     * Do the same for the 3 Coordinators as well.
     */
    private static final String dataCenterUSE = "172.31.95.2";
    private static final String dataCenterUSW = "172.31.81.233";
    private static final String dataCenterSING = "172.31.84.11";

    private static final String coordinatorUSE = "172.31.90.136";
    private static final String coordinatorUSW = "172.31.90.20";
    private static final String coordinatorSING = "172.31.88.229";

    private static final String truetimeServer = "172.31.80.100";

    /**
     * {@code #ConcurrentHashMap} stores all the waiting task timestamps by their own key.
     *
     */
    private static ConcurrentHashMap<String, PriorityQueue<String>> allTimestamps = new
            ConcurrentHashMap<>();

    /**
     * {@code #ConcurrentHashMap} stores all the waiting task operation (put/get)
     * by their own key.
     *
     */
    private static ConcurrentHashMap<String, HashMap<String, Integer>> allOperations = new
            ConcurrentHashMap<>();

    /**
     * Acquire lock measn peek at the current operation queue for the specified key,
     * if the next one is the current operation, we start the thread. Otherwise, wait
     * until awaken by other threads.
     *
     * @param timestamp the timestamp of the operation
     * @param key the key of the operation
     */
    public static void acquireLock(String timestamp, String key) {
        PriorityQueue<String> keyWaitingQueue;
        synchronized (allTimestamps) {
            keyWaitingQueue = allTimestamps.get(key);
            if (keyWaitingQueue == null) {
                PriorityQueue<String> newQueue = new PriorityQueue<String>();
                allTimestamps.put(key, newQueue);
                keyWaitingQueue = newQueue;
            }
            keyWaitingQueue = allTimestamps.get(key);
            keyWaitingQueue.add(timestamp);
        }

        synchronized (keyWaitingQueue) {
            String top = keyWaitingQueue.peek();
            while (!top.equals(timestamp)) {
                try {
                    keyWaitingQueue.wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                top = keyWaitingQueue.peek();
            }
        }
    }

    /**
     * Release lock means the current operation of the specified key is done.
     * we release it and notify all the other threads with that key that we are done.
     *
     * @param key the key of the operation
     */
    public static void releaseLock(String key) {
        PriorityQueue<String> keyWaitingQueue = allTimestamps.get(key);
        synchronized (keyWaitingQueue) {
            keyWaitingQueue.poll();
            keyWaitingQueue.notifyAll();
        }
    }


    @Override
    public void start() {
        KeyValueLib.dataCenters.put(dataCenterUSE, 1);
        KeyValueLib.dataCenters.put(dataCenterUSW, 2);
        KeyValueLib.dataCenters.put(dataCenterSING, 3);
        KeyValueLib.coordinators.put(coordinatorUSE, 1);
        KeyValueLib.coordinators.put(coordinatorUSW, 2);
        KeyValueLib.coordinators.put(coordinatorSING, 3);
        final RouteMatcher routeMatcher = new RouteMatcher();
        final HttpServer server = vertx.createHttpServer();
        server.setAcceptBacklog(32767);
        server.setUsePooledBuffers(true);
        server.setReceiveBufferSize(4 * 1024);

        routeMatcher.get("/put", new Handler<HttpServerRequest>() {
            @Override
            public void handle(final HttpServerRequest req) {
                /* Do not change this part */
                MultiMap map = req.params();
                final String key = map.get("key");
                final String value = map.get("value");
                String timestamp = "";
                try {
                    timestamp = KeyValueLib.GETTIME(truetimeServer);
                } catch (Exception e) {
                    System.out.println("Failed to get timestamp");
                }
                final String ts = timestamp;

                Thread t = new Thread(new Runnable() {
                    public void run() {
                        // acquireLock(ts, key);
                        try {
                            KeyValueLib.PRECOMMIT(dataCenterUSE, key, ts);
                            KeyValueLib.PRECOMMIT(dataCenterUSW, key, ts);
                            KeyValueLib.PRECOMMIT(dataCenterSING, key, ts);
                            KeyValueLib.PUT(dataCenterUSE, key, value, ts, consistencyType);
                            KeyValueLib.PUT(dataCenterUSW, key, value, ts, consistencyType);
                            KeyValueLib.PUT(dataCenterSING, key, value, ts, consistencyType);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        // releaseLock(key);
                    }
                });
                t.start();
                req.response().end(); // Do not remove this
            }
        });

        routeMatcher.get("/get", new Handler<HttpServerRequest>() {
            @Override
            public void handle(final HttpServerRequest req) {
                /* Do not change this part */
                MultiMap map = req.params();
                final String key = map.get("key");
                String timestamp = "";
                try {
                    timestamp = KeyValueLib.GETTIME(truetimeServer);
                } catch (Exception e) {
                    System.out.println("Failed to get timestamp");
                }
                final String ts = timestamp;

                Thread t = new Thread(new Runnable() {
                    public void run() {
                        // acquireLock(ts, key);
                        // releaseLock(key);
                        String response = "0";
                        try {
                            switch (region) {
                                // 1 for US-E, 2 for US-W, 3 for Singapore
                                case 1:
                                    response = KeyValueLib.GET(
                                        dataCenterUSE, key, ts, consistencyType);
                                    break;
                                case 2:
                                    response = KeyValueLib.GET(
                                        dataCenterUSW, key, ts, consistencyType);
                                    break;
                                case 3:
                                    response = KeyValueLib.GET(
                                        dataCenterSING, key, ts, consistencyType);
                                    break;
                                default:
                            }
                        } catch (IOException e) {
                            e.printStackTrace();
                        }

                        req.response().end(response);
                    }
                });
                t.start();
            }
        });
        /* This endpoint is used by the grader to change the consistency level */
        routeMatcher.get("/consistency", new Handler<HttpServerRequest>() {
            @Override
            public void handle(final HttpServerRequest req) {
                MultiMap map = req.params();
                consistencyType = map.get("consistency");
                req.response().end();
            }
        });
        routeMatcher.noMatch(new Handler<HttpServerRequest>() {
            @Override
            public void handle(final HttpServerRequest req) {
                req.response().putHeader("Content-Type", "text/html");
                String response = "Not found.";
                req.response().putHeader("Content-Length",
                        String.valueOf(response.length()));
                req.response().end(response);
                req.response().close();
            }
        });
        server.requestHandler(routeMatcher);
        server.listen(8080);
    }
}
