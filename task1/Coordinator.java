import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.InterruptedException;
import java.net.URL;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.PriorityQueue;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;

import org.vertx.java.core.Handler;
import org.vertx.java.core.MultiMap;
import org.vertx.java.core.http.HttpServer;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.http.RouteMatcher;
import org.vertx.java.platform.Verticle;

public class Coordinator extends Verticle {

    /**
     * TODO: Set the values of the following variables to the PRIVATE IP of your
     * three dataCenter instances.
     */
    private static final String dataCenter1 = "172.31.86.200";
    private static final String dataCenter2 = "172.31.84.202";
    private static final String dataCenter3 = "172.31.86.160";

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

    /**
     * This function is to acquire lock for the PUT operation.
     * We need to put one writer in the hashmap.
     *
     * @param key the key of the operation
     */
    public static void putLock(String key) {
        HashMap<String, Integer> keyWaitingOperations;
        // Retrieve the waiting queue for the specified key.
        synchronized (allOperations) {
            keyWaitingOperations = allOperations.get(key);
            if (keyWaitingOperations == null) {
                HashMap<String, Integer> newMap = new HashMap<String, Integer>();
                allOperations.put(key, newMap);
                keyWaitingOperations = newMap;
            }
        }
        synchronized (keyWaitingOperations) {
            while (keyWaitingOperations.size() != 0) {
                try {
                    keyWaitingOperations.wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            keyWaitingOperations.put("PUT", 1);
        }
    }

    /**
     * This function is to release lock for the PUT operation.
     * We need to remove one writer in the hashmap.
     *
     * @param key the key of the operation
     */
    public static void putUnlock(String key) {
        HashMap<String, Integer> keyWaitingOperations;
        // Retrieve the waiting queue for the specified key.
        synchronized (allOperations) {
            keyWaitingOperations = allOperations.get(key);
        }
        synchronized (keyWaitingOperations) {
            keyWaitingOperations.remove("PUT");
        }
    }

    /**
     * This function is to acquire lock for the GET operation.
     * We need to add one reader in the hashmap.
     *
     * @param key the key of the operation
     */
    public static void getLock(String key) {
        HashMap<String, Integer> keyWaitingOperations;
        // Retrieve the waiting queue for the specified key.
        synchronized (allOperations) {
            keyWaitingOperations = allOperations.get(key);
            if (keyWaitingOperations == null) {
                HashMap<String, Integer> newMap = new HashMap<String, Integer>();
                allOperations.put(key, newMap);
                keyWaitingOperations = newMap;
            }
        }
        // Add the GET job to the waiting queue.
        synchronized (keyWaitingOperations) {
            if (keyWaitingOperations.size() == 0) {
                keyWaitingOperations.put("GET", 1);
            } else {
                keyWaitingOperations.put("GET",
                        keyWaitingOperations.get("GET") + 1);
            }
        }
    }

    /**
     * This function is to release lock for the GET operation.
     * We need to reduce one reader in the hashmap.
     *
     * @param key the key of the operation
     */
    public static void getUnlock(String key) {
        HashMap<String, Integer> keyWaitingOperations;
        // Retrieve the waiting queue for the specified key.
        synchronized (allOperations) {
            keyWaitingOperations = allOperations.get(key);
        }
        // Update the number of GET operations.
        synchronized (keyWaitingOperations) {
            int numOps = keyWaitingOperations.get("GET");
            numOps--;
            if (numOps == 0) {
                keyWaitingOperations.remove("GET");
                keyWaitingOperations.notifyAll();
            } else {
                keyWaitingOperations.put("GET", numOps);
            }
        }
    }

    @Override
    public void start() {
        //DO NOT MODIFY THIS
        KeyValueLib.dataCenters.put(dataCenter1, 1);
        KeyValueLib.dataCenters.put(dataCenter2, 2);
        KeyValueLib.dataCenters.put(dataCenter3, 3);
        final RouteMatcher routeMatcher = new RouteMatcher();
        final HttpServer server = vertx.createHttpServer();
        server.setAcceptBacklog(32767);
        server.setUsePooledBuffers(true);
        server.setReceiveBufferSize(4 * 1024);

        routeMatcher.get("/put", new Handler<HttpServerRequest>() {
            @Override
            public void handle(final HttpServerRequest req) {
                MultiMap map = req.params();
                final String key = map.get("key");
                final String value = map.get("value");
                //You may use the following timestamp for ordering requests
                final String timestamp = new Timestamp(System.currentTimeMillis()
                                        + TimeZone.getTimeZone("EST").getRawOffset()).toString();
                Thread t = new Thread(new Runnable() {
                    public void run() {
                        //TODO: Write code for PUT operation here.
                        //Each PUT operation is handled in a different thread.
                        //Highly recommended that you make use of helper functions.
                        acquireLock(timestamp, key);
                        putLock(key);
                        // There is no pending job. Just do the PUT job.
                        try {
                            KeyValueLib.PUT(dataCenter1, key, value);
                            KeyValueLib.PUT(dataCenter2, key, value);
                            KeyValueLib.PUT(dataCenter3, key, value);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        putUnlock(key);
                        releaseLock(key);
                    }
                });
                t.start();

                // Every important notice should be repeated three times
                //Do not remove this
                //Do not remove this
                //Do not remove this
                req.response().end();

            }
        });

        routeMatcher.get("/get", new Handler<HttpServerRequest>() {
            @Override
            public void handle(final HttpServerRequest req) {
                MultiMap map = req.params();
                final String key = map.get("key");
                final String loc = map.get("loc");
                //You may use the following timestamp for ordering requests
                final String timestamp = new Timestamp(System.currentTimeMillis()
                                + TimeZone.getTimeZone("EST").getRawOffset()).toString();
                Thread t = new Thread(new Runnable() {
                    public void run() {
                        //TODO: Write code for GET operation here.
                        //Each GET operation is handled in a different thread.
                        //Highly recommended that you make use of helper functions.
                        acquireLock(timestamp, key);
                        getLock(key);
                        // Need to release lock in order not to block other GET requests
                        releaseLock(key);
                        // Do the GET job.
                        String value = "null";
                        try {
                            switch (loc) {
                                case "1":
                                    value = KeyValueLib.GET(dataCenter1, key);
                                    break;
                                case "2":
                                    value = KeyValueLib.GET(dataCenter2, key);
                                    break;
                                case "3":
                                    value = KeyValueLib.GET(dataCenter3, key);
                                    break;
                                default:
                            }
                        } catch (IOException e) {
                            e.printStackTrace();
                        }

                        if (value.equals("null")) {
                            value = "0";
                        }
                        getUnlock(key);
                        req.response().end(value);
                    }
                });
                t.start();
            }
        });

        routeMatcher.get("/flush", new Handler<HttpServerRequest>() {
            @Override
            public void handle(final HttpServerRequest req) {
                //Flush all datacenters before each test.
                URL url = null;
                try {
                  flush(dataCenter1);
                  flush(dataCenter2);
                  flush(dataCenter3);
                } catch (Exception e) {
                  e.printStackTrace();
                }
                // This endpoint will be used by the auto-grader to flush your datacenter
                // before tests
                // You can initialize/re-initialize the required data structures here
                req.response().end();
            }

            private void flush(String dataCenter) throws Exception {
                URL url = new URL("http://" + dataCenter + ":8080/flush");
                BufferedReader in = new BufferedReader(
                                      new InputStreamReader(
                                        url.openConnection().getInputStream()));
                String inputLine;
                while ((inputLine = in.readLine()) != null);
                in.close();
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
