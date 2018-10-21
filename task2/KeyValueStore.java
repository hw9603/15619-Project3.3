import java.sql.Timestamp;
import java.util.ArrayList;
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

public class KeyValueStore extends Verticle {
    /* TODO: Add code to implement your backend storage */

    /**
     * {@code #ConcurrentHashMap} stores all the key-value pairs.
     *
     */
    private static ConcurrentHashMap<String, String> keyValueStorage = new ConcurrentHashMap<>();

    /**
     * {@code #ConcurrentHashMap} stores all the waiting task timestamps by their own key.
     *
     */
    private static ConcurrentHashMap<String, PriorityQueue<Long>> allTimestamps = new
            ConcurrentHashMap<>();

    /**
     * {@code #ConcurrentHashMap} stores all the waiting task operation (put/get)
     * by their own key.
     *
     */
    private static ConcurrentHashMap<String, HashMap<String, Integer>> allOperations = new
            ConcurrentHashMap<>();

    /**
     * {@code #ConcurrentHashMap} stores the timestamp for the last update for each key.
     *
     */
    private static ConcurrentHashMap<String, Long> updateTimestamp = new ConcurrentHashMap<>();

    /**
     * Acquire lock measn peek at the current operation queue for the specified key,
     * if the next one is the current operation, we start the thread. Otherwise, wait
     * until awaken by other threads.
     *
     * @param timestamp the timestamp of the operation
     * @param key the key of the operation
     */
    public static void acquireLock(Long timestamp, String key) {
        PriorityQueue<Long> keyWaitingQueue;
        synchronized (allTimestamps) {
            keyWaitingQueue = allTimestamps.get(key);
            if (keyWaitingQueue == null) {
                PriorityQueue<Long> newQueue = new PriorityQueue<Long>();
                allTimestamps.put(key, newQueue);
                keyWaitingQueue = newQueue;
            }
            keyWaitingQueue = allTimestamps.get(key);
        }

        synchronized (keyWaitingQueue) {
            Long top = keyWaitingQueue.peek();
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
        PriorityQueue<Long> keyWaitingQueue = allTimestamps.get(key);
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
        final KeyValueStore keyValueStore = new KeyValueStore();
        final RouteMatcher routeMatcher = new RouteMatcher();
        final HttpServer server = vertx.createHttpServer();
        server.setAcceptBacklog(32767);
        server.setUsePooledBuffers(true);
        server.setReceiveBufferSize(4 * 1024);

        routeMatcher.get("/put", new Handler<HttpServerRequest>() {
            @Override
            public void handle(final HttpServerRequest req) {
                MultiMap map = req.params();
                String key = map.get("key");
                String value = map.get("value");
                String consistency = map.get("consistency");
                Integer region = Integer.parseInt(map.get("region"));

                Long timestamp = Long.parseLong(map.get("timestamp"));

                /* TODO: Add code here to handle the put request
                     Remember to use the explicit timestamp if needed! */

                Thread t = new Thread(new Runnable() {
                    public void run() {
                        if (consistency.equals("strong")) {
                            acquireLock(timestamp, key);
                            putLock(key);
                            keyValueStorage.put(key, value);
                            putUnlock(key);
                            releaseLock(key);
                        } else {
                            if (!updateTimestamp.containsKey(key)) {
                                keyValueStorage.put(key, value);
                                updateTimestamp.put(key, timestamp);
                            } else if (updateTimestamp.get(key).compareTo(timestamp) < 0) {
                                keyValueStorage.put(key, value);
                                updateTimestamp.put(key, timestamp);
                            }
                        }
                    }
                });
                t.start();
                String response = "stored";
                req.response().putHeader("Content-Type", "text/plain");
                req.response().putHeader("Content-Length",
                        String.valueOf(response.length()));
                req.response().end(response);
                req.response().close();
            }
        });

        routeMatcher.get("/get", new Handler<HttpServerRequest>() {
            @Override
            public void handle(final HttpServerRequest req) {
                MultiMap map = req.params();
                final String key = map.get("key");
                String consistency = map.get("consistency");
                final Long timestamp = Long.parseLong(map.get("timestamp"));

                /* TODO: Add code here to handle the get request
                     Remember that you may need to do some locking for this */

                Thread t = new Thread(new Runnable() {
                    public void run() {
                        String response = null;
                        if (consistency.equals("strong")) {
                            // Add the timestamp to the queue.
                            PriorityQueue<Long> keyWaitingQueue;
                            synchronized (allTimestamps) {
                                keyWaitingQueue = allTimestamps.get(key);
                                if (keyWaitingQueue == null) {
                                    PriorityQueue<Long> newQueue = new PriorityQueue<Long>();
                                    allTimestamps.put(key, newQueue);
                                    keyWaitingQueue = newQueue;
                                }
                                keyWaitingQueue = allTimestamps.get(key);
                            }
                            synchronized (keyWaitingQueue) {
                                keyWaitingQueue.add(timestamp);
                            }
                            acquireLock(timestamp, key);
                            getLock(key);
                            // Need to release lock in order not to block other GET requests
                            releaseLock(key);
                            // Do the GET job.
                            response = keyValueStorage.get(key);
                            getUnlock(key);
                        } else {
                            response = keyValueStorage.get(key);
                        }
                        if (response == null) {
                            response = "0";
                        }
                        req.response().putHeader("Content-Type", "text/plain");
                        if (response != null) {
                            req.response().putHeader("Content-Length",
                                    String.valueOf(response.length()));
                        }
                        req.response().end(response);
                        req.response().close();
                    }
                });
                t.start();
            }
        });

        // Clears stored keys.
        routeMatcher.get("/reset", new Handler<HttpServerRequest>() {
            @Override
            public void handle(final HttpServerRequest req) {
                /* TODO: Add code to here to flush your datastore. This is MANDATORY */
                keyValueStorage.clear();
                allTimestamps.clear();
                allOperations.clear();

                req.response().putHeader("Content-Type", "text/plain");
                req.response().end();
                req.response().close();
            }
        });

        // Handler for PRECOMMIT
        routeMatcher.get("/precommit", new Handler<HttpServerRequest>() {
            @Override
            public void handle(final HttpServerRequest req) {
                MultiMap map = req.params();
                String key = map.get("key");
                final Long timestamp = Long.parseLong(map.get("timestamp"));
                /* TODO: Add code to handle the signal here if you wish */
                Thread t = new Thread(new Runnable() {
                    public void run() {
                        // Add timestamp to the queue.
                        PriorityQueue<Long> keyWaitingQueue;
                        synchronized (allTimestamps) {
                            keyWaitingQueue = allTimestamps.get(key);
                            if (keyWaitingQueue == null) {
                                PriorityQueue<Long> newQueue = new PriorityQueue<Long>();
                                allTimestamps.put(key, newQueue);
                                keyWaitingQueue = newQueue;
                            }
                            keyWaitingQueue = allTimestamps.get(key);
                        }
                        synchronized (keyWaitingQueue) {
                            keyWaitingQueue.add(timestamp);
                        }
                    }
                });
                t.start();
                req.response().putHeader("Content-Type", "text/plain");
                /* Do not remove the following lines */
                req.response().end("stored");
                req.response().close();
            }
        });

        // Handler when there is no match.
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

