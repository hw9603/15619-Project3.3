import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

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
    private static final String dataCenterUSE = "<DATACENTER_USE-PRIVATE-IP>";
    private static final String dataCenterUSW = "<DATACENTER_USW-PRIVATE-IP>";
    private static final String dataCenterSING = "<DATACENTER_SING-PRIVATE-IP>";

    private static final String coordinatorUSE = "<COORDINATOR_USE-PRIVATE-IP>";
    private static final String coordinatorUSW = "<COORDINATOR_USW-PRIVATE-IP>";
    private static final String coordinatorSING = "<COORDINATOR_SING-PRIVATE-IP>";

    private static final String truetimeServer = "<TRUETIMESERVER-PRIVATE-IP>";

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
                    /* TODO: Add code to handle PUT request here
                     * Each operation is handled in a new thread.
                     * Use of helper functions is highly recommended */
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
                        /* TODO: Add code to handle GET requests here
                         * Each operation is handled in a new thread.
                         * Use of helper functions is highly recommended */
                        String response = "0";
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
