package com.termux.api.ble;

import android.app.Application;
import android.content.Context;
import android.net.LocalServerSocket;
import android.net.LocalSocket;
import android.util.Log;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Long-lived Unix-domain JSON-RPC server for the BLE bridge.
 *
 * Newline-delimited JSON in both directions. Multiple clients may connect;
 * each connection to a BLE device is identified by its MAC address in the
 * "address" parameter. If "address" is omitted, the manager falls back to the
 * sole connected device (or errors when more than one is connected).
 *
 * Security: only processes sharing the Termux UID (sharedUserId="com.termux")
 * are accepted, checked via getPeerCredentials().getUid().
 */
public class BleSocketServer {

    private static final String TAG = "BleSocketServer";

    private final Context mContext;
    private final Application mApp;
    private final BleManager mManager;
    private final ExecutorService mWorkers = Executors.newCachedThreadPool();
    private final List<ClientConnection> mClients = new CopyOnWriteArrayList<>();
    private Thread mAcceptThread;
    private volatile boolean mRunning = false;

    public BleSocketServer(Application app) {
        this.mApp = app;
        this.mContext = app.getApplicationContext();
        this.mManager = new BleManager(mContext, this::broadcastEvent);
    }

    /** Start accepting connections. Safe to call once. */
    public synchronized void start() {
        if (mRunning) return;
        mRunning = true;
        mAcceptThread = new Thread(() -> {
            LocalServerSocket server = null;
            try {
                server = new LocalServerSocket(BleConstants.SOCKET_NAME);
                Log.i(TAG, "BLE socket listening at abstract://" + BleConstants.SOCKET_NAME);
                while (mRunning) {
                    try {
                        LocalSocket client = server.accept();
                        mWorkers.execute(() -> handleClient(client));
                    } catch (IOException e) {
                        if (mRunning) Log.e(TAG, "accept failed", e);
                    }
                }
            } catch (Throwable t) {
                Log.e(TAG, "Could not start BLE socket server", t);
            } finally {
                if (server != null) {
                    try { server.close(); } catch (IOException ignored) {}
                }
            }
        }, "BleSocket-accept");
        mAcceptThread.start();
    }

    private void handleClient(LocalSocket client) {
        try {
            int peerUid = client.getPeerCredentials().getUid();
            if (peerUid != mApp.getApplicationInfo().uid) {
                Log.w(TAG, "Rejecting BLE client with uid " + peerUid);
                client.close();
                return;
            }
        } catch (IOException e) {
            Log.e(TAG, "getPeerCredentials failed", e);
            try { client.close(); } catch (IOException ignored) {}
            return;
        }

        final ClientConnection conn;
        try {
            conn = new ClientConnection(client);
        } catch (IOException e) {
            Log.e(TAG, "Failed to create client connection", e);
            try { client.close(); } catch (IOException ignored) {}
            return;
        }
        mClients.add(conn);
        Log.i(TAG, "BLE client connected");
        try {
            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(client.getInputStream(), StandardCharsets.UTF_8));
            String line;
            while ((line = reader.readLine()) != null && mRunning) {
                line = line.trim();
                if (line.isEmpty()) continue;
                final String request = line;
                mWorkers.execute(() -> {
                    try {
                        dispatch(request, conn);
                    } catch (Throwable t) {
                        Log.e(TAG, "dispatch error", t);
                        conn.send(errorResponse(null, BleConstants.ERR_INTERNAL, String.valueOf(t.getMessage())));
                    }
                });
            }
        } catch (IOException e) {
            // client disconnected; normal
        } finally {
            mClients.remove(conn);
            conn.close();
            Log.i(TAG, "BLE client disconnected");
        }
    }

    /** Route a JSON-RPC request to the appropriate BleManager operation. */
    private void dispatch(String raw, ClientConnection conn) {
        Object id = null;
        try {
            JSONObject req = new JSONObject(raw);
            id = req.opt(BleConstants.KEY_ID);
            String cmd = req.optString(BleConstants.KEY_CMD, "");
            JSONObject params = req.optJSONObject(BleConstants.KEY_PARAMS);

            switch (cmd) {
                case "ping":
                    conn.send(okResponse(id, new JSONObject().put("pong", true)));
                    return;

                case "get_state":
                    conn.send(okResponse(id, mManager.getState()));
                    return;

                case "list_connections":
                    conn.send(okResponse(id, new JSONObject()
                            .put("connections", mManager.listConnections())));
                    return;

                case "request_permissions": {
                    if (BlePermissionActivity.hasPermissions(mContext)) {
                        conn.send(okResponse(id, new JSONObject().put("granted", true)));
                        return;
                    }
                    final Object requestId = id;
                    BlePermissionActivity.request(mContext, BlePermissionActivity.requiredPermissions(),
                            granted -> {
                                JSONObject p = new JSONObject();
                                try { p.put("granted", granted); } catch (Exception ignored) {}
                                broadcastEvent("permissions_result", p);
                                if (requestId != null) conn.send(okResponse(requestId, p));
                            });
                    return; // response sent asynchronously
                }

                case "scan.start": {
                    long timeout = params != null ? params.optLong("timeout", 10000) : 10000;
                    List<String> uuids = new ArrayList<>();
                    if (params != null && params.has("service_uuids")) {
                        JSONArray arr = params.optJSONArray("service_uuids");
                        if (arr != null) for (int i = 0; i < arr.length(); i++) uuids.add(arr.getString(i));
                    }
                    mManager.startScan(timeout, uuids);
                    conn.send(okResponse(id, new JSONObject().put("started", true)));
                    return;
                }

                case "scan.stop":
                    mManager.stopScan();
                    conn.send(okResponse(id, new JSONObject().put("stopped", true)));
                    return;

                case "connect": {
                    String addr = requiredString(params, "address");
                    int timeout = params != null ? params.optInt("timeout", 10000) : 10000;
                    mManager.connect(addr, timeout);
                    conn.send(okResponse(id, new JSONObject()
                            .put("address", addr).put("connected", true)));
                    return;
                }

                case "disconnect": {
                    String addr = optionalString(params, "address");
                    mManager.disconnect(addr);
                    conn.send(okResponse(id, new JSONObject().put("disconnected", true)));
                    return;
                }

                case "disconnect_all":
                    mManager.disconnectAll();
                    conn.send(okResponse(id, new JSONObject().put("disconnected", true)));
                    return;

                case "get_reconnect_config":
                    conn.send(okResponse(id, mManager.getReconnectConfig()));
                    return;

                case "set_reconnect_config": {
                    boolean auto = params != null && params.optBoolean("autoReconnect", false);
                    int maxAttempts = params != null ? params.optInt("maxAttempts", -1) : -1;
                    long delayMs = params != null ? params.optLong("delayMs", -1) : -1;
                    long heartbeatMs = params != null ? params.optLong("heartbeatIntervalMs", -1) : -1;
                    int hbFails = params != null ? params.optInt("heartbeatFailThreshold", -1) : -1;
                    conn.send(okResponse(id, mManager.setReconnectConfig(
                            auto, maxAttempts, delayMs, heartbeatMs, hbFails)));
                    return;
                }

                case "set_auto_reconnect": {
                    String addr = requiredString(params, "address");
                    boolean enabled = params != null && params.optBoolean("enabled", true);
                    mManager.setAutoReconnect(addr, enabled);
                    conn.send(okResponse(id, new JSONObject()
                            .put("address", addr).put("autoReconnect", enabled)));
                    return;
                }

                case "discover": {
                    String addr = optionalString(params, "address");
                    int timeout = params != null ? params.optInt("timeout", 10000) : 10000;
                    JSONArray services = mManager.getServices(addr, timeout);
                    conn.send(okResponse(id, new JSONObject().put("services", services)));
                    return;
                }

                case "read": {
                    String addr = optionalString(params, "address");
                    String uuid = requiredString(params, "uuid");
                    int timeout = params != null ? params.optInt("timeout", 10000) : 10000;
                    byte[] value = mManager.readCharacteristic(addr, uuid, timeout);
                    conn.send(okResponse(id, new JSONObject()
                            .put("uuid", uuid.toLowerCase(java.util.Locale.US))
                            .put("value", hex(value))));
                    return;
                }

                case "write": {
                    String addr = optionalString(params, "address");
                    String uuid = requiredString(params, "uuid");
                    String hex = requiredString(params, "value");
                    byte[] value = unhex(hex);
                    boolean withResponse = params != null && params.optBoolean("response", false);
                    int timeout = params != null ? params.optInt("timeout", 10000) : 10000;
                    mManager.writeCharacteristic(addr, uuid, value, withResponse, timeout);
                    conn.send(okResponse(id, new JSONObject().put("written", true)));
                    return;
                }

                case "subscribe": {
                    String addr = optionalString(params, "address");
                    String uuid = requiredString(params, "uuid");
                    int timeout = params != null ? params.optInt("timeout", 10000) : 10000;
                    mManager.subscribe(addr, uuid, timeout);
                    conn.send(okResponse(id, new JSONObject().put("subscribed", true)));
                    return;
                }

                case "unsubscribe": {
                    String addr = optionalString(params, "address");
                    String uuid = requiredString(params, "uuid");
                    int timeout = params != null ? params.optInt("timeout", 10000) : 10000;
                    mManager.unsubscribe(addr, uuid, timeout);
                    conn.send(okResponse(id, new JSONObject().put("unsubscribed", true)));
                    return;
                }

                case "request_mtu": {
                    String addr = optionalString(params, "address");
                    int mtu = params != null ? params.optInt("mtu", 517) : 517;
                    int timeout = params != null ? params.optInt("timeout", 10000) : 10000;
                    mManager.requestMtu(addr, mtu, timeout);
                    conn.send(okResponse(id, new JSONObject().put("mtu", mtu)));
                    return;
                }

                default:
                    conn.send(errorResponse(id, BleConstants.ERR_UNKNOWN_COMMAND,
                            "Unknown command: " + cmd));
            }
        } catch (BleManager.BleException e) {
            conn.send(errorResponse(id, e.code, e.getMessage()));
        } catch (JSONException e) {
            conn.send(errorResponse(id, BleConstants.ERR_BAD_REQUEST, "Bad JSON: " + e.getMessage()));
        } catch (Throwable t) {
            Log.e(TAG, "dispatch throwable", t);
            conn.send(errorResponse(id, BleConstants.ERR_INTERNAL, String.valueOf(t.getMessage())));
        }
    }

    private static String requiredString(JSONObject params, String key) throws JSONException {
        if (params == null || !params.has(key)) {
            throw new JSONException("Missing parameter: " + key);
        }
        return params.getString(key);
    }

    private static String optionalString(JSONObject params, String key) throws JSONException {
        if (params == null || !params.has(key) || params.isNull(key)) return null;
        return params.getString(key);
    }

    private void broadcastEvent(String name, JSONObject params) {
        JSONObject event = new JSONObject();
        try {
            event.put(BleConstants.KEY_EVENT, name);
            event.put(BleConstants.KEY_PARAMS, params != null ? params : new JSONObject());
        } catch (JSONException e) {
            Log.e(TAG, "build event", e);
            return;
        }
        String line = event.toString();
        for (ClientConnection c : mClients) c.sendRaw(line);
    }

    private JSONObject okResponse(Object id, JSONObject result) {
        JSONObject r = new JSONObject();
        try {
            if (id != null) r.put(BleConstants.KEY_ID, id);
            r.put(BleConstants.KEY_OK, true);
            r.put(BleConstants.KEY_RESULT, result);
        } catch (JSONException e) {
            Log.e(TAG, "okResponse", e);
        }
        return r;
    }

    private JSONObject errorResponse(Object id, String code, String message) {
        JSONObject r = new JSONObject();
        try {
            if (id != null) r.put(BleConstants.KEY_ID, id);
            r.put(BleConstants.KEY_OK, false);
            JSONObject e = new JSONObject();
            e.put("code", code);
            e.put("message", message != null ? message : "");
            r.put(BleConstants.KEY_ERROR, e);
        } catch (JSONException ex) {
            Log.e(TAG, "errorResponse", ex);
        }
        return r;
    }

    private static String hex(byte[] bytes) {
        if (bytes == null) return "";
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) sb.append(String.format(java.util.Locale.US, "%02x", b));
        return sb.toString();
    }

    private static byte[] unhex(String s) {
        if (s == null || (s.length() % 2) != 0) throw new IllegalArgumentException("Invalid hex string");
        byte[] out = new byte[s.length() / 2];
        for (int i = 0; i < out.length; i++) {
            out[i] = (byte) Integer.parseInt(s.substring(i * 2, i * 2 + 2), 16);
        }
        return out;
    }

    /** One open client connection; writes are synchronized so events/responses do not interleave. */
    private static class ClientConnection {
        private final LocalSocket mSocket;
        private final BufferedWriter mWriter;
        private volatile boolean mClosed = false;

        ClientConnection(LocalSocket socket) throws IOException {
            this.mSocket = socket;
            this.mWriter = new BufferedWriter(
                    new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8));
        }

        synchronized void send(JSONObject obj) {
            if (obj == null) return;
            sendRaw(obj.toString());
        }

        synchronized void sendRaw(String line) {
            if (mClosed) return;
            try {
                mWriter.write(line);
                mWriter.newLine();
                mWriter.flush();
            } catch (IOException e) {
                Log.e(TAG, "send failed", e);
                close();
            }
        }

        synchronized void close() {
            if (mClosed) return;
            mClosed = true;
            try { mWriter.close(); } catch (IOException ignored) {}
            try { mSocket.close(); } catch (IOException ignored) {}
        }
    }

    public void stop() {
        mRunning = false;
        for (ClientConnection c : mClients) c.close();
        mClients.clear();
        mManager.destroy();
        mWorkers.shutdownNow();
        if (mAcceptThread != null) mAcceptThread.interrupt();
    }
}
