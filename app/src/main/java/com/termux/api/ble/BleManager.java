package com.termux.api.ble;

import android.Manifest;
import android.annotation.SuppressLint;
import android.bluetooth.BluetoothAdapter;
import android.bluetooth.BluetoothDevice;
import android.bluetooth.BluetoothGatt;
import android.bluetooth.BluetoothGattCallback;
import android.bluetooth.BluetoothGattCharacteristic;
import android.bluetooth.BluetoothGattDescriptor;
import android.bluetooth.BluetoothGattService;
import android.bluetooth.BluetoothManager;
import android.bluetooth.BluetoothProfile;
import android.bluetooth.le.BluetoothLeScanner;
import android.bluetooth.le.ScanCallback;
import android.bluetooth.le.ScanFilter;
import android.bluetooth.le.ScanResult;
import android.bluetooth.le.ScanSettings;
import android.content.Context;
import android.content.pm.PackageManager;
import android.os.Build;
import android.os.Handler;
import android.os.HandlerThread;
import android.util.Log;

import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Core BLE manager supporting MULTIPLE simultaneous device connections.
 *
 * Each device gets its own {@link BleConnection} holding an independent
 * BluetoothGatt, lock, pending-operation latches and callback, so operations
 * on one device never clobber another's state. All GATT API calls are posted
 * to a single HandlerThread (Android requires GATT calls from one thread),
 * while callers wait on their connection's latch off-thread.
 *
 * Async results (scan results, notifications, connection state) are pushed
 * through the {@link EventSink}.
 */
public class BleManager {

    private static final String TAG = "BleManager";
    public static final UUID CCCD_UUID = UUID.fromString("00002902-0000-1000-8000-00805f9b34fb");

    /** Where scan results, notifications and connection changes are delivered. */
    public interface EventSink {
        void onEvent(String name, JSONObject params);
    }

    private final Context mContext;
    private final BluetoothManager mBluetoothManager;
    private final BluetoothAdapter mAdapter;
    private final Handler mGattHandler;
    private final EventSink mSink;

    /** Timers for heartbeat and reconnect backoff. */
    private final ScheduledExecutorService mTimer = Executors.newScheduledThreadPool(2, r -> {
        Thread t = new Thread(r, "BleManager-timer");
        t.setDaemon(true);
        return t;
    });

    /** Global defaults for auto-reconnect / heartbeat (overridable per connection). */
    private volatile boolean mAutoReconnectDefault = false;
    private volatile int mMaxReconnectAttempts = 5;
    private volatile long mReconnectDelayMs = 1000;
    private volatile long mHeartbeatIntervalMs = 0;   // 0 = disabled
    private volatile int mHeartbeatFailThreshold = 3;

    /** address (lower-case) -> connection */
    private final ConcurrentHashMap<String, BleConnection> mConnections = new ConcurrentHashMap<>();

    // ---- global scan state (a single scanner is shared) ----
    private BluetoothLeScanner mScanner;
    private ScanCallback mScanCallback;
    private volatile boolean mScanning = false;

    public BleManager(Context context, EventSink sink) {
        this.mContext = context.getApplicationContext();
        this.mSink = sink;
        this.mBluetoothManager = (BluetoothManager) mContext.getSystemService(Context.BLUETOOTH_SERVICE);
        this.mAdapter = mBluetoothManager != null ? mBluetoothManager.getAdapter() : null;

        HandlerThread thread = new HandlerThread("BleManager-GATT", android.os.Process.THREAD_PRIORITY_BACKGROUND);
        thread.start();
        this.mGattHandler = new Handler(thread.getLooper());
    }

    // -------------------------------------------------- reconnect / heartbeat config

    /** Globally enable/disable auto-reconnect for new connections. */
    public void setAutoReconnect(boolean enabled) { mAutoReconnectDefault = enabled; }

    /** Configure global reconnect/heartbeat behavior. */
    public JSONObject setReconnectConfig(boolean autoReconnect, int maxAttempts,
                                         long delayMs, long heartbeatMs, int heartbeatFails) {
        mAutoReconnectDefault = autoReconnect;
        if (maxAttempts >= 0) mMaxReconnectAttempts = maxAttempts;
        if (delayMs > 0) mReconnectDelayMs = delayMs;
        if (heartbeatMs >= 0) mHeartbeatIntervalMs = heartbeatMs;
        if (heartbeatFails > 0) mHeartbeatFailThreshold = heartbeatFails;
        // Apply heartbeat setting to already-connected devices.
        for (BleConnection c : mConnections.values()) {
            if (c.connected) scheduleHeartbeat(c);
            else cancelHeartbeat(c);
        }
        return getReconnectConfig();
    }

    public JSONObject getReconnectConfig() {
        JSONObject o = new JSONObject();
        try {
            o.put("autoReconnect", mAutoReconnectDefault);
            o.put("maxAttempts", mMaxReconnectAttempts);
            o.put("delayMs", mReconnectDelayMs);
            o.put("heartbeatIntervalMs", mHeartbeatIntervalMs);
            o.put("heartbeatFailThreshold", mHeartbeatFailThreshold);
        } catch (Exception ignored) {}
        return o;
    }

    /** Enable/disable auto-reconnect for one connection (does not disconnect it). */
    public void setAutoReconnect(String address, boolean enabled) throws BleException {
        String addr = normalize(address);
        BleConnection c = mConnections.get(addr);
        if (c == null) throw new BleException(BleConstants.ERR_NOT_FOUND, "No connection for " + addr);
        c.autoReconnect = enabled;
        if (!enabled) {
            c.reconnectAttempts = 0;
            cancelReconnect(c);
        }
    }

    // ------------------------------------------------------------------ state

    public JSONObject getState() {
        JSONObject o = new JSONObject();
        try {
            o.put("hasBluetoothHardware", mContext.getPackageManager().hasSystemFeature(PackageManager.FEATURE_BLUETOOTH_LE));
            o.put("adapterPresent", mAdapter != null);
            o.put("enabled", mAdapter != null && mAdapter.isEnabled());
            o.put("permissionGranted", BlePermissionActivity.hasPermissions(mContext));
            o.put("scanning", mScanning);
            JSONArray conns = new JSONArray();
            for (BleConnection c : mConnections.values()) conns.put(c.toJson());
            o.put("connections", conns);
            // Backwards-compatible fields describing the "default" connection.
            BleConnection first = firstConnected();
            o.put("connected", first != null);
            o.put("address", first != null ? first.address : JSONObject.NULL);
        } catch (Exception e) {
            Log.e(TAG, "getState", e);
        }
        return o;
    }

    /** List all known connections and their state. */
    public JSONArray listConnections() {
        JSONArray arr = new JSONArray();
        for (BleConnection c : mConnections.values()) arr.put(c.toJson());
        return arr;
    }

    private BleConnection firstConnected() {
        for (BleConnection c : mConnections.values()) {
            if (c.connected && c.gatt != null) return c;
        }
        return null;
    }

    private void requireAdapter() throws BleException {
        if (mAdapter == null) {
            throw new BleException(BleConstants.ERR_NO_BLUETOOTH, "No Bluetooth adapter on this device");
        }
        if (!mAdapter.isEnabled()) {
            throw new BleException(BleConstants.ERR_NO_BLUETOOTH, "Bluetooth is turned off");
        }
    }

    private static String normalize(String address) {
        return address == null ? null : address.trim().toLowerCase(Locale.US);
    }

    /** Resolve an address: explicit, else the sole connection, else error. */
    private BleConnection resolve(String address) throws BleException {
        String addr = normalize(address);
        if (addr != null) {
            BleConnection c = mConnections.get(addr);
            if (c == null || c.gatt == null || !c.connected) {
                throw new BleException(BleConstants.ERR_NOT_CONNECTED, "Not connected to " + addr);
            }
            return c;
        }
        // No explicit address: legacy compatibility.
        List<BleConnection> live = new ArrayList<>();
        for (BleConnection c : mConnections.values()) {
            if (c.connected && c.gatt != null) live.add(c);
        }
        if (live.isEmpty()) {
            throw new BleException(BleConstants.ERR_NOT_CONNECTED, "Not connected to any device");
        }
        if (live.size() > 1) {
            throw new BleException(BleConstants.ERR_BAD_REQUEST,
                    "Multiple devices connected (" + live.size() + "); specify 'address'");
        }
        return live.get(0);
    }

    // ------------------------------------------------------------------ scan

    public void startScan(long timeoutMs, List<String> uuidFilters) throws BleException {
        requireAdapter();
        if (!BlePermissionActivity.hasPermissions(mContext)) {
            throw new BleException(BleConstants.ERR_PERMISSION,
                    "BLE permissions not granted; call request_permissions first");
        }
        synchronized (this) {
            if (mScanning) stopScanInternal();
        }
        mGattHandler.post(() -> {
            try {
                mScanner = mAdapter.getBluetoothLeScanner();
                if (mScanner == null) {
                    emitError("bluetooth_unavailable", "BLE scanner unavailable");
                    return;
                }

                List<ScanFilter> filters = new ArrayList<>();
                if (uuidFilters != null && !uuidFilters.isEmpty()) {
                    for (String s : uuidFilters) {
                        filters.add(new ScanFilter.Builder()
                                .setServiceUuid(android.os.ParcelUuid.fromString(s))
                                .build());
                    }
                }

                ScanSettings settings = new ScanSettings.Builder()
                        .setScanMode(ScanSettings.SCAN_MODE_LOW_LATENCY)
                        .build();

                mScanCallback = new ScanCallback() {
                    @Override
                    public void onScanResult(int callbackType, ScanResult result) {
                        try {
                            mSink.onEvent("advertisement", scanResultToJson(result));
                        } catch (Exception e) {
                            Log.e(TAG, "onScanResult", e);
                        }
                    }

                    @Override
                    public void onScanFailed(int errorCode) {
                        JSONObject p = new JSONObject();
                        try { p.put("errorCode", errorCode); } catch (Exception ignored) {}
                        mSink.onEvent("scan_failed", p);
                    }
                };

                if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.S &&
                        mContext.checkSelfPermission(Manifest.permission.BLUETOOTH_SCAN) != PackageManager.PERMISSION_GRANTED) {
                    emitError(BleConstants.ERR_PERMISSION, "BLUETOOTH_SCAN permission missing");
                    return;
                }

                mScanner.startScan(filters, settings, mScanCallback);
                mScanning = true;
                mSink.onEvent("scan_started", new JSONObject());

                if (timeoutMs > 0) {
                    mGattHandler.postDelayed(() -> {
                        if (mScanning) stopScanInternal();
                    }, timeoutMs);
                }
            } catch (Throwable t) {
                Log.e(TAG, "startScan failed", t);
                emitError(BleConstants.ERR_OPERATION, "startScan: " + t.getMessage());
            }
        });
    }

    public void stopScan() {
        mGattHandler.post(this::stopScanInternal);
    }

    private void stopScanInternal() {
        try {
            if (mScanner != null && mScanCallback != null && mScanning) {
                if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.S &&
                        mContext.checkSelfPermission(Manifest.permission.BLUETOOTH_SCAN) != PackageManager.PERMISSION_GRANTED) {
                    return;
                }
                mScanner.stopScan(mScanCallback);
            }
        } catch (Throwable t) {
            Log.e(TAG, "stopScan", t);
        } finally {
            mScanning = false;
            mScanCallback = null;
            mSink.onEvent("scan_stopped", new JSONObject());
        }
    }

    private JSONObject scanResultToJson(ScanResult r) {
        JSONObject o = new JSONObject();
        try {
            BluetoothDevice d = r.getDevice();
            o.put("address", d.getAddress());
            o.put("name", d.getName() != null ? d.getName() : JSONObject.NULL);
            o.put("rssi", r.getRssi());
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
                o.put("isConnectable", r.isConnectable());
            }
            if (r.getScanRecord() != null) {
                o.put("data", toHex(r.getScanRecord().getBytes()));
            }
        } catch (Exception e) {
            Log.e(TAG, "scanResultToJson", e);
        }
        return o;
    }

    // --------------------------------------------------------------- connect

    /** Connect to a device. If already connected, this is a no-op. */
    public void connect(String address, int timeoutMs) throws BleException {
        requireAdapter();
        if (!BlePermissionActivity.hasPermissions(mContext)) {
            throw new BleException(BleConstants.ERR_PERMISSION, "BLE permissions not granted");
        }
        String addr = normalize(address);
        if (addr == null) {
            throw new BleException(BleConstants.ERR_BAD_REQUEST, "Missing address");
        }

        BleConnection conn = mConnections.get(addr);
        if (conn != null && conn.connected && conn.gatt != null) {
            return; // already connected
        }
        if (conn == null) {
            BluetoothDevice device;
            try {
                device = mAdapter.getRemoteDevice(address);
            } catch (IllegalArgumentException e) {
                throw new BleException(BleConstants.ERR_BAD_REQUEST, "Invalid address: " + address);
            }
            conn = new BleConnection(addr, device);
            mConnections.put(addr, conn);
        }

        // Manual connect is an explicit user action: enable auto-reconnect here.
        conn.autoReconnect = mAutoReconnectDefault;
        conn.userDisconnected = false;
        conn.reconnectAttempts = 0;
        cancelReconnect(conn);

        doConnect(conn, timeoutMs);
    }

    /**
     * Perform the actual GATT connect and wait for the result. Does not itself
     * change auto-reconnect flags (used by both manual connect and reconnect).
     */
    private void doConnect(final BleConnection c, int timeoutMs) throws BleException {
        c.lock.lock();
        try {
            c.connectOk = false;
            c.connectLatch = new CountDownLatch(1);
            final CountDownLatch latch = c.connectLatch;
            final String[] err = {null};

            mGattHandler.post(() -> {
                try {
                    if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.S &&
                            mContext.checkSelfPermission(Manifest.permission.BLUETOOTH_CONNECT) != PackageManager.PERMISSION_GRANTED) {
                        err[0] = "BLUETOOTH_CONNECT permission missing";
                        if (c.connectLatch != null) c.connectLatch.countDown();
                        return;
                    }
                    // A previous GATT may be closed but not nulled by the callback.
                    if (c.gatt != null) {
                        try { c.gatt.close(); } catch (Exception ignored) {}
                        c.gatt = null;
                    }
                    BluetoothGatt gatt = c.device.connectGatt(mContext, false, c.callback,
                            BluetoothDevice.TRANSPORT_LE);
                    c.gatt = gatt;
                    if (gatt == null) {
                        err[0] = "connectGatt returned null";
                        if (c.connectLatch != null) c.connectLatch.countDown();
                    }
                } catch (Throwable t) {
                    err[0] = t.getMessage();
                    if (c.connectLatch != null) c.connectLatch.countDown();
                }
            });

            boolean done;
            try {
                done = latch.await(timeoutMs, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new BleException(BleConstants.ERR_TIMEOUT, "connect interrupted");
            }
            if (!done) {
                closeConnection(c, false);
                throw new BleException(BleConstants.ERR_TIMEOUT, "connect timed out");
            }
            if (!c.connectOk) {
                closeConnection(c, false);
                throw new BleException(BleConstants.ERR_OPERATION,
                        err[0] != null ? err[0] : "Connection failed");
            }
            // Connection succeeded: reset backoff, start heartbeat.
            c.reconnectAttempts = 0;
            cancelReconnect(c);
            scheduleHeartbeat(c);
        } finally {
            c.connectLatch = null;
            c.lock.unlock();
        }
    }

    /** Disconnect one device (address optional; resolves like {@link #resolve}). */
    public void disconnect(String address) throws BleException {
        BleConnection c = resolve(address);
        // Mark as user-initiated so no reconnect is attempted.
        mGattHandler.post(() -> {
            c.userDisconnected = true;
            cancelReconnect(c);
            cancelHeartbeat(c);
            closeConnection(c, false);
        });
    }

    /** Disconnect every device and clear the pool. */
    public void disconnectAll() {
        mGattHandler.post(() -> {
            for (BleConnection c : mConnections.values()) {
                c.userDisconnected = true;
                cancelReconnect(c);
                cancelHeartbeat(c);
                closeConnection(c, false);
            }
            mConnections.clear();
        });
    }

    /**
     * Close the GATT connection and reset per-connection transient state.
     * @param unexpected true when the link dropped on its own (triggers reconnect)
     */
    private void closeConnection(BleConnection c, boolean unexpected) {
        cancelHeartbeat(c);
        try {
            if (c.gatt != null) {
                if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.S &&
                        mContext.checkSelfPermission(Manifest.permission.BLUETOOTH_CONNECT) != PackageManager.PERMISSION_GRANTED) {
                    // still fall through to state reset
                } else {
                    c.gatt.disconnect();
                    c.gatt.close();
                }
            }
        } catch (Throwable t) {
            Log.e(TAG, "disconnect " + c.address, t);
        } finally {
            c.gatt = null;
            c.connected = false;
            synchronized (c.subscribed) {
                // Keep a copy so we can restore subscriptions after reconnect.
                c.subscriptionsToRestore.clear();
                c.subscriptionsToRestore.addAll(c.subscribed);
                c.subscribed.clear();
            }
            // Wake any operation still waiting on this connection.
            if (c.waitLatch != null) c.waitLatch.countDown();
            if (c.servicesLatch != null) c.servicesLatch.countDown();
            if (c.connectLatch != null) c.connectLatch.countDown();

            if (unexpected && c.autoReconnect && !c.userDisconnected) {
                scheduleReconnect(c);
            }
        }
    }

    // ---------------------------------------------------- heartbeat + reconnect

    /** Periodically read RSSI; consecutive failures mark the link dead. */
    private void scheduleHeartbeat(final BleConnection c) {
        if (mHeartbeatIntervalMs <= 0) return;
        cancelHeartbeat(c);
        c.heartbeatFails = 0;
        c.heartbeatTask = mTimer.scheduleAtFixedRate(() -> {
            if (!c.connected || c.gatt == null || c.userDisconnected) {
                cancelHeartbeat(c);
                return;
            }
            // Skip if another operation currently holds the lock, to avoid piling up.
            if (!c.lock.tryLock()) return;
            try {
                c.waitLatch = new CountDownLatch(1);
                c.waitResult = false;
                mGattHandler.post(() -> {
                    try {
                        boolean ok = c.gatt != null && c.gatt.readRemoteRssi();
                        if (!ok) { c.waitResult = false; c.waitLatch.countDown(); }
                    } catch (Throwable t) {
                        c.waitResult = false;
                        if (c.waitLatch != null) c.waitLatch.countDown();
                    }
                });
                boolean ok = c.waitLatch.await(5, TimeUnit.SECONDS);
                if (ok && c.waitResult) {
                    c.heartbeatFails = 0;
                    JSONObject p = new JSONObject();
                    try { p.put("address", c.address); p.put("rssi", c.waitStatus); } catch (Exception ignored) {}
                    mSink.onEvent("heartbeat", p);
                } else {
                    c.heartbeatFails++;
                    Log.w(TAG, "Heartbeat failed for " + c.address + " (" + c.heartbeatFails + ")");
                    if (c.heartbeatFails >= mHeartbeatFailThreshold) {
                        cancelHeartbeat(c);
                        // Treat as a dropped link.
                        mGattHandler.post(() -> closeConnection(c, true));
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                c.waitLatch = null;
                c.lock.unlock();
            }
        }, mHeartbeatIntervalMs, mHeartbeatIntervalMs, TimeUnit.MILLISECONDS);
    }

    private void cancelHeartbeat(BleConnection c) {
        ScheduledFuture<?> f = c.heartbeatTask;
        if (f != null) { f.cancel(false); c.heartbeatTask = null; }
    }

    /** Schedule a reconnect with exponential backoff + jitter. */
    private void scheduleReconnect(final BleConnection c) {
        cancelReconnect(c);
        if (!c.autoReconnect || c.userDisconnected) return;
        if (c.reconnectAttempts >= mMaxReconnectAttempts) {
            Log.w(TAG, "Giving up reconnect for " + c.address + " after " + c.reconnectAttempts + " attempts");
            JSONObject p = new JSONObject();
            try { p.put("address", c.address); p.put("reason", "max_attempts"); } catch (Exception ignored) {}
            mSink.onEvent("reconnect_failed", p);
            return;
        }
        int attempt = c.reconnectAttempts;
        long backoff = mReconnectDelayMs * (1L << Math.min(attempt, 6)); // cap the shift
        long jitter = ThreadLocalRandom.current().nextLong(0, Math.max(1, backoff / 4));
        final long delay = backoff + jitter;
        c.reconnectAttempts++;

        JSONObject p = new JSONObject();
        try {
            p.put("address", c.address);
            p.put("attempt", c.reconnectAttempts);
            p.put("delayMs", delay);
        } catch (Exception ignored) {}
        mSink.onEvent("reconnecting", p);

        c.reconnectTask = mTimer.schedule(() -> {
            c.reconnectTask = null;
            if (c.userDisconnected || !c.autoReconnect) return;
            try {
                doConnect(c, 10000);
                // Restore subscriptions that were active before the drop.
                restoreSubscriptions(c);
                JSONObject ok = new JSONObject();
                try { ok.put("address", c.address); ok.put("attempt", c.reconnectAttempts); } catch (Exception ignored) {}
                mSink.onEvent("reconnected", ok);
            } catch (BleException e) {
                Log.w(TAG, "Reconnect attempt " + c.reconnectAttempts + " failed for " + c.address + ": " + e.getMessage());
                // If still eligible, schedule the next attempt.
                mGattHandler.post(() -> {
                    if (c.autoReconnect && !c.userDisconnected && !c.connected) {
                        closeConnection(c, true);
                    }
                });
            }
        }, delay, TimeUnit.MILLISECONDS);
    }

    private void cancelReconnect(BleConnection c) {
        ScheduledFuture<?> f = c.reconnectTask;
        if (f != null) { f.cancel(false); c.reconnectTask = null; }
    }

    /** Re-enable notifications for characteristics subscribed before disconnect. */
    private void restoreSubscriptions(BleConnection c) {
        java.util.List<String> toRestore;
        synchronized (c.subscribed) {
            toRestore = new ArrayList<>(c.subscriptionsToRestore);
        }
        for (String uuid : toRestore) {
            try {
                subscribe(c.address, uuid, 10000);
            } catch (BleException e) {
                Log.w(TAG, "Failed to restore subscription " + uuid + " on " + c.address, e);
            }
        }
    }

    // --------------------------------------------------------------- services

    public JSONArray getServices(String address, int timeoutMs) throws BleException {
        final BleConnection c = resolve(address);
        final CountDownLatch latch = new CountDownLatch(1);
        final JSONArray[] out = {null};
        final String[] err = {null};

        c.lock.lock();
        try {
            c.servicesLatch = latch;
            c.servicesOut = out;
            c.waitLatch = null;

            mGattHandler.post(() -> {
                try {
                    boolean started = c.gatt != null && c.gatt.discoverServices();
                    if (!started) {
                        err[0] = "discoverServices failed to start";
                        latch.countDown();
                    }
                } catch (Throwable t) {
                    err[0] = t.getMessage();
                    latch.countDown();
                }
            });

            try {
                if (!latch.await(timeoutMs, TimeUnit.MILLISECONDS)) {
                    throw new BleException(BleConstants.ERR_TIMEOUT, "service discovery timed out");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new BleException(BleConstants.ERR_TIMEOUT, "service discovery interrupted");
            }
            if (err[0] != null) throw new BleException(BleConstants.ERR_OPERATION, err[0]);
            return out[0] != null ? out[0] : new JSONArray();
        } finally {
            c.servicesLatch = null;
            c.servicesOut = null;
            c.lock.unlock();
        }
    }

    // ------------------------------------------------------------ read/write

    public byte[] readCharacteristic(String address, String uuidStr, int timeoutMs) throws BleException {
        final BleConnection c = resolve(address);
        BluetoothGattCharacteristic ch = c.findCharacteristic(uuidStr);
        if (ch == null) throw new BleException(BleConstants.ERR_NOT_FOUND, "Characteristic not found: " + uuidStr);

        c.lock.lock();
        try {
            c.waitLatch = new CountDownLatch(1);
            c.waitResult = false;

            mGattHandler.post(() -> {
                try {
                    boolean ok = c.gatt != null && c.gatt.readCharacteristic(ch);
                    if (!ok) { c.waitResult = false; c.waitLatch.countDown(); }
                } catch (Throwable t) {
                    Log.e(TAG, "readCharacteristic", t);
                    c.waitResult = false;
                    c.waitLatch.countDown();
                }
            });

            awaitWait(c, timeoutMs, "read");
            byte[] v = c.lastValue;
            return v != null ? v : new byte[0];
        } finally {
            c.waitLatch = null;
            c.lock.unlock();
        }
    }

    public void writeCharacteristic(String address, String uuidStr, byte[] value,
                                    boolean withResponse, int timeoutMs) throws BleException {
        final BleConnection c = resolve(address);
        BluetoothGattCharacteristic ch = c.findCharacteristic(uuidStr);
        if (ch == null) throw new BleException(BleConstants.ERR_NOT_FOUND, "Characteristic not found: " + uuidStr);
        if (value == null) throw new BleException(BleConstants.ERR_BAD_REQUEST, "Missing value");

        int writeType = withResponse
                ? BluetoothGattCharacteristic.WRITE_TYPE_DEFAULT
                : BluetoothGattCharacteristic.WRITE_TYPE_NO_RESPONSE;

        c.lock.lock();
        try {
            c.waitLatch = new CountDownLatch(1);
            c.waitResult = false;

            mGattHandler.post(() -> {
                try {
                    boolean ok;
                    if (c.gatt == null) { ok = false; }
                    else if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
                        int r = c.gatt.writeCharacteristic(ch, value, writeType);
                        ok = r == BluetoothGatt.GATT_SUCCESS;
                        c.waitStatus = r;
                    } else {
                        ch.setWriteType(writeType);
                        ch.setValue(value);
                        ok = c.gatt.writeCharacteristic(ch);
                    }
                    if (!ok) { c.waitResult = false; c.waitLatch.countDown(); }
                } catch (Throwable t) {
                    Log.e(TAG, "writeCharacteristic", t);
                    c.waitResult = false;
                    c.waitLatch.countDown();
                }
            });

            awaitWait(c, timeoutMs, "write");
        } finally {
            c.waitLatch = null;
            c.lock.unlock();
        }
    }

    private void awaitWait(BleConnection c, int timeoutMs, String what) throws BleException {
        CountDownLatch latch = c.waitLatch;
        if (latch == null) return;
        try {
            if (!latch.await(timeoutMs, TimeUnit.MILLISECONDS)) {
                throw new BleException(BleConstants.ERR_TIMEOUT, what + " timed out");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new BleException(BleConstants.ERR_TIMEOUT, what + " interrupted");
        }
        if (!c.waitResult) {
            throw new BleException(BleConstants.ERR_OPERATION, what + " failed (status=" + c.waitStatus + ")");
        }
    }

    // -------------------------------------------------------------- subscribe

    public void subscribe(String address, String uuidStr, int timeoutMs) throws BleException {
        final BleConnection c = resolve(address);
        BluetoothGattCharacteristic ch = c.findCharacteristic(uuidStr);
        if (ch == null) throw new BleException(BleConstants.ERR_NOT_FOUND, "Characteristic not found: " + uuidStr);

        c.lock.lock();
        try {
            c.waitLatch = new CountDownLatch(1);
            c.waitResult = false;

            mGattHandler.post(() -> {
                try {
                    if (c.gatt == null) { c.waitResult = false; c.waitLatch.countDown(); return; }
                    boolean ok = c.gatt.setCharacteristicNotification(ch, true);
                    if (!ok) { c.waitResult = false; c.waitLatch.countDown(); return; }

                    BluetoothGattDescriptor cccd = ch.getDescriptor(CCCD_UUID);
                    if (cccd == null) {
                        c.waitResult = true;
                        c.waitLatch.countDown();
                        return;
                    }
                    byte[] val = BluetoothGattDescriptor.ENABLE_NOTIFICATION_VALUE;
                    if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
                        int r = c.gatt.writeDescriptor(cccd, val);
                        c.waitResult = r == BluetoothGatt.GATT_SUCCESS;
                        c.waitStatus = r;
                    } else {
                        cccd.setValue(val);
                        c.waitResult = c.gatt.writeDescriptor(cccd);
                    }
                    if (!c.waitResult) c.waitLatch.countDown();
                } catch (Throwable t) {
                    Log.e(TAG, "subscribe", t);
                    c.waitResult = false;
                    c.waitLatch.countDown();
                }
            });

            awaitWait(c, timeoutMs, "subscribe");
            c.subscribed.add(uuidStr.toLowerCase(Locale.US));
        } finally {
            c.waitLatch = null;
            c.lock.unlock();
        }
    }

    public void unsubscribe(String address, String uuidStr, int timeoutMs) throws BleException {
        final BleConnection c = resolve(address);
        BluetoothGattCharacteristic ch = c.findCharacteristic(uuidStr);
        if (ch == null) throw new BleException(BleConstants.ERR_NOT_FOUND, "Characteristic not found: " + uuidStr);

        c.lock.lock();
        try {
            c.waitLatch = new CountDownLatch(1);
            c.waitResult = false;
            mGattHandler.post(() -> {
                try {
                    if (c.gatt == null) { c.waitResult = false; c.waitLatch.countDown(); return; }
                    BluetoothGattDescriptor cccd = ch.getDescriptor(CCCD_UUID);
                    if (cccd == null) {
                        c.gatt.setCharacteristicNotification(ch, false);
                        c.waitResult = true;
                        c.waitLatch.countDown();
                        return;
                    }
                    byte[] val = BluetoothGattDescriptor.DISABLE_NOTIFICATION_VALUE;
                    if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
                        int r = c.gatt.writeDescriptor(cccd, val);
                        c.waitResult = r == BluetoothGatt.GATT_SUCCESS;
                    } else {
                        cccd.setValue(val);
                        c.waitResult = c.gatt.writeDescriptor(cccd);
                    }
                    if (c.waitResult) c.gatt.setCharacteristicNotification(ch, false);
                    if (!c.waitResult) c.waitLatch.countDown();
                } catch (Throwable t) {
                    Log.e(TAG, "unsubscribe", t);
                    c.waitResult = false;
                    c.waitLatch.countDown();
                }
            });
            awaitWait(c, timeoutMs, "unsubscribe");
            c.subscribed.remove(uuidStr.toLowerCase(Locale.US));
        } finally {
            c.waitLatch = null;
            c.lock.unlock();
        }
    }

    public void requestMtu(String address, int mtu, int timeoutMs) throws BleException {
        final BleConnection c = resolve(address);
        c.lock.lock();
        try {
            c.waitLatch = new CountDownLatch(1);
            c.waitResult = false;
            mGattHandler.post(() -> {
                try {
                    boolean ok = c.gatt != null && c.gatt.requestMtu(mtu);
                    if (!ok) { c.waitResult = false; c.waitLatch.countDown(); }
                } catch (Throwable t) {
                    c.waitResult = false;
                    c.waitLatch.countDown();
                }
            });
            awaitWait(c, timeoutMs, "requestMtu");
        } finally {
            c.waitLatch = null;
            c.lock.unlock();
        }
    }

    // ---------------------------------------------------------------- helpers

    private void emitError(String code, String message) {
        JSONObject p = new JSONObject();
        try { p.put("code", code); p.put("message", message); } catch (Exception ignored) {}
        mSink.onEvent("error", p);
    }

    private static String toHex(byte[] bytes) {
        if (bytes == null) return "";
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) sb.append(String.format(Locale.US, "%02x", b));
        return sb.toString();
    }

    // ============================================================ connection

    /** Per-device state: its own GATT, lock, latches, subscriptions and callback. */
    private final class BleConnection {
        final String address;
        final BluetoothDevice device;
        final ReentrantLock lock = new ReentrantLock(true);
        final Set<String> subscribed = new CopyOnWriteArraySet<>();
        final ConnCallback callback = new ConnCallback(this);

        BluetoothGatt gatt;
        volatile boolean connected = false;

        // --- reconnect / heartbeat state ---
        boolean autoReconnect = mAutoReconnectDefault;
        volatile boolean userDisconnected = false;
        volatile int reconnectAttempts = 0;
        volatile int heartbeatFails = 0;
        ScheduledFuture<?> heartbeatTask;
        ScheduledFuture<?> reconnectTask;
        /** UUIDs that were subscribed when the link dropped; restored on reconnect. */
        final Set<String> subscriptionsToRestore = new HashSet<>();

        volatile CountDownLatch waitLatch;
        volatile boolean waitResult;
        volatile int waitStatus;
        volatile byte[] lastValue;

        volatile CountDownLatch connectLatch;
        volatile boolean connectOk;

        volatile CountDownLatch servicesLatch;
        volatile JSONArray[] servicesOut;

        BleConnection(String address, BluetoothDevice device) {
            this.address = address;
            this.device = device;
        }

        BluetoothGattCharacteristic findCharacteristic(String uuidStr) {
            if (gatt == null) return null;
            UUID target;
            try { target = UUID.fromString(uuidStr); } catch (Exception e) { return null; }
            for (BluetoothGattService service : gatt.getServices()) {
                for (BluetoothGattCharacteristic ch : service.getCharacteristics()) {
                    if (target.equals(ch.getUuid())) return ch;
                }
            }
            return null;
        }

        JSONObject toJson() {
            JSONObject o = new JSONObject();
            try {
                o.put("address", address);
                o.put("connected", connected && gatt != null);
                o.put("name", device.getName() != null ? device.getName() : JSONObject.NULL);
                JSONArray subs = new JSONArray();
                synchronized (subscribed) {
                    for (String s : subscribed) subs.put(s);
                }
                o.put("subscriptions", subs);
                o.put("autoReconnect", autoReconnect);
                o.put("reconnectAttempts", reconnectAttempts);
                o.put("heartbeatFails", heartbeatFails);
            } catch (Exception ignored) {}
            return o;
        }
    }

    /** Callback bound to one connection; updates only that connection's state. */
    @SuppressLint("MissingPermission")
    private final class ConnCallback extends BluetoothGattCallback {
        final BleConnection c;

        ConnCallback(BleConnection c) { this.c = c; }

        @Override
        public void onConnectionStateChange(BluetoothGatt gatt, int status, int newState) {
            if (status == BluetoothGatt.GATT_SUCCESS && newState == BluetoothProfile.STATE_CONNECTED) {
                c.connected = true;
                c.reconnectAttempts = 0;
                JSONObject p = new JSONObject();
                try { p.put("address", c.address); p.put("state", "connected"); } catch (Exception ignored) {}
                mSink.onEvent("connection_state", p);
            } else {
                boolean wasConnected = c.connected;
                // Capture result for a pending connect() BEFORE resetting state.
                if (c.connectLatch != null) {
                    c.connectOk = false;
                    c.connectLatch.countDown();
                }
                JSONObject p = new JSONObject();
                try {
                    p.put("address", c.address);
                    p.put("state", "disconnected");
                    p.put("status", status);
                    p.put("unexpected", !c.userDisconnected);
                } catch (Exception ignored) {}
                if (wasConnected || newState == BluetoothProfile.STATE_DISCONNECTED) {
                    mSink.onEvent("connection_state", p);
                }
                // Unexpected drop on an auto-reconnect connection: route through
                // closeConnection so subscriptions are saved and reconnect is scheduled.
                boolean unexpected = !c.userDisconnected && c.connected;
                if (c.gatt == gatt) {
                    // Let closeConnection handle gatt.close() and state reset.
                    closeConnection(c, unexpected);
                } else {
                    try { gatt.close(); } catch (Exception ignored) {}
                }
            }
            if (c.connectLatch != null && status == BluetoothGatt.GATT_SUCCESS &&
                    newState == BluetoothProfile.STATE_CONNECTED) {
                c.connectOk = true;
                c.connectLatch.countDown();
            }
        }

        @Override
        public void onServicesDiscovered(BluetoothGatt gatt, int status) {
            CountDownLatch latch = c.servicesLatch;
            JSONArray[] out = c.servicesOut;
            if (status == BluetoothGatt.GATT_SUCCESS && out != null && out[0] == null) {
                JSONArray services = new JSONArray();
                try {
                    for (BluetoothGattService svc : gatt.getServices()) {
                        JSONObject sj = new JSONObject();
                        sj.put("uuid", svc.getUuid().toString().toLowerCase(Locale.US));
                        sj.put("type", svc.getType());
                        JSONArray chars = new JSONArray();
                        for (BluetoothGattCharacteristic ch : svc.getCharacteristics()) {
                            JSONObject cj = new JSONObject();
                            cj.put("uuid", ch.getUuid().toString().toLowerCase(Locale.US));
                            cj.put("properties", ch.getProperties());
                            JSONArray descs = new JSONArray();
                            for (BluetoothGattDescriptor d : ch.getDescriptors()) {
                                descs.put(d.getUuid().toString().toLowerCase(Locale.US));
                            }
                            cj.put("descriptors", descs);
                            chars.put(cj);
                        }
                        sj.put("characteristics", chars);
                        services.put(sj);
                    }
                    out[0] = services;
                } catch (Exception e) {
                    Log.e(TAG, "onServicesDiscovered build json", e);
                }
            }
            if (latch != null) latch.countDown();
        }

        @Override
        public void onCharacteristicRead(BluetoothGatt gatt, BluetoothGattCharacteristic ch, byte[] value, int status) {
            c.lastValue = value;
            c.waitStatus = status;
            c.waitResult = status == BluetoothGatt.GATT_SUCCESS;
            if (c.waitLatch != null) c.waitLatch.countDown();
        }

        // Legacy signature (pre-Android 13).
        @Override
        public void onCharacteristicRead(BluetoothGatt gatt, BluetoothGattCharacteristic ch, int status) {
            c.lastValue = ch.getValue();
            c.waitStatus = status;
            c.waitResult = status == BluetoothGatt.GATT_SUCCESS;
            if (c.waitLatch != null) c.waitLatch.countDown();
        }

        @Override
        public void onCharacteristicWrite(BluetoothGatt gatt, BluetoothGattCharacteristic ch, int status) {
            c.waitStatus = status;
            c.waitResult = status == BluetoothGatt.GATT_SUCCESS;
            if (c.waitLatch != null) c.waitLatch.countDown();
        }

        @Override
        public void onCharacteristicChanged(BluetoothGatt gatt, BluetoothGattCharacteristic ch, byte[] value) {
            pushNotify(gatt, ch, value);
        }

        // Legacy signature (pre-Android 13).
        @Override
        public void onCharacteristicChanged(BluetoothGatt gatt, BluetoothGattCharacteristic ch) {
            pushNotify(gatt, ch, ch.getValue());
        }

        private void pushNotify(BluetoothGatt gatt, BluetoothGattCharacteristic ch, byte[] value) {
            try {
                JSONObject p = new JSONObject();
                p.put("address", c.address);
                p.put("uuid", ch.getUuid().toString().toLowerCase(Locale.US));
                p.put("value", toHex(value));
                mSink.onEvent("notify", p);
            } catch (Exception e) {
                Log.e(TAG, "onCharacteristicChanged", e);
            }
        }

        @Override
        public void onDescriptorWrite(BluetoothGatt gatt, BluetoothGattDescriptor d, int status) {
            c.waitStatus = status;
            c.waitResult = status == BluetoothGatt.GATT_SUCCESS;
            if (c.waitLatch != null) c.waitLatch.countDown();
        }

        @Override
        public void onMtuChanged(BluetoothGatt gatt, int mtu, int status) {
            c.waitStatus = status;
            c.waitResult = status == BluetoothGatt.GATT_SUCCESS;
            JSONObject p = new JSONObject();
            try { p.put("address", c.address); p.put("mtu", mtu); p.put("status", status); } catch (Exception ignored) {}
            mSink.onEvent("mtu_changed", p);
            if (c.waitLatch != null) c.waitLatch.countDown();
        }

        @Override
        public void onReadRemoteRssi(BluetoothGatt gatt, int rssi, int status) {
            // Heartbeat uses waitStatus to carry the RSSI value back.
            c.waitStatus = rssi;
            c.waitResult = status == BluetoothGatt.GATT_SUCCESS;
            if (c.waitLatch != null) c.waitLatch.countDown();
        }
    }

    /** BleException carries a stable error code back to the JSON-RPC layer. */
    public static class BleException extends Exception {
        public final String code;
        public BleException(String code, String message) {
            super(message);
            this.code = code;
        }
    }

    public void destroy() {
        for (BleConnection c : mConnections.values()) {
            c.userDisconnected = true;
            cancelReconnect(c);
            cancelHeartbeat(c);
        }
        disconnectAll();
        if (mScanning) stopScan();
        mTimer.shutdownNow();
    }
}
