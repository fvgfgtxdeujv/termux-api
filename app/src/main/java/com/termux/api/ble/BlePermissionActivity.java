package com.termux.api.ble;

import android.Manifest;
import android.app.Activity;
import android.content.Context;
import android.content.Intent;
import android.content.pm.PackageManager;
import android.os.Build;
import android.util.Log;

import com.termux.shared.android.PermissionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * Invisible activity used to request BLE runtime permissions. Unlike the rest of
 * Termux:API, the BLE bridge is driven by a long-lived socket rather than an
 * ordered broadcast, so it cannot use {@code ResultReturner}. Instead the result
 * is delivered through a static callback.
 */
public class BlePermissionActivity extends Activity {

    private static final String TAG = "BlePermissionActivity";
    private static final int REQ_CODE = 0x424c45; // "BLE"
    private static final String EXTRA_PERMISSIONS = "com.termux.api.ble.extra.PERMISSIONS";

    private static volatile Consumer<Boolean> sPendingCallback;
    private static volatile boolean sInFlight = false;

    /**
     * Permissions required for BLE operations on the current Android version.
     */
    public static String[] requiredPermissions() {
        List<String> perms = new ArrayList<>();
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.S) { // Android 12+
            perms.add(Manifest.permission.BLUETOOTH_SCAN);
            perms.add(Manifest.permission.BLUETOOTH_CONNECT);
        } else if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.M) { // Android 6-11
            perms.add(Manifest.permission.ACCESS_FINE_LOCATION);
        }
        return perms.toArray(new String[0]);
    }

    public static boolean hasPermissions(Context context) {
        for (String p : requiredPermissions()) {
            if (!PermissionUtils.checkPermission(context, p)) return false;
        }
        return true;
    }

    /**
     * Ask for the given permissions. The callback receives true only if all were
     * granted. Only one request can be in flight at a time.
     */
    public static synchronized void request(Context context, String[] permissions, Consumer<Boolean> callback) {
        if (sPendingCallback != null) {
            // A previous request never delivered its result; replace it so we do not deadlock.
            Consumer<Boolean> stale = sPendingCallback;
            sPendingCallback = null;
            stale.accept(false);
        }
        sPendingCallback = callback;
        Intent intent = new Intent(context, BlePermissionActivity.class)
                .addFlags(Intent.FLAG_ACTIVITY_NEW_TASK)
                .putStringArrayExtra(EXTRA_PERMISSIONS, permissions);
        context.startActivity(intent);
    }

    @Override
    protected void onResume() {
        super.onResume();
        String[] perms = getIntent().getStringArrayExtra(EXTRA_PERMISSIONS);
        if (perms == null || perms.length == 0) {
            finish();
            return;
        }
        PermissionUtils.requestPermissions(this, perms, REQ_CODE);
    }

    @Override
    public void onRequestPermissionsResult(int requestCode, String[] permissions, int[] grantResults) {
        super.onRequestPermissionsResult(requestCode, permissions, grantResults);
        if (requestCode != REQ_CODE) return;

        boolean allGranted = true;
        if (grantResults.length == 0) {
            allGranted = false;
        } else {
            for (int r : grantResults) {
                if (r != PackageManager.PERMISSION_GRANTED) {
                    allGranted = false;
                    break;
                }
            }
        }
        Consumer<Boolean> cb;
        synchronized (BlePermissionActivity.class) {
            cb = sPendingCallback;
            sPendingCallback = null;
        }
        if (cb != null) {
            try {
                cb.accept(allGranted);
            } catch (Throwable t) {
                Log.e(TAG, "Error in permission callback", t);
            }
        }
        finish();
    }
}
