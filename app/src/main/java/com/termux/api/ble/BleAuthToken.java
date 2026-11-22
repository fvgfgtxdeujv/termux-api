package com.termux.api.ble;

import android.content.Context;
import android.content.SharedPreferences;
import android.util.Log;

import java.security.SecureRandom;

/**
 * Manages the pairing token used to authenticate BLE bridge clients.
 *
 * This replaces the deprecated android:sharedUserId mechanism. Instead of
 * trusting processes by UID (which requires identical app signatures), the
 * bridge requires clients to present a secret token generated on first run.
 * The token is shown in the app's main Activity so the user can copy it into
 * Termux once; it is then remembered by the Python client.
 */
public final class BleAuthToken {

    private static final String TAG = "BleAuthToken";
    private static final String PREFS_NAME = "ble_bridge_auth";
    private static final String KEY_TOKEN = "token";
    private static final int TOKEN_BYTES = 16; // 128-bit

    private BleAuthToken() {}

    private static SharedPreferences prefs(Context context) {
        return context.getApplicationContext()
                .getSharedPreferences(PREFS_NAME, Context.MODE_PRIVATE);
    }

    /**
     * Get the current token, generating and persisting one on first use.
     */
    public static synchronized String getOrCreate(Context context) {
        SharedPreferences p = prefs(context);
        String token = p.getString(KEY_TOKEN, null);
        if (token == null || token.isEmpty()) {
            byte[] bytes = new byte[TOKEN_BYTES];
            new SecureRandom().nextBytes(bytes);
            token = toHex(bytes);
            p.edit().putString(KEY_TOKEN, token).apply();
            Log.i(TAG, "Generated new BLE bridge pairing token");
        }
        return token;
    }

    /** Rotate to a new token (invalidates all existing clients). */
    public static synchronized String rotate(Context context) {
        byte[] bytes = new byte[TOKEN_BYTES];
        new SecureRandom().nextBytes(bytes);
        String token = toHex(bytes);
        prefs(context).edit().putString(KEY_TOKEN, token).apply();
        Log.i(TAG, "Rotated BLE bridge pairing token");
        return token;
    }

    /** Constant-time comparison to avoid timing leaks. */
    public static boolean matches(String expected, String provided) {
        if (expected == null || provided == null) return false;
        byte[] a = expected.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        byte[] b = provided.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        if (a.length != b.length) return false;
        int diff = 0;
        for (int i = 0; i < a.length; i++) diff |= a[i] ^ b[i];
        return diff == 0;
    }

    private static String toHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) sb.append(String.format(java.util.Locale.US, "%02x", b));
        return sb.toString();
    }
}
