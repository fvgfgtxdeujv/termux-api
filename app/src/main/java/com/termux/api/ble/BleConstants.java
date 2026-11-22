package com.termux.api.ble;

/**
 * Shared protocol constants for the Termux:API BLE bridge.
 *
 * Transport: an Android {@code LocalServerSocket} in the ABSTRACT namespace.
 * Framing:   newline-delimited UTF-8 JSON (one JSON object per line).
 *
 * Request:  {"id":1,"cmd":"scan.start","params":{...}}
 * Response: {"id":1,"ok":true,"result":{...}}
 *           {"id":1,"ok":false,"error":{"code":"...","message":"..."}}
 * Event:    {"event":"advertisement","params":{...}}   (no "id")
 */
public final class BleConstants {

    private BleConstants() {}

    /** Abstract-namespace socket name. Python clients connect to b"\0" + this name. */
    public static final String SOCKET_NAME = "com.termux.api.ble";

    public static final String KEY_ID = "id";
    public static final String KEY_CMD = "cmd";
    public static final String KEY_PARAMS = "params";
    public static final String KEY_OK = "ok";
    public static final String KEY_RESULT = "result";
    public static final String KEY_ERROR = "error";
    public static final String KEY_EVENT = "event";

    // error codes
    public static final String ERR_BAD_REQUEST = "bad_request";
    public static final String ERR_UNKNOWN_COMMAND = "unknown_command";
    public static final String ERR_PERMISSION = "permission_denied";
    public static final String ERR_NO_BLUETOOTH = "bluetooth_unavailable";
    public static final String ERR_NOT_CONNECTED = "not_connected";
    public static final String ERR_NOT_FOUND = "not_found";
    public static final String ERR_OPERATION = "operation_failed";
    public static final String ERR_TIMEOUT = "timeout";
    public static final String ERR_INTERNAL = "internal_error";
    public static final String ERR_UNAUTHORIZED = "unauthorized";
}
