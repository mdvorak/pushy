package com.relayrides.pushy.apns;

import java.io.IOException;

/**
 * Failed to establish connection with proxy server.
 *
 * @author cen38289
 * @since 19/06/14
 */
public class ProxyConnectionException extends IOException {

    public ProxyConnectionException() {
    }

    public ProxyConnectionException(String message) {
        super(message);
    }

    public ProxyConnectionException(String message, Throwable cause) {
        super(message, cause);
    }

    public ProxyConnectionException(Throwable cause) {
        super(cause);
    }
}
