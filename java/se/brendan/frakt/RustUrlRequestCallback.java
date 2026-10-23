package se.brendan.frakt;

import org.chromium.net.UrlRequest;
import org.chromium.net.UrlResponseInfo;
import org.chromium.net.CronetException;
import java.nio.ByteBuffer;

/**
 * UrlRequest.Callback that forwards every callback to frakt's Rust code with
 * the request's handler id. The natives are registered by frakt when it loads
 * this class from its embedded DEX.
 *
 * Reading is driven from Rust: no callback here calls UrlRequest.read().
 */
public class RustUrlRequestCallback extends UrlRequest.Callback {
    private final long handlerId;

    public RustUrlRequestCallback(long handlerId) {
        this.handlerId = handlerId;
    }

    @Override
    public void onRedirectReceived(UrlRequest request, UrlResponseInfo info, String newLocationUrl) {
        nativeOnRedirectReceived(handlerId, request, info);
    }

    @Override
    public void onResponseStarted(UrlRequest request, UrlResponseInfo info) {
        nativeOnResponseStarted(handlerId, request, info);
    }

    @Override
    public void onReadCompleted(UrlRequest request, UrlResponseInfo info, ByteBuffer byteBuffer) {
        // The data is at [0, position). Clearing first lets Rust hand the
        // same buffer to the next read() as soon as it has copied the data.
        int length = byteBuffer.position();
        byteBuffer.clear();
        nativeOnReadCompleted(handlerId, request, info, byteBuffer, length);
    }

    @Override
    public void onSucceeded(UrlRequest request, UrlResponseInfo info) {
        nativeOnSucceeded(handlerId, request, info);
    }

    @Override
    public void onFailed(UrlRequest request, UrlResponseInfo info, CronetException error) {
        nativeOnFailed(handlerId, request, info, error);
    }

    @Override
    public void onCanceled(UrlRequest request, UrlResponseInfo info) {
        nativeOnCanceled(handlerId, request, info);
    }

    private static native void nativeOnRedirectReceived(long handlerId, UrlRequest request, UrlResponseInfo info);

    private static native void nativeOnResponseStarted(long handlerId, UrlRequest request, UrlResponseInfo info);

    private static native void nativeOnReadCompleted(
        long handlerId, UrlRequest request, UrlResponseInfo info, ByteBuffer byteBuffer, int length);

    private static native void nativeOnSucceeded(long handlerId, UrlRequest request, UrlResponseInfo info);

    private static native void nativeOnFailed(
        long handlerId, UrlRequest request, UrlResponseInfo info, CronetException error);

    private static native void nativeOnCanceled(long handlerId, UrlRequest request, UrlResponseInfo info);
}
