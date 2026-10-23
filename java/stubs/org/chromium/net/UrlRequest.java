package org.chromium.net;

import java.nio.ByteBuffer;

/**
 * Stub for Cronet's UrlRequest class.
 * Only used at compile time - real implementation provided by Cronet at runtime.
 */
public abstract class UrlRequest {

    /**
     * Stub for UrlRequest.Callback abstract class.
     * Our RustUrlRequestCallback will extend this.
     */
    public abstract static class Callback {

        /**
         * Called when redirect is received.
         */
        public abstract void onRedirectReceived(
            UrlRequest request,
            UrlResponseInfo info,
            String newLocationUrl
        ) throws Exception;

        /**
         * Called when response headers are received.
         */
        public abstract void onResponseStarted(
            UrlRequest request,
            UrlResponseInfo info
        ) throws Exception;

        /**
         * Called when data is read.
         */
        public abstract void onReadCompleted(
            UrlRequest request,
            UrlResponseInfo info,
            ByteBuffer byteBuffer
        ) throws Exception;

        /**
         * Called when request completes successfully.
         */
        public abstract void onSucceeded(
            UrlRequest request,
            UrlResponseInfo info
        );

        /**
         * Called when request fails.
         */
        public abstract void onFailed(
            UrlRequest request,
            UrlResponseInfo info,
            CronetException error
        );

        /**
         * Called when the request is canceled.
         */
        public void onCanceled(UrlRequest request, UrlResponseInfo info) {}
    }
}
