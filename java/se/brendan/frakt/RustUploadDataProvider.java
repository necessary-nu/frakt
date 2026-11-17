package se.brendan.frakt;

import org.chromium.net.UploadDataProvider;
import org.chromium.net.UploadDataSink;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * UploadDataProvider whose body stays in frakt's Rust code: each read has the
 * request's handler copy the next bytes, from memory or from its open file,
 * straight into Cronet's buffer. The native is registered by frakt when it
 * loads this class from its embedded DEX.
 *
 * The body is read by offset and only the offset is kept here, so rewinding
 * is resetting it. Cronet calls read() and rewind() one at a time.
 */
public class RustUploadDataProvider extends UploadDataProvider {
    private final long handlerId;
    private final long length;
    private long offset = 0;

    public RustUploadDataProvider(long handlerId, long length) {
        this.handlerId = handlerId;
        this.length = length;
    }

    @Override
    public long getLength() {
        return length;
    }

    @Override
    public void read(UploadDataSink uploadDataSink, ByteBuffer byteBuffer) {
        int position = byteBuffer.position();
        int read = nativeRead(handlerId, offset, byteBuffer, position, byteBuffer.remaining());
        if (read < 0) {
            // Rust keeps the cause and reports it when Cronet fails the request.
            uploadDataSink.onReadError(new IOException("frakt could not read the request body"));
            return;
        }
        byteBuffer.position(position + read);
        offset += read;
        // The length is known up front, so no read is a final chunk.
        uploadDataSink.onReadSucceeded(false);
    }

    @Override
    public void rewind(UploadDataSink uploadDataSink) {
        offset = 0;
        uploadDataSink.onRewindSucceeded();
    }

    /**
     * Copies up to {@code remaining} bytes of the body at {@code offset} into
     * {@code buffer}, which must be direct, from {@code position}. Returns the
     * number copied, or -1 if the body could not be read.
     */
    private static native int nativeRead(
        long handlerId, long offset, ByteBuffer buffer, int position, int remaining);
}
