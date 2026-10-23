package java.nio;

public abstract class ByteBuffer extends Buffer {
    public abstract int remaining();
    public abstract ByteBuffer put(byte[] src, int offset, int length);
}
