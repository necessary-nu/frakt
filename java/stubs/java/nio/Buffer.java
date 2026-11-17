package java.nio;

public abstract class Buffer {
    public final native int position();
    public final native Buffer position(int newPosition);
    public final native Buffer clear();
}
