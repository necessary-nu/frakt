package se.brendan.frakt.bgtest;

import android.content.Context;

/** The JNI entry points of examples/android_bg_harness.rs. */
final class Harness {
    static {
        System.loadLibrary("android_bg_harness");
    }

    private Harness() {}

    static native String start(Context context, String identifier, String filesDir);

    static native String enqueue(String job, String url);

    static native String cancel(String job);

    static native String jobs();
}
