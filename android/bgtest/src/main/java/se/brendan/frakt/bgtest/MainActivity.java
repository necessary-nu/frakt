package se.brendan.frakt.bgtest;

import android.app.Activity;
import android.content.Intent;
import android.os.Bundle;
import android.util.Log;

/**
 * Runs one harness action from the launching intent and finishes:
 * {@code am start -n se.brendan.frakt.bgtest/.MainActivity --es action
 * enqueue --es job ID --es url URL}, or {@code --es action collect},
 * {@code jobs} or {@code cancel}. Results go to logcat under FraktHarness;
 * events go to files/events.jsonl.
 */
public final class MainActivity extends Activity {
    private static final String IDENTIFIER = "bgtest";

    @Override
    protected void onCreate(Bundle state) {
        super.onCreate(state);
        Intent intent = getIntent();
        String action = intent.getStringExtra("action");
        log(Harness.start(this, IDENTIFIER, getFilesDir().getAbsolutePath()));
        if ("enqueue".equals(action)) {
            log(Harness.enqueue(intent.getStringExtra("job"), intent.getStringExtra("url")));
        } else if ("cancel".equals(action)) {
            log(Harness.cancel(intent.getStringExtra("job")));
        } else if ("jobs".equals(action)) {
            log(Harness.jobs());
        }
        finish();
    }

    private static void log(String message) {
        Log.i("FraktHarness", message);
    }
}
