package se.brendan.frakt.background;

import android.content.Context;

import androidx.work.BackoffPolicy;
import androidx.work.Constraints;
import androidx.work.Data;
import androidx.work.ExistingWorkPolicy;
import androidx.work.NetworkType;
import androidx.work.OneTimeWorkRequest;
import androidx.work.WorkManager;

import java.io.File;
import java.util.concurrent.TimeUnit;

import org.json.JSONObject;

/**
 * What frakt calls through JNI. Job state lives in {@link JobFiles}, which
 * the worker writes and frakt reads, not in this process.
 */
public final class FraktBackground {
    static final String KEY_IDENTIFIER = "identifier";
    static final String KEY_JOB = "job";
    static final String KEY_URL = "url";
    static final String KEY_HEADERS = "headers";
    static final String KEY_DESTINATION = "destination";

    private FraktBackground() {}

    /** The directory holding the job files for {@code identifier}. */
    public static String directory(Context context, String identifier) {
        return jobDirectory(context, identifier).getAbsolutePath();
    }

    static File jobDirectory(Context context, String identifier) {
        File directory = new File(
            new File(context.getFilesDir(), "frakt-background"), JobFiles.hex(identifier));
        directory.mkdirs();
        return directory;
    }

    static String workName(String identifier, String job) {
        return "frakt-background:" + identifier + ":" + job;
    }

    /**
     * Enqueue {@code job}. {@code headers} is a JSON array of [name, value]
     * pairs. Returns null, or why the job could not be enqueued.
     */
    public static String enqueue(
        Context context,
        String identifier,
        String job,
        String url,
        String headers,
        String destination
    ) {
        try {
            JobFiles files = new JobFiles(jobDirectory(context, identifier), job);
            JSONObject status = new JSONObject();
            status.put("id", job);
            status.put("url", url);
            if (destination != null) {
                status.put("destination", destination);
            }
            status.put("state", "waiting");
            status.put("downloaded", files.part.exists() ? files.part.length() : 0);
            status.put("total", -1);
            files.writeStatus(status);

            Data.Builder input = new Data.Builder()
                .putString(KEY_IDENTIFIER, identifier)
                .putString(KEY_JOB, job)
                .putString(KEY_URL, url)
                .putString(KEY_HEADERS, headers);
            if (destination != null) {
                input.putString(KEY_DESTINATION, destination);
            }
            OneTimeWorkRequest request = new OneTimeWorkRequest.Builder(FraktDownloadWorker.class)
                .setInputData(input.build())
                .setConstraints(new Constraints.Builder()
                    .setRequiredNetworkType(NetworkType.CONNECTED)
                    .build())
                .setBackoffCriteria(BackoffPolicy.EXPONENTIAL, 10, TimeUnit.SECONDS)
                .addTag("frakt-background:" + identifier)
                .build();
            // KEEP leaves a pending job alone; a finished or failed one is
            // replaced, which is how a failed job is resumed.
            WorkManager.getInstance(context)
                .enqueueUniqueWork(workName(identifier, job), ExistingWorkPolicy.KEEP, request)
                .getResult()
                .get();
            return null;
        } catch (Exception e) {
            return e.toString();
        }
    }

    /** Cancel {@code job} and delete its files. Returns null or an error. */
    public static String cancel(Context context, String identifier, String job) {
        try {
            WorkManager.getInstance(context)
                .cancelUniqueWork(workName(identifier, job))
                .getResult()
                .get();
            new JobFiles(jobDirectory(context, identifier), job).deleteAll();
            return null;
        } catch (Exception e) {
            return e.toString();
        }
    }
}
