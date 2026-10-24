package se.brendan.frakt.background;

import android.app.Notification;
import android.app.NotificationChannel;
import android.app.NotificationManager;
import android.content.Context;
import android.content.pm.ServiceInfo;
import android.net.Uri;
import android.os.Build;
import android.os.SystemClock;

import androidx.annotation.NonNull;
import androidx.work.Data;
import androidx.work.ForegroundInfo;
import androidx.work.Worker;
import androidx.work.WorkerParameters;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Downloads one job into its {@link JobFiles}, resuming a partial file with
 * Range and If-Range. It needs nothing but this class and WorkManager, so it
 * runs the same in a process the system started only for it.
 */
public final class FraktDownloadWorker extends Worker {
    private static final int MAX_ATTEMPTS = 5;
    private static final long PROGRESS_INTERVAL_MS = 500;
    private static final String CHANNEL = "frakt-background";

    /** A failure that retrying cannot fix, such as a 404. */
    private static final class PermanentFailure extends Exception {
        final int httpStatus;

        PermanentFailure(int httpStatus, String message) {
            super(message);
            this.httpStatus = httpStatus;
        }
    }

    public FraktDownloadWorker(@NonNull Context context, @NonNull WorkerParameters parameters) {
        super(context, parameters);
    }

    @NonNull
    @Override
    public Result doWork() {
        Data input = getInputData();
        String identifier = input.getString(FraktBackground.KEY_IDENTIFIER);
        String job = input.getString(FraktBackground.KEY_JOB);
        String url = input.getString(FraktBackground.KEY_URL);
        if (identifier == null || job == null || url == null) {
            return Result.failure();
        }
        JobFiles files =
            new JobFiles(FraktBackground.jobDirectory(getApplicationContext(), identifier), job);
        JSONObject status = files.readStatus();
        if (status == null) {
            // The job was cancelled, which deletes its status.
            return Result.failure();
        }

        try {
            setForegroundAsync(foregroundInfo(job, url)).get();
        } catch (Exception e) {
            // From Android 12, starting a foreground service from the
            // background can be refused; the download still runs, as
            // ordinary work.
        }

        try {
            download(files, status, url, input.getString(FraktBackground.KEY_HEADERS));
            return Result.success();
        } catch (PermanentFailure e) {
            files.part.delete();
            files.resume.delete();
            fail(files, status, e.getMessage(), e.httpStatus, false);
            return Result.failure();
        } catch (IOException | JSONException e) {
            if (isStopped()) {
                return Result.retry();
            }
            boolean resumable = files.part.exists() && files.resume.exists();
            if (getRunAttemptCount() + 1 < MAX_ATTEMPTS) {
                try {
                    status.put("state", "waiting");
                    status.put("error", String.valueOf(e));
                    files.writeStatus(status);
                } catch (IOException | JSONException ignored) {
                    // The next attempt rewrites the status.
                }
                return Result.retry();
            }
            fail(files, status, String.valueOf(e), 0, resumable);
            return Result.failure();
        }
    }

    private void fail(JobFiles files, JSONObject status, String error, int httpStatus, boolean resumable) {
        try {
            status.put("state", "failed");
            status.put("error", error);
            status.put("httpStatus", httpStatus);
            status.put("resumable", resumable);
            files.writeStatus(status);
        } catch (IOException | JSONException ignored) {
            // Nothing else can record it.
        }
    }

    private void download(JobFiles files, JSONObject status, String url, String headers)
        throws IOException, JSONException, PermanentFailure {
        JSONObject resume = files.readResume();
        long have = files.part.exists() ? files.part.length() : 0;
        String validator = null;
        if (resume != null && url.equals(resume.optString("url"))) {
            validator = resume.optString("validator", null);
        }
        if (validator == null || have == 0) {
            have = 0;
            files.part.delete();
        }

        HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
        try {
            connection.setConnectTimeout(30_000);
            connection.setReadTimeout(60_000);
            connection.setInstanceFollowRedirects(true);
            if (headers != null) {
                JSONArray pairs = new JSONArray(headers);
                for (int i = 0; i < pairs.length(); i++) {
                    JSONArray pair = pairs.getJSONArray(i);
                    connection.addRequestProperty(pair.getString(0), pair.getString(1));
                }
            }
            // The file must be the stored bytes, so a resumed range lines up
            // and the host can hash what the server published.
            connection.setRequestProperty("Accept-Encoding", "identity");
            if (have > 0) {
                connection.setRequestProperty("Range", "bytes=" + have + "-");
                connection.setRequestProperty("If-Range", validator);
            }

            int code = connection.getResponseCode();
            boolean append;
            long total;
            if (code == HttpURLConnection.HTTP_PARTIAL && have > 0) {
                long[] range = contentRange(connection.getHeaderField("Content-Range"));
                if (range == null || range[0] != have) {
                    files.part.delete();
                    files.resume.delete();
                    throw new IOException("server resumed at the wrong offset: "
                        + connection.getHeaderField("Content-Range"));
                }
                append = true;
                total = range[2];
            } else if (code == HttpURLConnection.HTTP_OK) {
                append = false;
                have = 0;
                total = connection.getContentLengthLong();
            } else if (code == 416 && have > 0) {
                long[] range = contentRange(connection.getHeaderField("Content-Range"));
                if (range != null && range[2] == have) {
                    finish(files, status, connection, code, have);
                    return;
                }
                files.part.delete();
                files.resume.delete();
                throw new IOException("partial file no longer matches: HTTP 416");
            } else if (code == 408 || code == 429 || code >= 500) {
                throw new IOException("HTTP " + code);
            } else {
                throw new PermanentFailure(code, "HTTP " + code);
            }

            JSONObject validators = new JSONObject();
            validators.put("url", url);
            String newValidator = validator(connection);
            if (newValidator != null) {
                validators.put("validator", newValidator);
                files.writeResume(validators);
            } else {
                files.resume.delete();
            }

            long downloaded = have;
            status.put("state", "running");
            status.put("downloaded", downloaded);
            status.put("total", total);
            files.writeStatus(status);
            long lastReport = SystemClock.elapsedRealtime();

            byte[] buffer = new byte[64 * 1024];
            try (InputStream in = connection.getInputStream();
                 OutputStream out = new FileOutputStream(files.part, append)) {
                int n;
                while ((n = in.read(buffer)) >= 0) {
                    out.write(buffer, 0, n);
                    downloaded += n;
                    long now = SystemClock.elapsedRealtime();
                    if (now - lastReport >= PROGRESS_INTERVAL_MS) {
                        lastReport = now;
                        if (isStopped()) {
                            throw new IOException("stopped");
                        }
                        status.put("downloaded", downloaded);
                        files.writeStatus(status);
                        setProgressAsync(new Data.Builder()
                            .putLong("downloaded", downloaded)
                            .putLong("total", total)
                            .build());
                    }
                }
            }
            if (total >= 0 && downloaded != total) {
                throw new IOException("connection closed after " + downloaded + " of " + total + " bytes");
            }
            finish(files, status, connection, code, downloaded);
        } finally {
            connection.disconnect();
        }
    }

    private void finish(JobFiles files, JSONObject status, HttpURLConnection connection, int code, long size)
        throws IOException, JSONException {
        if (isStopped()) {
            throw new IOException("stopped");
        }
        files.done.delete();
        if (!files.part.renameTo(files.done)) {
            throw new IOException("could not move the finished download into place");
        }
        files.resume.delete();
        JSONArray headers = new JSONArray();
        for (Map.Entry<String, List<String>> header : connection.getHeaderFields().entrySet()) {
            // OkHttp adds X-Android-* entries of its own; they are not the
            // server's.
            if (header.getKey() == null || header.getKey().startsWith("X-Android-")) {
                continue;
            }
            for (String value : header.getValue()) {
                headers.put(new JSONArray().put(header.getKey()).put(value));
            }
        }
        status.put("state", "succeeded");
        status.put("downloaded", size);
        status.put("total", size);
        status.put("file", files.done.getAbsolutePath());
        status.put("status", code == 416 ? 206 : code);
        status.put("headers", headers);
        status.remove("error");
        files.writeStatus(status);
    }

    /** A strong ETag, else Last-Modified: If-Range takes only these. */
    private static String validator(HttpURLConnection connection) {
        String etag = connection.getHeaderField("ETag");
        if (etag != null && !etag.startsWith("W/")) {
            return etag;
        }
        return connection.getHeaderField("Last-Modified");
    }

    /** {first, last, total} from a Content-Range, with total -1 if "*". */
    private static long[] contentRange(String value) {
        if (value == null || !value.startsWith("bytes ")) {
            return null;
        }
        try {
            String spec = value.substring(6).trim();
            int slash = spec.indexOf('/');
            String total = spec.substring(slash + 1);
            long size = total.equals("*") ? -1 : Long.parseLong(total);
            String range = spec.substring(0, slash);
            if (range.equals("*")) {
                return new long[] {-1, -1, size};
            }
            int dash = range.indexOf('-');
            return new long[] {
                Long.parseLong(range.substring(0, dash)),
                Long.parseLong(range.substring(dash + 1)),
                size,
            };
        } catch (RuntimeException e) {
            return null;
        }
    }

    @SuppressWarnings("deprecation")
    private ForegroundInfo foregroundInfo(String job, String url) {
        Context context = getApplicationContext();
        int id = ("frakt-background:" + job).hashCode();
        Notification.Builder builder;
        if (Build.VERSION.SDK_INT >= 26) {
            NotificationManager manager = context.getSystemService(NotificationManager.class);
            if (manager.getNotificationChannel(CHANNEL) == null) {
                manager.createNotificationChannel(new NotificationChannel(
                    CHANNEL, "Downloads", NotificationManager.IMPORTANCE_LOW));
            }
            builder = new Notification.Builder(context, CHANNEL);
        } else {
            builder = new Notification.Builder(context);
        }
        Notification notification = builder
            .setSmallIcon(android.R.drawable.stat_sys_download)
            .setContentTitle("Downloading")
            .setContentText(Uri.parse(url).getHost())
            .setOngoing(true)
            .build();
        if (Build.VERSION.SDK_INT >= 29) {
            return new ForegroundInfo(id, notification, ServiceInfo.FOREGROUND_SERVICE_TYPE_DATA_SYNC);
        }
        return new ForegroundInfo(id, notification);
    }
}
