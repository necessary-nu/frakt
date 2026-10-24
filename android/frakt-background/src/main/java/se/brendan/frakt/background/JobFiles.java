package se.brendan.frakt.background;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

import org.json.JSONException;
import org.json.JSONObject;

/**
 * The files that hold one job's state, so that it survives the process.
 *
 * <ul>
 *   <li>{@code <key>.json}: the status frakt reads (state, progress, result).
 *   <li>{@code <key>.part}: the bytes downloaded so far.
 *   <li>{@code <key>.resume}: the URL and validators the partial file was
 *       downloaded with, for If-Range.
 *   <li>{@code <key>.download}: the finished file, until frakt has handed it
 *       to the host.
 * </ul>
 *
 * The key is the job id in hex, so any id is a safe file name.
 */
final class JobFiles {
    final File status;
    final File part;
    final File resume;
    final File done;

    JobFiles(File directory, String job) {
        String key = hex(job);
        status = new File(directory, key + ".json");
        part = new File(directory, key + ".part");
        resume = new File(directory, key + ".resume");
        done = new File(directory, key + ".download");
    }

    static String hex(String text) {
        StringBuilder out = new StringBuilder();
        for (byte b : text.getBytes(StandardCharsets.UTF_8)) {
            out.append(String.format("%02x", b & 0xff));
        }
        return out.toString();
    }

    JSONObject readStatus() {
        return read(status);
    }

    JSONObject readResume() {
        return read(resume);
    }

    void writeStatus(JSONObject value) throws IOException {
        write(status, value);
    }

    void writeResume(JSONObject value) throws IOException {
        write(resume, value);
    }

    void deleteAll() {
        status.delete();
        part.delete();
        resume.delete();
        done.delete();
    }

    private static JSONObject read(File file) {
        try (InputStream in = new FileInputStream(file)) {
            byte[] bytes = new byte[(int) file.length()];
            int read = 0;
            while (read < bytes.length) {
                int n = in.read(bytes, read, bytes.length - read);
                if (n < 0) {
                    break;
                }
                read += n;
            }
            return new JSONObject(new String(bytes, 0, read, StandardCharsets.UTF_8));
        } catch (IOException | JSONException e) {
            return null;
        }
    }

    /** Replace the file atomically, so a reader never sees half of it. */
    private static void write(File file, JSONObject value) throws IOException {
        File temporary = new File(file.getPath() + ".tmp");
        try (OutputStream out = new FileOutputStream(temporary)) {
            out.write(value.toString().getBytes(StandardCharsets.UTF_8));
            out.flush();
        }
        if (!temporary.renameTo(file)) {
            temporary.delete();
            throw new IOException("could not replace " + file);
        }
    }
}
