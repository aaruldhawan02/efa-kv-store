package site.ycsb.db.rdmastorage;

import site.ycsb.ByteArrayByteIterator;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

/**
 * YCSB binding for RDMAStorage. Each YCSB worker thread constructs its own
 * instance; init() opens a libfabric connection via the C++ ErasureClient,
 * cleanup() tears it down. read/insert/update/delete map to the obvious
 * native calls. scan() is unsupported and returns NOT_IMPLEMENTED.
 *
 * Properties:
 *   rdmastorage.coord  - coordinator host (default "node0")
 *   rdmastorage.port   - coordinator port (default 7777)
 */
public class RdmaStorageClient extends DB {

  static {
    System.loadLibrary("rdmastorage_jni");
  }

  private static final String SINGLE_FIELD = "field0";

  private long handle;

  @Override
  public void init() throws DBException {
    String coord = getProperties().getProperty("rdmastorage.coord", "node0");
    int port = Integer.parseInt(getProperties().getProperty("rdmastorage.port", "7777"));
    try {
      handle = ncInit(coord, port);
    } catch (RuntimeException e) {
      throw new DBException(e);
    }
    if (handle == 0L) {
      throw new DBException("ncInit returned null handle");
    }
  }

  @Override
  public void cleanup() throws DBException {
    if (handle != 0L) {
      ncClose(handle);
      handle = 0L;
    }
  }

  @Override
  public Status read(String table, String key, Set<String> fields,
                     Map<String, ByteIterator> result) {
    byte[] val = ncGet(handle, key.getBytes());
    if (val == null) {
      return Status.NOT_FOUND;
    }
    // RDMAStorage stores the value as a single opaque blob — we don't preserve
    // YCSB's per-field structure. Return everything under one synthetic field.
    result.put(SINGLE_FIELD, new ByteArrayByteIterator(val));
    return Status.OK;
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    return ncPut(handle, key.getBytes(), flatten(values)) == 0
        ? Status.OK : Status.ERROR;
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    // No partial updates — every write is a full-object overwrite.
    return insert(table, key, values);
  }

  @Override
  public Status delete(String table, String key) {
    return ncDelete(handle, key.getBytes()) == 0
        ? Status.OK : Status.ERROR;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount,
                     Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {
    return Status.NOT_IMPLEMENTED;
  }

  private static byte[] flatten(Map<String, ByteIterator> values) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try {
      for (Map.Entry<String, ByteIterator> e : values.entrySet()) {
        out.write(e.getValue().toArray());
      }
    } catch (IOException ignored) {
      // ByteArrayOutputStream.write does not throw in practice.
    }
    return out.toByteArray();
  }

  // ── Native methods ──────────────────────────────────────────────────────
  private static native long   ncInit(String coord, int port);
  private static native void   ncClose(long handle);
  private static native int    ncPut(long handle, byte[] key, byte[] value);
  private static native byte[] ncGet(long handle, byte[] key);
  private static native int    ncDelete(long handle, byte[] key);
  private static native int    ncGetK(long handle);
  private static native int    ncGetM(long handle);
}
