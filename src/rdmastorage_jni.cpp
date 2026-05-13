// JNI shim that exposes the C++ ErasureClient to YCSB's Java DB binding.
// One opaque jlong handle per client; methods are static so multi-threaded
// YCSB can keep one instance per thread without contention.

#include "client_lib.hpp"
#include <jni.h>
#include <exception>
#include <memory>
#include <string>
#include <sys/socket.h>
#include <unistd.h>
#include <vector>

namespace {

struct RdmaClient {
    std::unique_ptr<Network>       net;
    std::unique_ptr<ErasureClient> ec;
};

void throw_runtime(JNIEnv *env, const char *msg) {
    jclass cls = env->FindClass("java/lang/RuntimeException");
    if (cls) env->ThrowNew(cls, msg);
}

} // namespace

extern "C" {

JNIEXPORT jlong JNICALL
Java_site_ycsb_db_rdmastorage_RdmaStorageClient_ncInit(
        JNIEnv *env, jclass, jstring jcoord, jint port) {
    const char *coord = env->GetStringUTFChars(jcoord, nullptr);
    RdmaClient *c = nullptr;
    try {
        auto info = coord_discover(coord, port);
        c = new RdmaClient();
        c->net = std::make_unique<Network>(Network::Open());
        c->ec  = std::make_unique<ErasureClient>(*c->net, info);
    } catch (const std::exception &e) {
        delete c;
        env->ReleaseStringUTFChars(jcoord, coord);
        throw_runtime(env, e.what());
        return 0;
    }
    env->ReleaseStringUTFChars(jcoord, coord);
    return reinterpret_cast<jlong>(c);
}

JNIEXPORT void JNICALL
Java_site_ycsb_db_rdmastorage_RdmaStorageClient_ncClose(
        JNIEnv *, jclass, jlong handle) {
    // Stop the watcher thread cleanly. We can't let it outlive this call —
    // if we leak the whole RdmaClient, the JVM eventually unloads our .so at
    // exit and the watcher (still in recv) segfaults returning to a
    // now-unmapped code page.
    auto *c = reinterpret_cast<RdmaClient *>(handle);
    if (c && c->ec) {
        if (c->ec->coord_fd >= 0) {
            shutdown(c->ec->coord_fd, SHUT_RDWR);
            close(c->ec->coord_fd);
            c->ec->coord_fd = -1;
        }
        if (c->ec->watcher_thread.joinable()) {
            c->ec->watcher_thread.join();
        }
    }
    // FIXME(2026-05-13): full ~ErasureClient teardown (libfabric MR/EP/domain
    // closes) triggers a separate SIGSEGV. Skip the delete for now — OS
    // reclaims fds, MRs, and memory on process exit. Remove once destructor
    // bug is debugged.
    (void)handle;
}

JNIEXPORT jint JNICALL
Java_site_ycsb_db_rdmastorage_RdmaStorageClient_ncPut(
        JNIEnv *env, jclass, jlong handle, jbyteArray jkey, jbyteArray jval) {
    auto *c = reinterpret_cast<RdmaClient *>(handle);
    jsize klen = env->GetArrayLength(jkey);
    jsize vlen = env->GetArrayLength(jval);
    jbyte *kbuf = env->GetByteArrayElements(jkey, nullptr);
    jbyte *vbuf = env->GetByteArrayElements(jval, nullptr);
    std::string key(reinterpret_cast<const char *>(kbuf), klen);
    bool ok = false;
    try {
        ok = c->ec->Put(key,
                        reinterpret_cast<const uint8_t *>(vbuf),
                        static_cast<size_t>(vlen));
    } catch (...) {
        ok = false;
    }
    env->ReleaseByteArrayElements(jkey, kbuf, JNI_ABORT);
    env->ReleaseByteArrayElements(jval, vbuf, JNI_ABORT);
    return ok ? 0 : -1;
}

JNIEXPORT jbyteArray JNICALL
Java_site_ycsb_db_rdmastorage_RdmaStorageClient_ncGet(
        JNIEnv *env, jclass, jlong handle, jbyteArray jkey) {
    auto *c = reinterpret_cast<RdmaClient *>(handle);
    jsize klen = env->GetArrayLength(jkey);
    jbyte *kbuf = env->GetByteArrayElements(jkey, nullptr);
    std::string key(reinterpret_cast<const char *>(kbuf), klen);
    env->ReleaseByteArrayElements(jkey, kbuf, JNI_ABORT);

    std::vector<uint8_t> data;
    try {
        data = c->ec->Get(key);
    } catch (...) {
        return nullptr;
    }
    if (data.empty()) return nullptr;
    jbyteArray result = env->NewByteArray(static_cast<jsize>(data.size()));
    if (!result) return nullptr;
    env->SetByteArrayRegion(result, 0,
                            static_cast<jsize>(data.size()),
                            reinterpret_cast<const jbyte *>(data.data()));
    return result;
}

JNIEXPORT jint JNICALL
Java_site_ycsb_db_rdmastorage_RdmaStorageClient_ncDelete(
        JNIEnv *env, jclass, jlong handle, jbyteArray jkey) {
    auto *c = reinterpret_cast<RdmaClient *>(handle);
    jsize klen = env->GetArrayLength(jkey);
    jbyte *kbuf = env->GetByteArrayElements(jkey, nullptr);
    std::string key(reinterpret_cast<const char *>(kbuf), klen);
    env->ReleaseByteArrayElements(jkey, kbuf, JNI_ABORT);
    try {
        c->ec->Delete(key);
        return 0;
    } catch (...) {
        return -1;
    }
}

JNIEXPORT jint JNICALL
Java_site_ycsb_db_rdmastorage_RdmaStorageClient_ncGetK(
        JNIEnv *, jclass, jlong handle) {
    return reinterpret_cast<RdmaClient *>(handle)->ec->k;
}

JNIEXPORT jint JNICALL
Java_site_ycsb_db_rdmastorage_RdmaStorageClient_ncGetM(
        JNIEnv *, jclass, jlong handle) {
    return reinterpret_cast<RdmaClient *>(handle)->ec->m;
}

} // extern "C"
