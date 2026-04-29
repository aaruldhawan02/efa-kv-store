#include "client_lib.hpp"
#include <pybind11/pybind11.h>
#include <memory>
#include <stdexcept>

namespace py = pybind11;

// Wrapper that owns the Network + ErasureClient so both stay alive for the
// lifetime of the Python object.  Connections are established once in the
// constructor; subsequent put/get calls reuse them with no reconnect overhead.
struct Client {
    std::unique_ptr<Network>      net;
    std::unique_ptr<ErasureClient> ec;

    // Connect via coordinator.
    Client(const std::string &coord_host, int k, int m, int port = 7777) {
        auto addrs = coord_discover(coord_host.c_str(), port, k, m);
        net = std::make_unique<Network>(Network::Open());
        ec  = std::make_unique<ErasureClient>(*net, k, m, addrs);
    }

    void put(const std::string &key, py::bytes data) {
        py::buffer_info info(py::buffer(data).request());
        // pybind11 bytes → raw pointer
        std::string s = data;
        if (s.empty()) throw std::invalid_argument("data is empty");
        if (s.size() > (size_t)ec->k * kMaxShardSize)
            throw std::invalid_argument("data too large");
        bool ok = ec->Put(key,
                          reinterpret_cast<const uint8_t *>(s.data()),
                          s.size());
        if (!ok) throw std::runtime_error("PUT failed");
    }

    py::bytes get(const std::string &key) {
        auto data = ec->Get(key);
        if (data.empty()) throw std::runtime_error("key not found");
        return py::bytes(reinterpret_cast<const char *>(data.data()),
                         data.size());
    }

    void delete_(const std::string &key) {
        ec->Delete(key);
    }
};

PYBIND11_MODULE(rdmastorage, m) {
    m.doc() = "RDMA-backed erasure-coded key-value store";

    py::class_<Client>(m, "Client")
        .def(py::init<const std::string &, int, int, int>(),
             py::arg("coord"),
             py::arg("k"),
             py::arg("m") = 1,
             py::arg("port") = 7777,
             R"(
Connect to the storage cluster via the coordinator.

Args:
    coord: hostname or IP of the coordinator (e.g. "node0")
    k:     number of data shards
    m:     number of parity shards (default 1)
    port:  coordinator port (default 7777)
)")
        .def("put", &Client::put,
             py::arg("key"), py::arg("data"),
             "Store bytes under key.")
        .def("get", &Client::get,
             py::arg("key"),
             "Retrieve bytes stored under key.")
        .def("delete", &Client::delete_,
             py::arg("key"),
             "Delete key from all shards.");
}
