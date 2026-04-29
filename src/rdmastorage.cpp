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

    // Connect via coordinator. k and m are assigned automatically.
    Client(const std::string &coord_host, int port = 7777) {
        auto info = coord_discover(coord_host.c_str(), port);
        net = std::make_unique<Network>(Network::Open());
        ec  = std::make_unique<ErasureClient>(*net, info);
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

    // Returns (encode_us, ctrl_rtt_us, rdma_us, commit_rtt_us) for last PUT.
    py::tuple last_put_phases() {
        auto &p = ec->last_put_phases;
        return py::make_tuple(p.encode_ns / 1e3, p.ctrl_rtt_ns / 1e3,
                              p.rdma_ns / 1e3,   p.commit_rtt_ns / 1e3);
    }

    // Returns (decode_us, ctrl_rtt_us, rdma_us) for last GET.
    py::tuple last_get_phases() {
        auto &p = ec->last_get_phases;
        return py::make_tuple(p.encode_ns / 1e3, p.ctrl_rtt_ns / 1e3,
                              p.rdma_ns / 1e3);
    }

    // Returns list of server positions (0-indexed) marked dead by coordinator.
    py::list dead_servers() {
        py::list result;
        for (int i = 0; i < ec->k + ec->m; i++)
            if (ec->dead[i].load())
                result.append(i);
        return result;
    }
};

PYBIND11_MODULE(rdmastorage, m) {
    m.doc() = "RDMA-backed erasure-coded key-value store";

    py::class_<Client>(m, "Client")
        .def(py::init<const std::string &, int>(),
             py::arg("coord"),
             py::arg("port") = 7777,
             R"(
Connect to the storage cluster via the coordinator.
k and m are assigned automatically based on the number of live servers.

Args:
    coord: hostname or IP of the coordinator (e.g. "node0")
    port:  coordinator port (default 7777)
)")
        .def_property_readonly("k", [](const Client &c){ return c.ec->k; })
        .def_property_readonly("m", [](const Client &c){ return c.ec->m; })
        .def("put", &Client::put,
             py::arg("key"), py::arg("data"),
             "Store bytes under key.")
        .def("get", &Client::get,
             py::arg("key"),
             "Retrieve bytes stored under key.")
        .def("delete", &Client::delete_,
             py::arg("key"),
             "Delete key from all shards.")
        .def("last_put_phases", &Client::last_put_phases,
             "Returns (encode_us, ctrl_rtt_us, rdma_write_us, commit_rtt_us) for last PUT.")
        .def("last_get_phases", &Client::last_get_phases,
             "Returns (decode_us, ctrl_rtt_us, rdma_read_us) for last GET.")
        .def("dead_servers", &Client::dead_servers,
             "Returns list of server positions marked dead by the coordinator.");
}
