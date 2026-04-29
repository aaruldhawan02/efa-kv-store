#include <arpa/inet.h>
#include <chrono>
#include <cstring>
#include <map>
#include <mutex>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <vector>

static constexpr int kPort           = 7777;
static constexpr int kPingIntervalMs = 2000; // ping every 2s
static constexpr int kPingTimeoutMs  = 1000; // 1s to respond
static constexpr int kMaxMissed      = 3;    // dead after 3 consecutive misses

struct ServerEntry {
    int         idx;
    std::string hex_addr;
    int         fd;
    bool        alive   = true;
    int         missed  = 0;
};

static std::mutex                 g_mu;
static std::map<int, ServerEntry> g_servers; // registration-idx → entry
static int                        g_next_idx = 0;
static std::vector<int>           g_watchers; // client fds watching for DEAD events

// Notify all watching clients that server idx died.
// Must be called with g_mu held.
static void notify_dead(int idx) {
    char msg[32];
    int  n    = snprintf(msg, sizeof(msg), "DEAD %d\n", idx);
    auto it   = g_watchers.begin();
    while (it != g_watchers.end()) {
        if (send(*it, msg, n, MSG_NOSIGNAL) <= 0) {
            close(*it);
            it = g_watchers.erase(it);
        } else {
            ++it;
        }
    }
}

// n/3 parity shards, rest data — matches user's requested breakdown.
static void compute_km(int n, int &k, int &m) {
    m = n / 3;
    k = n - m;
}

// SWIM-inspired failure detector: direct ping over the persistent TCP
// connection kept open after REGISTER. A full SWIM implementation would
// also do indirect pings via peer servers; we use direct-only here.
static void ping_server(int idx) {
    while (true) {
        std::this_thread::sleep_for(std::chrono::milliseconds(kPingIntervalMs));

        int  fd;
        bool already_dead;
        {
            std::lock_guard<std::mutex> lk(g_mu);
            auto it = g_servers.find(idx);
            if (it == g_servers.end()) return;
            already_dead = !it->second.alive;
            fd           = it->second.fd;
        }
        if (already_dead) return;

        // Direct ping
        if (send(fd, "PING\n", 5, MSG_NOSIGNAL) <= 0) {
            goto mark_dead;
        }

        {
            struct timeval tv;
            tv.tv_sec  = kPingTimeoutMs / 1000;
            tv.tv_usec = (kPingTimeoutMs % 1000) * 1000;
            setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
            char buf[16] = {};
            int  r       = recv(fd, buf, sizeof(buf) - 1, 0);
            if (r > 0 && strncmp(buf, "PONG", 4) == 0) {
                std::lock_guard<std::mutex> lk(g_mu);
                auto it = g_servers.find(idx);
                if (it != g_servers.end()) it->second.missed = 0;
                continue;
            }
        }

        {
            // Missed a ping — not dead yet unless kMaxMissed consecutive
            std::lock_guard<std::mutex> lk(g_mu);
            auto it = g_servers.find(idx);
            if (it == g_servers.end()) return;
            it->second.missed++;
            if (it->second.missed < kMaxMissed) continue;
        }

mark_dead:
        {
            std::lock_guard<std::mutex> lk(g_mu);
            auto it = g_servers.find(idx);
            if (it == g_servers.end()) return;
            it->second.alive = false;
            close(it->second.fd);
            int alive = 0;
            for (auto &[i, s] : g_servers) if (s.alive) alive++;
            int k, m;
            compute_km(alive, k, m);
            fprintf(stderr, "[coord] Server %d declared dead. alive=%d → k=%d m=%d\n",
                    idx, alive, k, m);
            notify_dead(idx);
        }
        return;
    }
}

int main(int argc, char **argv) {
    int port = (argc >= 2) ? atoi(argv[1]) : kPort;

    int srv = socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1;
    setsockopt(srv, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port        = htons(port);

    if (bind(srv, (sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind"); return 1;
    }
    listen(srv, 64);
    printf("Coordinator listening on port %d\n", port);
    fflush(stdout);

    for (;;) {
        int cli = accept(srv, nullptr, nullptr);
        if (cli < 0) continue;

        // Read one line
        char buf[512];
        int total = 0;
        while (total < (int)sizeof(buf) - 1) {
            int n = recv(cli, buf + total, 1, 0);
            if (n <= 0) break;
            total++;
            if (buf[total - 1] == '\n') break;
        }
        buf[total] = '\0';
        while (total > 0 && (buf[total-1] == '\n' || buf[total-1] == '\r'))
            buf[--total] = '\0';

        if (strncmp(buf, "REGISTER ", 9) == 0) {
            char hex[256];
            if (sscanf(buf + 9, "%255s", hex) != 1) {
                send(cli, "ERROR bad REGISTER\n", 19, 0);
                close(cli);
                continue;
            }
            int idx;
            {
                std::lock_guard<std::mutex> lk(g_mu);
                idx = g_next_idx++;
                g_servers[idx] = {idx, std::string(hex), cli, true, 0};
                int alive = 0;
                for (auto &[i, s] : g_servers) if (s.alive) alive++;
                int k, m;
                compute_km(alive, k, m);
                fprintf(stderr, "[coord] Server %d registered. alive=%d → k=%d m=%d\n",
                        idx, alive, k, m);
            }
            char resp[32];
            snprintf(resp, sizeof(resp), "OK %d\n", idx);
            send(cli, resp, strlen(resp), 0);
            // Don't close cli — keep alive for PING/PONG
            std::thread(ping_server, idx).detach();

        } else if (strcmp(buf, "LIST") == 0) {
            std::lock_guard<std::mutex> lk(g_mu);
            std::vector<std::pair<int,std::string>> alive; // (reg_idx, hex_addr)
            for (auto &[i, s] : g_servers)
                if (s.alive) alive.push_back({i, s.hex_addr});
            int n = (int)alive.size();
            if (n < 2) {
                char msg[64];
                snprintf(msg, sizeof(msg), "WAIT %d/2\n", n);
                send(cli, msg, strlen(msg), 0);
            } else {
                int k, m;
                compute_km(n, k, m);
                std::string resp;
                resp += "K " + std::to_string(k) + "\n";
                resp += "M " + std::to_string(m) + "\n";
                for (auto &[i, a] : alive)
                    resp += std::to_string(i) + " " + a + "\n";
                resp += "END\n";
                send(cli, resp.c_str(), resp.size(), 0);
            }
            close(cli);

        } else if (strcmp(buf, "WATCH") == 0) {
            // Client keeps this connection open to receive DEAD notifications.
            // Don't close cli — add to watchers list.
            std::lock_guard<std::mutex> lk(g_mu);
            g_watchers.push_back(cli);
            fprintf(stderr, "[coord] Client watching for failure events (fd=%d)\n", cli);

        } else if (strcmp(buf, "STATUS") == 0) {
            std::lock_guard<std::mutex> lk(g_mu);
            int alive = 0;
            for (auto &[i, s] : g_servers) if (s.alive) alive++;
            int k, m;
            compute_km(alive, k, m);
            std::string resp = "alive=" + std::to_string(alive) +
                               " k=" + std::to_string(k) +
                               " m=" + std::to_string(m) + "\n";
            for (auto &[i, s] : g_servers) {
                resp += "  shard " + std::to_string(i) +
                        (s.alive ? " [alive]" : " [dead]") +
                        ": " + s.hex_addr.substr(0, 16) + "...\n";
            }
            send(cli, resp.c_str(), resp.size(), 0);
            close(cli);

        } else {
            send(cli, "ERROR unknown command\n", 21, 0);
            close(cli);
        }
    }
}
