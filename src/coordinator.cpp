#include <arpa/inet.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>
#include <map>
#include <string>

static constexpr int kPort = 7777;

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
    listen(srv, 32);
    printf("Coordinator listening on port %d\n", port);
    fflush(stdout);

    // shard_idx → hex fabric address (auto-assigned in registration order)
    std::map<int, std::string> registry;
    int next_idx = 0;

    for (;;) {
        int cli = accept(srv, nullptr, nullptr);
        if (cli < 0) continue;

        char buf[512];
        int n = 0, total = 0;
        // Read until newline
        while (total < (int)sizeof(buf) - 1) {
            n = recv(cli, buf + total, 1, 0);
            if (n <= 0) break;
            total++;
            if (buf[total - 1] == '\n') break;
        }
        buf[total] = '\0';
        // Strip trailing whitespace
        while (total > 0 && (buf[total-1] == '\n' || buf[total-1] == '\r'))
            buf[--total] = '\0';

        if (strncmp(buf, "REGISTER ", 9) == 0) {
            char hex[256];
            if (sscanf(buf + 9, "%255s", hex) == 1) {
                int idx = next_idx++;
                registry[idx] = hex;
                fprintf(stderr, "shard %d registered: %.16s...\n", idx, hex);
                char resp[32];
                snprintf(resp, sizeof(resp), "OK %d\n", idx);
                send(cli, resp, strlen(resp), 0);
            } else {
                send(cli, "ERROR bad REGISTER\n", 19, 0);
            }

        } else if (strncmp(buf, "LIST ", 5) == 0) {
            int k, m;
            if (sscanf(buf + 5, "%d %d", &k, &m) != 2) {
                send(cli, "ERROR bad LIST\n", 15, 0);
            } else {
                int need = k + m;
                // Check all indices 0..need-1 are registered
                bool ready = true;
                for (int i = 0; i < need; i++) {
                    if (registry.find(i) == registry.end()) { ready = false; break; }
                }
                if (!ready) {
                    char msg[64];
                    snprintf(msg, sizeof(msg), "WAIT %zu/%d\n",
                             registry.size(), need);
                    send(cli, msg, strlen(msg), 0);
                } else {
                    std::string resp;
                    for (int i = 0; i < need; i++)
                        resp += registry[i] + "\n";
                    resp += "END\n";
                    send(cli, resp.c_str(), resp.size(), 0);
                }
            }

        } else if (strcmp(buf, "STATUS") == 0) {
            std::string resp = "Registered shards: " +
                               std::to_string(registry.size()) + "\n";
            for (auto &[idx, hex] : registry)
                resp += "  shard " + std::to_string(idx) + ": " +
                        hex.substr(0, 16) + "...\n";
            send(cli, resp.c_str(), resp.size(), 0);

        } else {
            send(cli, "ERROR unknown command\n", 21, 0);
        }

        close(cli);
    }
}
