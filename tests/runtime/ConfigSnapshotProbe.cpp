#include <array>
#include <iostream>

#include "runtime/RuntimeConfig.h"

int main()
{
    std::array<char, arena::runtime::ConfigSnapshot::maxBytes + 1>
        data;
    std::cin.read(data.data(), data.size());
    const auto parsed = arena::runtime::ConfigSnapshot::parse(
        std::string_view(
            data.data(),
            static_cast<std::size_t>(std::cin.gcount())
        )
    );
    if (!parsed)
    {
        std::cout << "{\"accepted\":false}\n";
        return 0;
    }
    const auto& value = **parsed;
    std::cout << std::boolalpha << "{\"accepted\":true,\"version\":\""
              << value.version()
              << "\",\"desired_ready\":" << value.desiredReady()
              << ",\"drain_grace_ms\":" << value.drainGraceMs()
              << ",\"nodes\":[";
    bool first = true;
    for (const auto& node : value.nodes())
    {
        if (!first)
            std::cout << ',';
        first = false;
        std::cout << "{\"service\":\"" << node.service
                  << "\",\"id\":\"" << node.id << "\",\"host\":\""
                  << node.host << "\",\"port\":" << node.port
                  << ",\"ready\":" << node.ready << '}';
    }
    std::cout << "]}\n";
}