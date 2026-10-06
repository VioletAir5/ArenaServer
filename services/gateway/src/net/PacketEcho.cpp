#include "net/PacketEcho.h"

#include <exception>
#include <iostream>
#include <memory>
#include <stdexcept>

#include "protocol/PacketCodec.hpp"

namespace service::gateway
{
ConnectionMessageCallback
makePacketEchoHandler(std::size_t maxBodySize)
{
    if (maxBodySize > kMaxPacketSize)
    {
        throw std::invalid_argument(
            "gateway packet body limit exceeds 256 KiB"
        );
    }
    auto codec = std::make_shared<protocol::PacketCodec>(maxBodySize);
    return [codec](
               IConnection& connection,
               std::span<const std::byte> bytes
           )
    {
        try
        {
            const auto status = codec->feed(
                bytes,
                [&](const protocol::PacketHeader& request,
                    std::string_view payload)
                {
                    if (request.m_type !=
                        protocol::PacketHeader::kEchoRequest)
                    {
                        connection.close();
                        return false;
                    }
                    auto response = protocol::PacketCodec::encode(
                        protocol::PacketHeader::kEchoResponse,
                        request.m_requestId,
                        std::as_bytes(std::span(payload))
                    );
                    if (!response)
                    {
                        connection.close();
                        return false;
                    }
                    connection.send(std::as_bytes(std::span(
                        response->peek(),
                        response->readableBytes()
                    )));
                    return connection.connected();
                }
            );
            if (status == protocol::FrameDecodeStatus::Invalid)
            {
                std::cerr << "PacketCodec invalid frame id="
                          << connection.id() << '\n';
                connection.close();
            }
        }
        catch (const std::exception& error)
        {
            std::cerr << "PacketCodec callback failed: "
                      << error.what() << '\n';
            connection.close();
        }
        catch (...)
        {
            connection.close();
        }
    };
}
} // namespace service::gateway
