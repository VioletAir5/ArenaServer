#include "protocol/EnvelopeCodec.hpp"

#include <cstdint>

#include <google/protobuf/io/coded_stream.h>

namespace service::protocol
{
namespace
{
bool errorWithinDepth(const arena::protocol::Error& error, int depth)
{
    if (depth > EnvelopeCodec::kMaxDepth)
    {
        return false;
    }
    for (const auto& cause : error.causes())
    {
        if (!errorWithinDepth(cause, depth + 1))
        {
            return false;
        }
    }
    return true;
}

bool validEnvelope(const arena::protocol::Envelope& envelope)
{
    switch (envelope.payload_case())
    {
    case arena::protocol::Envelope::kLoginRequest:
    case arena::protocol::Envelope::kPing:
    case arena::protocol::Envelope::kPong:
        return true;
    case arena::protocol::Envelope::kLoginResponse:
        return !envelope.login_response().has_error() ||
               errorWithinDepth(envelope.login_response().error(), 3);
    case arena::protocol::Envelope::kError:
        return errorWithinDepth(envelope.error(), 2);
    case arena::protocol::Envelope::PAYLOAD_NOT_SET:
        return false;
    }
    return false;
}
} // namespace

bool EnvelopeCodec::decode(
    std::span<const std::byte> bytes,
    arena::protocol::Envelope& output
)
{
    if (bytes.empty() || bytes.size() > kMaxBytes)
    {
        return false;
    }
    google::protobuf::io::CodedInputStream input(
        reinterpret_cast<const std::uint8_t*>(bytes.data()),
        static_cast<int>(bytes.size())
    );
    input.SetTotalBytesLimit(static_cast<int>(kMaxBytes));
    input.SetRecursionLimit(kMaxDepth);

    arena::protocol::Envelope parsed;
    if (!parsed.ParseFromCodedStream(&input) ||
        !input.ConsumedEntireMessage() || !validEnvelope(parsed))
    {
        return false;
    }
    output.Swap(&parsed);
    return true;
}

std::optional<std::string> EnvelopeCodec::encode(
    const arena::protocol::Envelope& envelope
)
{
    if (!validEnvelope(envelope) ||
        envelope.ByteSizeLong() > kMaxBytes)
    {
        return std::nullopt;
    }
    std::string bytes;
    if (!envelope.SerializeToString(&bytes))
    {
        return std::nullopt;
    }
    return bytes;
}
} // namespace service::protocol