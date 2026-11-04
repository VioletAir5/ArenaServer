/** @file
 * @brief Keeps exact owner routing, frame bounds and reply identity
 * in one adapter.
 */
#include "lobby/RoomClient.h"

#include <algorithm>
#include <limits>
#include <set>
#include <stdexcept>
#include <utility>

#include "room/RoomWire.h"
#include "security/SecureErase.h"

namespace service::lobby
{
namespace
{
namespace proto = arena::room;
namespace wire = service::room::wire;
using Status = arena::rpc::Status;
using namespace std::chrono_literals;
bool nodeName(std::string_view s)
{
    return !s.empty() && s.size() <= 64 && s.front() >= 'a' &&
           s.front() <= 'z' &&
           std::all_of(
               s.begin(),
               s.end(),
               [](unsigned char c)
               {
                   return (c >= 'a' && c <= 'z') ||
                          (c >= '0' && c <= '9') || c == '_' ||
                          c == '-';
               }
           );
}
RoomClient::Config validated(RoomClient::Config c)
{
    std::set<std::string> names;
    bool valid = !c.nodes.empty() && c.nodes.size() <= 128 &&
                 c.token.size() >= 32 && c.token.size() <= 256 &&
                 c.maxInflight && c.maxInflight <= 4096 &&
                 c.timeout >= 1ms && c.timeout <= 1000ms;
    for (const auto& n : c.nodes)
        valid = valid && nodeName(n.id) &&
                names.insert(n.id).second && n.endpoint.port() &&
                n.endpoint.address().is_loopback();
    if (!valid)
    {
        arena::security::eraseSecret(c.token);
        throw std::invalid_argument(
            "invalid Lobby Room client configuration"
        );
    }
    return c;
}
bool image(const proto::Image& x)
{
    return static_cast<bool>(wire::unpackImage(x));
}
} // namespace
RoomClient::RoomClient(
    boost::asio::any_io_executor executor, Config c
)
    : m_config(validated(std::move(c)))
    , m_client(
          std::move(executor),
          m_config.nodes.front().endpoint,
          m_config.maxInflight
      )
{
}
RoomClient::~RoomClient() noexcept
{
    try
    {
        stop();
    }
    catch (...)
    {
    }
    arena::security::eraseSecret(m_config.token);
}
std::optional<std::string> RoomClient::nodeOf(std::string_view worker)
{
    const auto colon = worker.find(':');
    if (colon == std::string_view::npos ||
        !nodeName(worker.substr(0, colon)) ||
        worker.size() - colon - 1 != 32 ||
        !std::all_of(
            worker.begin() + static_cast<std::ptrdiff_t>(colon + 1),
            worker.end(),
            [](unsigned char c)
            {
                return (c >= '0' && c <= '9') ||
                       (c >= 'a' && c <= 'f');
            }
        ))
        return {};
    return std::string(worker.substr(0, colon));
}
std::vector<std::string> RoomClient::nodes() const
{
    std::vector<std::string> out;
    if (m_config.runtime)
    {
        const auto snapshot = m_config.runtime->snapshot();
        for (const auto& node : snapshot->nodes())
            if (node.service == "room" && node.ready)
                out.push_back(node.id);
    }
    else
        for (const auto& node : m_config.nodes)
            out.push_back(node.id);
    return out;
}
RoomClient::Result<boost::asio::ip::tcp::endpoint>
RoomClient::endpoint(std::string_view id, bool ready) const
{
    if (!nodeName(id))
        return std::unexpected(arena::rpc::INVALID_ARGUMENT);
    if (m_config.runtime)
    {
        const auto snapshot = m_config.runtime->snapshot();
        const auto found = std::find_if(
            snapshot->nodes().begin(),
            snapshot->nodes().end(),
            [&](const auto& n)
            { return n.service == "room" && n.id == id; }
        );
        if (found == snapshot->nodes().end() ||
            (ready && !found->ready))
            return std::unexpected(arena::rpc::UNAVAILABLE);
        return boost::asio::ip::tcp::endpoint{
            boost::asio::ip::make_address(found->host),
            found->port
        };
    }
    const auto found = std::find_if(
        m_config.nodes.begin(),
        m_config.nodes.end(),
        [&](const auto& n) { return n.id == id; }
    );
    if (found == m_config.nodes.end())
        return std::unexpected(arena::rpc::UNAVAILABLE);
    return found->endpoint;
}
RoomClient::Admission RoomClient::exchange(
    std::string id,
    bool ready,
    arena::rpc::Request request,
    arena::internal_rpc::Context context,
    arena::internal_rpc::Client::Completion completion
)
{
    if (!completion)
        return std::unexpected(arena::rpc::INVALID_ARGUMENT);
    if (context.deadline <= std::chrono::steady_clock::now())
        return std::unexpected(arena::rpc::DEADLINE_EXCEEDED);
    const auto selected = endpoint(id, ready);
    if (!selected)
        return std::unexpected(selected.error());
    request.set_caller_service("lobby");
    request.set_target_service("room");
    request.set_service_token(m_config.token);
    arena::internal_rpc::Options options;
    options.timeout = m_config.timeout;
    options.context = std::move(context);
    options.endpoint = *selected;
    // Native Client validates the final complete frame after adding
    // metadata.
    return m_client.call(
        std::move(request),
        std::move(options),
        std::move(completion)
    );
}
template <class Request, class Reply, class Verify>
RoomClient::Admission RoomClient::typed(
    arena::rpc::Request rpc,
    Request request,
    arena::internal_rpc::Context context,
    Verify verify,
    Completion<Reply> completion
)
{
    const auto node = nodeOf(request.worker_id());
    if (!completion || request.version() != 1 || !node)
        return std::unexpected(arena::rpc::INVALID_ARGUMENT);
    auto encoded = wire::serializeMessage(request);
    if (!encoded)
        return std::unexpected(arena::rpc::INVALID_ARGUMENT);
    rpc.set_payload(std::move(*encoded));
    const auto expectedWorker = request.worker_id();
    return exchange(
        *node,
        true,
        std::move(rpc),
        std::move(context),
        [completion = std::move(completion),
         expectedWorker,
         verify = std::move(verify)](
            arena::rpc::Response response
        ) mutable
        {
            if (response.status() != arena::rpc::OK)
            {
                completion(std::unexpected(response.status()));
                return;
            }
            Reply reply;
            if (!wire::parseMessage(response.payload(), reply) ||
                reply.version() != 1 ||
                reply.worker_id() != expectedWorker || !verify(reply))
            {
                completion(std::unexpected(arena::rpc::INTERNAL));
                return;
            }
            completion(std::move(reply));
        }
    );
}
RoomClient::Admission RoomClient::probe(
    std::string id,
    arena::internal_rpc::Context context,
    Completion<proto::HealthReply> completion
)
{
    if (!completion || !nodeName(id))
        return std::unexpected(arena::rpc::INVALID_ARGUMENT);
    arena::rpc::Request request;
    request.set_method("health.ready");
    const auto expected = id;
    return exchange(
        std::move(id),
        false,
        std::move(request),
        std::move(context),
        [completion = std::move(completion),
         expected](arena::rpc::Response response) mutable
        {
            if (response.status() != arena::rpc::OK)
            {
                completion(std::unexpected(response.status()));
                return;
            }
            proto::HealthReply reply;
            if (!wire::parseMessage(response.payload(), reply) ||
                reply.version() != 1 || reply.node_id() != expected ||
                nodeOf(reply.worker_id()) !=
                    std::optional(expected) ||
                !reply.has_stats())
            {
                completion(std::unexpected(arena::rpc::INTERNAL));
                return;
            }
            completion(std::move(reply));
        }
    );
}
RoomClient::Admission RoomClient::create(
    proto::CreateRequest request,
    arena::internal_rpc::Context context,
    Completion<proto::CreateReply> completion
)
{
    const auto room = request.room_id(), inc = request.incarnation();
    if (!room || !inc || !request.has_policy())
        return std::unexpected(arena::rpc::INVALID_ARGUMENT);
    arena::rpc::Request rpc;
    rpc.set_method("room.create");
    rpc.set_target_service("room");
    return typed<proto::CreateRequest, proto::CreateReply>(
        std::move(rpc),
        std::move(request),
        std::move(context),
        [room, inc](const auto& r)
        {
            return r.has_image() && r.image().room_id() == room &&
                   r.image().incarnation() == inc &&
                   !r.image().cursor() && !r.image().logical_tick() &&
                   !r.image().physical_tick() && image(r.image());
        },
        std::move(completion)
    );
}
RoomClient::Admission RoomClient::stage(
    proto::StageRequest request,
    arena::internal_rpc::Context context,
    Completion<proto::StageReceipt> completion
)
{
    const auto expected = request.image();
    const auto epoch = request.owner_epoch();
    const auto transfer = request.transfer_id();
    if (!epoch || transfer.empty() || !request.has_image() ||
        !image(expected))
        return std::unexpected(arena::rpc::INVALID_ARGUMENT);
    arena::rpc::Request rpc;
    rpc.set_method("room.stage");
    rpc.set_target_service("room");
    return typed<proto::StageRequest, proto::StageReceipt>(
        std::move(rpc),
        std::move(request),
        std::move(context),
        [expected, epoch, transfer](const auto& r)
        {
            return r.owner_epoch() == epoch &&
                   r.transfer_id() == transfer &&
                   r.room_id() == expected.room_id() &&
                   r.incarnation() == expected.incarnation() &&
                   r.physical_tick() == expected.physical_tick() &&
                   r.checksum() == expected.checksum() &&
                   r.cursor() == expected.cursor() &&
                   r.logical_tick() == expected.logical_tick() &&
                   r.fingerprint() == expected.fingerprint();
        },
        std::move(completion)
    );
}
RoomClient::Admission RoomClient::activate(
    proto::ActivateRequest request,
    arena::internal_rpc::Context context,
    Completion<proto::CacheReceipt> completion
)
{
    const auto expected = request.image();
    const auto epoch = request.owner_epoch(),
               revision = request.directory_revision();
    if (!epoch || !revision || !request.has_image() ||
        !image(expected))
        return std::unexpected(arena::rpc::INVALID_ARGUMENT);
    arena::rpc::Request rpc;
    rpc.set_method("room.activate");
    rpc.set_target_service("room");
    return typed<proto::ActivateRequest, proto::CacheReceipt>(
        std::move(rpc),
        std::move(request),
        std::move(context),
        [expected, epoch, revision](const auto& r)
        {
            return r.owner_epoch() == epoch &&
                   r.directory_revision() == revision && r.active() &&
                   r.room_id() == expected.room_id() &&
                   r.incarnation() == expected.incarnation() &&
                   r.cursor() == expected.cursor() &&
                   r.logical_tick() == expected.logical_tick() &&
                   r.fingerprint() == expected.fingerprint();
        },
        std::move(completion)
    );
}
RoomClient::Admission RoomClient::execute(
    proto::ExecuteRequest request,
    arena::internal_rpc::Context context,
    Completion<proto::ExecuteReply> completion
)
{
    if (!request.owner_epoch() || !request.has_base() ||
        !image(request.base()) || request.operations_size() < 1 ||
        request.operations_size() >
            static_cast<int>(wire::kMaxOperations) ||
        request.base().logical_tick() ==
            std::numeric_limits<std::uint64_t>::max())
        return std::unexpected(arena::rpc::INVALID_ARGUMENT);
    const auto base = request.base();
    const auto epoch = request.owner_epoch();
    auto cursor = base.cursor();
    for (int i = 0; i < request.operations_size(); ++i)
    {
        const auto& op = request.operations(i);
        if (cursor == std::numeric_limits<std::uint64_t>::max() ||
            op.cursor() != ++cursor ||
            (i + 1 == request.operations_size()
                 ? op.kind() != proto::OP_TICK ||
                       op.logical_tick() != base.logical_tick() + 1
                 : op.kind() != proto::OP_COMMAND ||
                       op.logical_tick() != 0))
            return std::unexpected(arena::rpc::INVALID_ARGUMENT);
    }
    const auto count = request.operations_size() - 1;
    arena::rpc::Request rpc;
    rpc.set_method("room.execute");
    rpc.set_target_service("room");
    return typed<proto::ExecuteRequest, proto::ExecuteReply>(
        std::move(rpc),
        std::move(request),
        std::move(context),
        [base, epoch, cursor, count](const auto& r)
        {
            if (!r.has_image() || r.owner_epoch() != epoch ||
                r.results_size() != count ||
                r.image().room_id() != base.room_id() ||
                r.image().incarnation() != base.incarnation() ||
                r.image().cursor() != cursor ||
                r.image().logical_tick() != base.logical_tick() + 1 ||
                r.image().physical_tick() < base.physical_tick() ||
                r.image().physical_tick() - base.physical_tick() >
                    1 ||
                !image(r.image()))
                return false;
            if (r.image().physical_tick() == base.physical_tick())
                return !r.has_replay();
            if (!r.has_replay())
                return false;
            const auto tick = wire::decodeTick(r.replay());
            return tick && tick->tick == r.image().physical_tick() &&
                   tick->checksum == r.image().checksum();
        },
        std::move(completion)
    );
}
RoomClient::Admission RoomClient::hint(
    arena::rpc::Request rpc,
    proto::HintRequest request,
    arena::internal_rpc::Context context,
    Completion<proto::CacheReceipt> completion
)
{
    const auto room = request.room_id(), inc = request.incarnation(),
               epoch = request.owner_epoch(),
               revision = request.directory_revision();
    if (!room || !inc || !epoch || !revision)
        return std::unexpected(arena::rpc::INVALID_ARGUMENT);
    return typed<proto::HintRequest, proto::CacheReceipt>(
        std::move(rpc),
        std::move(request),
        std::move(context),
        [room, inc, epoch, revision](const auto& r)
        {
            return r.room_id() == room && r.incarnation() == inc &&
                   r.owner_epoch() == epoch &&
                   r.directory_revision() == revision && !r.active();
        },
        std::move(completion)
    );
}
RoomClient::Admission RoomClient::freeze(
    proto::HintRequest r,
    arena::internal_rpc::Context c,
    Completion<proto::CacheReceipt> done
)
{
    arena::rpc::Request rpc;
    rpc.set_method("room.freeze");
    rpc.set_target_service("room");
    return hint(
        std::move(rpc),
        std::move(r),
        std::move(c),
        std::move(done)
    );
}
RoomClient::Admission RoomClient::retire(
    proto::HintRequest r,
    arena::internal_rpc::Context c,
    Completion<proto::CacheReceipt> done
)
{
    arena::rpc::Request rpc;
    rpc.set_method("room.retire");
    rpc.set_target_service("room");
    return hint(
        std::move(rpc),
        std::move(r),
        std::move(c),
        std::move(done)
    );
}
void RoomClient::stop()
{
    m_client.stop();
}
std::size_t RoomClient::inflight() const
{
    return m_client.inflight();
}
} // namespace service::lobby
