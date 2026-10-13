#pragma once

#include "auth/LoginAuthenticator.h"
#include "net/AccountSessions.h"
#include "net/MessageRouter.h"

namespace service::gateway
{
// All captured adapters/validators and the directory are shared across workers.
std::shared_ptr<const MessageRegistry> makeLoginMessageRegistry(
    std::shared_ptr<const service::auth::LoginAuthenticator>,
    std::shared_ptr<const service::auth::JwtTokens>,
    std::shared_ptr<AccountSessions>);
} // namespace service::gateway
