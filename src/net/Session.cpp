#include<net/Session.h>

#include<utility>

namespace service
{
    Session::Session(SessionID id, Socket&& socket)
        :_id{id}, _socket(std::move(socket))
    {

    }
} // namespace service
