# Domain lifecycle models

`arena_domain` defines Player, recoverable Session, Ticket, Room, Battle and Settlement.
It is a header-only business model library, not an executable service.

- [Vocabulary](../CONTEXT.md)
- [State, owner and transition contract](../docs/domain/lifecycles.md)
- [Independent table-driven tests](../tests/domain/LifecycleTest.cpp)

Each entity has a typed nonzero identity and one immutable owner thread. State reads and
transitions return `std::expected`; failed transitions preserve state and revision.
The generic graph algorithm lives in `shared/include/state/OwnedStateMachine.h`.

The service registries, authorization, lease timers, actor coordination and persistence
are future integrations. Only validated owner facts should become lifecycle events.
