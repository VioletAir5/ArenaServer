"""Unchanged public TCP/WS bot messages shared by real service acceptance."""
from RoomIntegrationTest import RoomPeer, decode_fields, snapshot, unsigned_field
from OnlinePresenceIntegrationTest import parse, scalar


def settlement(raw):
    f = decode_fields(raw)
    return dict(result=scalar(f, 1), match=scalar(f, 2), room=scalar(f, 3),
                tick=scalar(f, 4), checksum=scalar(f, 5), reason=scalar(f, 6),
                winner=scalar(f, 7), players=[decode_fields(p) for p in f.get(8, [])])


def managed(raw):
    f = decode_fields(scalar(parse(raw), 1))
    members = {}
    for raw_member in f.get(4, []):
        m = parse(raw_member)
        members[scalar(m, 1)] = dict(session=scalar(m, 2), epoch=scalar(m, 3),
                                   ready=scalar(m, 4), presence=scalar(m, 5))
    result = dict(snapshot=snapshot(scalar(f, 1)), match=scalar(f, 2), phase=scalar(f, 3),
                  members=members, played=scalar(f, 7), duration=scalar(f, 8))
    if 9 in f:
        result["settlement"] = settlement(scalar(f, 9))
    return result


class Bot(RoomPeer):
    def match_join(self, key):
        return parse(scalar(parse(self.rpc(40, unsigned_field(1, key), 41)), 1))

    def match_status(self, ticket_id):
        return parse(scalar(parse(self.rpc(44, unsigned_field(1, ticket_id), 45)), 1))

    def status(self):
        return managed(self.rpc(56, b"", 57))

    def ready(self, state, player):
        p = state["snapshot"]
        payload = unsigned_field(1, 1) + unsigned_field(2, p["incarnation"])
        payload += unsigned_field(3, p["players"][player]["epoch"])
        return managed(self.rpc(54, payload, 55))

    def result(self, match):
        return settlement(scalar(parse(self.rpc(58, unsigned_field(1, match), 59)), 1))
