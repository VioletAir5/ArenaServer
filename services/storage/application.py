"""SQL-free application workflow; repositories share the caller's transaction."""


def register_player(database, *, account_id, account_name, password_digest,
                    player_id, display_name, mmr=1500):
    """Register account and player together, or persist neither."""
    def register(work):
        account = work.accounts.create(account_id, account_name, password_digest)
        player = work.accounts.create_player(player_id, account_id, display_name, mmr)
        return account, player
    return database.run(register)
