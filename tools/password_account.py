#!/usr/bin/env python3
"""Print one salted account TSV row; redirect into an access-restricted file."""
import argparse
import getpass
import hashlib
import secrets

def digest(password):
    salt = secrets.token_bytes(16)
    key = hashlib.pbkdf2_hmac("sha256", password, salt, 600000, 32)
    return f"$arena-pbkdf2-sha256$600000${salt.hex()}${key.hex()}"

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("account")
    parser.add_argument("player_id", type=int)
    args = parser.parse_args()
    if not (0 < args.player_id < 2**64) or not (1 <= len(args.account) <= 128) or any(
            ord(c) < 33 or ord(c) > 126 for c in args.account):
        parser.error("account must be printable ASCII; player_id must be a positive uint64")
    password = getpass.getpass("Password: ").encode("utf-8")
    if not 1 <= len(password) <= 1024:
        parser.error("password must contain 1..1024 UTF-8 bytes")
    if getpass.getpass("Confirm password: ").encode("utf-8") != password:
        parser.error("passwords do not match")
    print(f"{args.account}\t{args.player_id}\t{digest(password)}")

if __name__ == "__main__":
    main()
