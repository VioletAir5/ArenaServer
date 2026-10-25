#include <unistd.h>

#include <filesystem>
#include <fstream>

#include <gtest/gtest.h>

#include "auth/FilePasswordAccounts.h"
using namespace service::auth;
namespace
{
const std::string vectorDigest =
    "$arena-pbkdf2-sha256$100000$000102030405060708090a0b0c0d0e0f$"
    "7da06b355b2fa40026be044b0145e911029eafed74d19429b67044c09e7106c"
    "8";
struct AccountsFile
{
    std::filesystem::path path;
    AccountsFile(std::string_view contents)
    {
        char name[] = "/tmp/arena-accounts-XXXXXX";
        const int fd = ::mkstemp(name);
        if (fd < 0)
            throw std::runtime_error("mkstemp");
        ::close(fd);
        path = name;
        std::ofstream(path) << contents;
    }
    ~AccountsFile()
    {
        std::filesystem::remove(path);
    }
};
} // namespace
TEST(PasswordAdapters, RandomSaltRoundTripRejectsWrongPassword)
{
    Pbkdf2PasswordDigest d;
    auto a = d.hash("correct-horse"), b = d.hash("correct-horse");
    ASSERT_TRUE(a);
    ASSERT_TRUE(b);
    EXPECT_NE(*a, *b);
    EXPECT_TRUE(d.validEncoding(*a));
    EXPECT_EQ(d.verify("correct-horse", *a), true);
    EXPECT_EQ(d.verify("wrong", *a), false);
}
TEST(
    PasswordAdapters, RejectsMalformedAndUnboundedWorkBeforeDerivation
)
{
    Pbkdf2PasswordDigest d;
    for (const auto* value :
         {"plaintext",
          "$arena-pbkdf2-sha256$0$aa$bb",
          "$arena-pbkdf2-sha256$2147483648$aa$bb",
          "$arena-pbkdf2-sha256$2000001$aa$bb"})
    {
        EXPECT_FALSE(d.validEncoding(value));
        EXPECT_EQ(
            d.verify("p", value).error(),
            AuthError::Unavailable
        );
    }
    EXPECT_EQ(d.hash("").error(), AuthError::InvalidCredentials);
    EXPECT_EQ(
        d.hash(std::string(1025, 'x')).error(),
        AuthError::InvalidCredentials
    );
}
TEST(PasswordAdapters, LoadsIndependentDigestAndRejectsUnknownAccount)
{
    AccountsFile f("alice\t42\t" + vectorDigest + "\n");
    auto accounts = std::make_shared<FilePasswordAccounts>(f.path);
    LoginAuthenticator login(
        accounts,
        std::make_shared<Pbkdf2PasswordDigest>()
    );
    EXPECT_EQ(
        login.authenticate("alice", "test-password")->value(),
        42
    );
    EXPECT_EQ(
        login.authenticate("alice", "wrong").error(),
        AuthError::InvalidCredentials
    );
    EXPECT_EQ(
        login.authenticate("unknown", "test-password").error(),
        AuthError::InvalidCredentials
    );
}
TEST(PasswordAdapters, RejectsDuplicateIdentitiesAndMalformedFiles)
{
    for (const auto& contents :
         {std::string{},
          std::string("alice\t42\tbad\n"),
          "alice\t42\t" + vectorDigest + "\nalice\t43\t" +
              vectorDigest + "\n",
          "alice\t42\t" + vectorDigest + "\nbob\t42\t" +
              vectorDigest + "\n",
          "alice\t0\t" + vectorDigest + "\n",
          "alice\t42\t" + vectorDigest + "\n\n"})
    {
        AccountsFile f(contents);
        EXPECT_THROW(
            FilePasswordAccounts{f.path},
            std::invalid_argument
        );
    }
    EXPECT_THROW(
        FilePasswordAccounts{"/does-not-exist/arena.tsv"},
        std::invalid_argument
    );
}
