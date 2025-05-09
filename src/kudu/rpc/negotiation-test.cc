// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "kudu/rpc/negotiation.h"

#include <krb5/krb5.h> // IWYU pragma: keep
#include <sasl/sasl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cstdio>
#include <cstdlib>
#include <functional>
#include <memory>
#include <optional>
#include <ostream>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/gutil/basictypes.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/walltime.h"
#include "kudu/rpc/client_negotiation.h"
#include "kudu/rpc/remote_user.h"
#include "kudu/rpc/rpc-test-base.h"
#include "kudu/rpc/sasl_common.h"
#include "kudu/rpc/server_negotiation.h"
#include "kudu/security/cert.h"
#include "kudu/security/crypto.h"
#include "kudu/security/security-test-util.h"
#include "kudu/security/security_flags.h"
#include "kudu/security/test/mini_kdc.h"
#include "kudu/security/tls_context.h"
#include "kudu/security/tls_socket.h"
#include "kudu/security/token.pb.h"
#include "kudu/security/token_signer.h"
#include "kudu/security/token_signing_key.h"
#include "kudu/security/token_verifier.h"
#include "kudu/util/env.h"
#include "kudu/util/jwt-util.h"
#include "kudu/util/jwt_test_certs.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/net/socket.h"
#include "kudu/util/path_util.h"
#include "kudu/util/status.h"
#include "kudu/util/subprocess.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"
#include "kudu/util/trace.h"
#include "kudu/util/user.h"

namespace kudu {
class JwtVerifier;
}  // namespace kudu

// HACK: MIT Kerberos doesn't have any way of determining its version number,
// but the error messages in krb5-1.10 and earlier are broken due to
// a bug: http://krbdev.mit.edu/rt/Ticket/Display.html?id=6973
//
// Since we don't have any way to explicitly figure out the version, we just
// look for this random macro which was added in 1.11 (the same version in which
// the above bug was fixed).
#ifndef KRB5_RESPONDER_QUESTION_PASSWORD
#define KRB5_VERSION_LE_1_10
#endif

#if defined(__APPLE__)
// Almost all functions in the SASL API are marked as deprecated
// since macOS 10.11.
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#endif // #if defined(__APPLE__)

DEFINE_bool(is_test_child, false,
            "Used by tests which require clean processes. "
            "See TestDisableInit.");
DECLARE_bool(rpc_encrypt_loopback_connections);
DECLARE_bool(rpc_trace_negotiation);

using kudu::security::Cert;
using kudu::security::PkiConfig;
using kudu::security::PrivateKey;
using kudu::security::RpcEncryption;
using kudu::security::SignedTokenPB;
using kudu::security::TlsContext;
using kudu::security::TokenSigner;
using kudu::security::TokenSigningPrivateKey;
using kudu::security::TokenVerifier;
using std::nullopt;
using std::optional;
using std::string;
using std::thread;
using std::unique_ptr;
using std::vector;

namespace kudu {
namespace rpc {

// The negotiation configuration for a client or server endpoint.
struct EndpointConfig {
  // The PKI configuration.
  PkiConfig pki;
  // The supported SASL mechanisms.
  vector<SaslMechanism::Type> sasl_mechs;
  // For the client, whether the client has the token.
  // For the server, whether the server has the TSK.
  bool token;
  // For the client, whether the client has the JWT.
  // For the server, whether the server has a JWTVerifier
  bool jwt;
  RpcEncryption encryption;
};
std::ostream& operator<<(std::ostream& o, EndpointConfig config) {
  auto bool_string = [] (bool b) { return b ? "true" : "false"; };
  o << "{pki: " << config.pki
    << ", sasl-mechs: [" << JoinMapped(config.sasl_mechs, SaslMechanism::name_of, ", ")
    << "], token: " << bool_string(config.token)
    << ", jwt: " << bool_string(config.jwt)
    << ", encryption: ";

  switch (config.encryption) {
    case RpcEncryption::DISABLED: o << "DISABLED"; break;
    case RpcEncryption::OPTIONAL: o << "OPTIONAL"; break;
    case RpcEncryption::REQUIRED: o << "REQUIRED"; break;
  }

  o << "}";
  return o;
}

// A description of a negotiation sequence, including client and server
// configuration, as well as expected results.
struct NegotiationDescriptor {
  EndpointConfig client;
  EndpointConfig server;

  bool use_test_socket;

  bool rpc_encrypt_loopback;

  // The expected client status from negotiating.
  Status client_status;
  // The expected server status from negotiating.
  Status server_status;

  // The expected negotiated authentication type.
  AuthenticationType negotiated_authn;

  // The expected SASL mechanism, if SASL authentication is negotiated.
  SaslMechanism::Type negotiated_mech;

  // Whether the negotiation is expected to perform a TLS handshake.
  bool tls_negotiated;
};
std::ostream& operator<<(std::ostream& o, NegotiationDescriptor c) {
  auto bool_string = [] (bool b) { return b ? "true" : "false"; };
  o << "{client: " << c.client
    << ", server: " << c.server
    << "}, rpc-encrypt-loopback: " << bool_string(c.rpc_encrypt_loopback);
  return o;
}

class NegotiationTestSocket : public Socket {
 public:
  // Return an arbitrary public IP
  Status GetPeerAddress(Sockaddr *cur_addr) const override {
    return cur_addr->ParseString("8.8.8.8:12345", 0);
  }
};

class TestNegotiation : public RpcTestBase,
                        public ::testing::WithParamInterface<NegotiationDescriptor> {
 public:
  void SetUp() override {
    RpcTestBase::SetUp();
    ASSERT_OK(SaslInit());
  }
};

TEST_P(TestNegotiation, TestNegotiation) {
  NegotiationDescriptor desc = GetParam();

  // Generate a trusted root certificate.
  PrivateKey ca_key;
  Cert ca_cert;
  ASSERT_OK(GenerateSelfSignedCAForTests(&ca_key, &ca_cert));

  // Create and configure a TLS context for each endpoint.
  TlsContext client_tls_context;
  TlsContext server_tls_context;
  ASSERT_OK(client_tls_context.Init());
  ASSERT_OK(server_tls_context.Init());
  ASSERT_OK(ConfigureTlsContext(desc.client.pki, ca_cert, ca_key, &client_tls_context));
  ASSERT_OK(ConfigureTlsContext(desc.server.pki, ca_cert, ca_key, &server_tls_context));

  FLAGS_rpc_encrypt_loopback_connections = desc.rpc_encrypt_loopback;

  // Generate an optional client token and server token verifier.
  // Note: the authz token validity period doesn't matter because we're only
  // concerned with authenticating the connection.
  TokenSigner token_signer(60, 0, 20, std::make_shared<TokenVerifier>());
  {
    unique_ptr<TokenSigningPrivateKey> key;
    ASSERT_OK(token_signer.CheckNeedKey(&key));
    // No keys are available yet, so should be able to add.
    ASSERT_NE(nullptr, key.get());
    ASSERT_OK(token_signer.AddKey(std::move(key)));
  }
  TokenVerifier token_verifier;
  optional<SignedTokenPB> authn_token;
  if (desc.client.token) {
    authn_token = SignedTokenPB();
    security::TokenPB token;
    token.set_expire_unix_epoch_seconds(WallTime_Now() + 60);
    token.mutable_authn()->set_username("client-token");
    ASSERT_TRUE(token.SerializeToString(authn_token->mutable_token_data()));
    ASSERT_OK(token_signer.SignToken(&*authn_token));
  }
  if (desc.server.token) {
    ASSERT_OK(token_verifier.ImportKeys(token_signer.verifier().ExportKeys()));
  }

  // Create JWT token
  // Create jwt_verifier
  const string jwks_file_name = "keys.jwks";
  const string jwt_test_dir = GetTestPath("jwt");
  const string jwt_data = kudu::CreateTestJWT(true);
  ASSERT_OK(kudu::CreateTestJWKSFile(jwt_test_dir, jwks_file_name));
  std::shared_ptr<JwtVerifier> jwt_verifier;
  if (desc.server.jwt) {
    jwt_verifier = std::make_shared<kudu::KeyBasedJwtVerifier>(
        JoinPathSegments(jwt_test_dir, jwks_file_name));
    ASSERT_OK(jwt_verifier-> Init());
  }
  optional<security::JwtRawPB> jwt_token;
  if (desc.client.jwt) {
    jwt_token = security::JwtRawPB();
    jwt_token->set_jwt_data(jwt_data);
  }

  // Create the listening socket, client socket, and server socket.
  Socket listening_socket;
  Sockaddr server_addr = Sockaddr::Wildcard();
  ASSERT_OK(listening_socket.Init(server_addr.family(), 0));
  ASSERT_OK(listening_socket.BindAndListen(server_addr, 1));
  ASSERT_OK(listening_socket.GetSocketAddress(&server_addr));

  unique_ptr<Socket> client_socket(new Socket());
  ASSERT_OK(client_socket->Init(server_addr.family(), 0));
  ASSERT_OK(client_socket->Connect(server_addr));

  unique_ptr<Socket> server_socket(desc.use_test_socket ?
                                   new NegotiationTestSocket() :
                                   new Socket());

  Sockaddr client_addr;
  CHECK_OK(listening_socket.Accept(server_socket.get(), &client_addr, 0));

  // Create and configure the client and server negotiation instances.
  ClientNegotiation client_negotiation(std::move(client_socket),
                                       &client_tls_context,
                                       authn_token,
                                       jwt_token,
                                       desc.client.encryption,
                                       desc.rpc_encrypt_loopback,
                                       "kudu");
  ServerNegotiation server_negotiation(std::move(server_socket),
                                       &server_tls_context,
                                       &token_verifier,
                                       jwt_verifier.get(),
                                       desc.server.encryption,
                                       desc.rpc_encrypt_loopback,
                                       "kudu");

  // Set client and server SASL mechanisms.
  MiniKdc kdc;
  bool kdc_started = false;
  auto start_kdc_once = [&] () {
    if (!kdc_started) {
      kdc_started = true;
      RETURN_NOT_OK(kdc.Start());
    }
    return Status::OK();
  };
  for (auto mech : desc.client.sasl_mechs) {
    switch (mech) {
      case SaslMechanism::INVALID: break;
      case SaslMechanism::PLAIN:
        ASSERT_OK(client_negotiation.EnablePlain("client-plain", "client-password"));
        break;
      case SaslMechanism::GSSAPI:
        ASSERT_OK(start_kdc_once());
        ASSERT_OK(kdc.CreateUserPrincipal("client-gssapi"));
        ASSERT_OK(kdc.Kinit("client-gssapi"));
        ASSERT_OK(kdc.SetKrb5Environment());
        client_negotiation.set_server_fqdn("127.0.0.1");
        ASSERT_OK(client_negotiation.EnableGSSAPI());
        break;
    }
  }
  for (auto mech : desc.server.sasl_mechs) {
    switch (mech) {
      case SaslMechanism::INVALID: break;
      case SaslMechanism::PLAIN:
        ASSERT_OK(server_negotiation.EnablePlain());
        break;
      case SaslMechanism::GSSAPI:
        ASSERT_OK(start_kdc_once());
        // Create the server principal and keytab.
        string kt_path;
        ASSERT_OK(kdc.CreateServiceKeytab("kudu/127.0.0.1", &kt_path));
        PCHECK(setenv("KRB5_KTNAME", kt_path.c_str(), 1 /*replace*/) == 0);
        server_negotiation.set_server_fqdn("127.0.0.1");
        ASSERT_OK(server_negotiation.EnableGSSAPI());
        break;
    }
  }

  // Run the client/server negotiation. Because negotiation is blocking, it
  // has to be done on separate threads.
  Status client_status;
  Status server_status;
  thread client_thread([&] () {
      scoped_refptr<Trace> t(new Trace());
      ADOPT_TRACE(t.get());
      client_status = client_negotiation.Negotiate();
      // Close the socket so that the server will not block forever on error.
      ignore_result(client_negotiation.socket()->Close());

      if (FLAGS_rpc_trace_negotiation || !client_status.ok()) {
        string msg = Trace::CurrentTrace()->DumpToString();
        if (!client_status.ok()) {
          LOG(WARNING) << "Failed client RPC negotiation. Client trace:\n" << msg;
        } else {
          LOG(INFO) << "RPC negotiation tracing enabled. Client trace:\n" << msg;
        }
      }
  });
  thread server_thread([&] () {
      scoped_refptr<Trace> t(new Trace());
      ADOPT_TRACE(t.get());
      server_status = server_negotiation.Negotiate();
      // Close the socket so that the client will not block forever on error.
      ignore_result(server_negotiation.socket()->Close());

      if (FLAGS_rpc_trace_negotiation || !server_status.ok()) {
        string msg = Trace::CurrentTrace()->DumpToString();
        if (!server_status.ok()) {
          LOG(WARNING) << "Failed server RPC negotiation. Server trace:\n" << msg;
        } else {
          LOG(INFO) << "RPC negotiation tracing enabled. Server trace:\n" << msg;
        }
      }
  });
  client_thread.join();
  server_thread.join();

  // Check the negotiation outcome against the expected outcome.
  EXPECT_EQ(desc.client_status.CodeAsString(), client_status.CodeAsString());
  EXPECT_EQ(desc.server_status.CodeAsString(), server_status.CodeAsString());
  EXPECT_STR_MATCHES(client_status.ToString(), desc.client_status.ToString());
  EXPECT_STR_MATCHES(server_status.ToString(), desc.server_status.ToString());

  if (client_status.ok()) {
    EXPECT_TRUE(server_status.ok());

    // Make sure the negotiations agree with the expected values.
    EXPECT_EQ(desc.negotiated_authn, client_negotiation.negotiated_authn());
    EXPECT_EQ(desc.negotiated_mech, client_negotiation.negotiated_mechanism());
    EXPECT_EQ(desc.negotiated_authn, server_negotiation.negotiated_authn());
    EXPECT_EQ(desc.negotiated_mech, server_negotiation.negotiated_mechanism());
    EXPECT_EQ(desc.tls_negotiated, server_negotiation.tls_negotiated());
    EXPECT_EQ(desc.tls_negotiated, server_negotiation.tls_negotiated());

    bool client_tls_socket = dynamic_cast<security::TlsSocket*>(client_negotiation.socket());
    bool server_tls_socket = dynamic_cast<security::TlsSocket*>(server_negotiation.socket());
    EXPECT_EQ(desc.rpc_encrypt_loopback, client_tls_socket);
    EXPECT_EQ(desc.rpc_encrypt_loopback, server_tls_socket);

    // Check that the expected user subject is authenticated.
    RemoteUser remote_user = server_negotiation.take_authenticated_user();
    switch (server_negotiation.negotiated_authn()) {
      case AuthenticationType::SASL:
        switch (server_negotiation.negotiated_mechanism()) {
          case SaslMechanism::PLAIN:
            EXPECT_EQ("client-plain", remote_user.username());
            break;
          case SaslMechanism::GSSAPI:
            EXPECT_EQ("client-gssapi", remote_user.username());
            EXPECT_EQ("client-gssapi@KRBTEST.COM", remote_user.principal().value_or(""));
            break;
          case SaslMechanism::INVALID: LOG(FATAL) << "invalid mechanism negotiated";
        }
        break;
      case AuthenticationType::CERTIFICATE: {
        // We expect the cert to be using the local username, because it hasn't
        // logged in from any Keytab.
        string expected;
        CHECK_OK(GetLoggedInUser(&expected));
        EXPECT_EQ(expected, remote_user.username());
        EXPECT_FALSE(remote_user.principal());
        break;
      }
      case AuthenticationType::TOKEN:
        EXPECT_EQ("client-token", remote_user.username());
        break;
      case AuthenticationType::JWT:
        break;
      case AuthenticationType::INVALID: LOG(FATAL) << "invalid authentication negotiated";
    }
  }
}

INSTANTIATE_TEST_SUITE_P(NegotiationCombinations,
                         TestNegotiation,
                         ::testing::Values(

        // client: no authn/mechs
        // server: no authn/mechs
        NegotiationDescriptor {
          EndpointConfig {
            PkiConfig::NONE,
            {},
            false,
            false,
            RpcEncryption::OPTIONAL,
          },
          EndpointConfig {
            PkiConfig::NONE,
            {},
            false,
            false,
            RpcEncryption::OPTIONAL,
          },
          false,
          false,
          Status::NotAuthorized("client is not configured with an authentication type"),
          Status::NetworkError(""),
          AuthenticationType::INVALID,
          SaslMechanism::INVALID,
          false,
        },

        // client: PLAIN
        // server: no authn/mechs
        NegotiationDescriptor {
          EndpointConfig {
            PkiConfig::NONE,
            { SaslMechanism::PLAIN },
            false,
            false,
            RpcEncryption::OPTIONAL,
          },
          EndpointConfig {
            PkiConfig::NONE,
            {},
            false,
            false,
            RpcEncryption::OPTIONAL,
          },
          false,
          false,
          Status::NotAuthorized(".* server mechanism list is empty"),
          Status::NotAuthorized(".* server mechanism list is empty"),
          AuthenticationType::INVALID,
          SaslMechanism::INVALID,
          false,
        },

        // client: PLAIN
        // server: PLAIN
        NegotiationDescriptor {
          EndpointConfig {
            PkiConfig::NONE,
            { SaslMechanism::PLAIN },
            false,
            false,
            RpcEncryption::OPTIONAL
          },
          EndpointConfig {
            PkiConfig::NONE,
            { SaslMechanism::PLAIN },
            false,
            false,
            RpcEncryption::DISABLED,
          },
          false,
          false,
          Status::OK(),
          Status::OK(),
          AuthenticationType::SASL,
          SaslMechanism::PLAIN,
          false,
        },

        // client: GSSAPI
        // server: GSSAPI
        NegotiationDescriptor {
          EndpointConfig {
            PkiConfig::NONE,
            { SaslMechanism::GSSAPI },
            false,
            false,
            RpcEncryption::OPTIONAL,
          },
          EndpointConfig {
            PkiConfig::NONE,
            { SaslMechanism::GSSAPI },
            false,
            false,
            RpcEncryption::DISABLED,
          },
          false,
          false,
          Status::OK(),
          Status::OK(),
          AuthenticationType::SASL,
          SaslMechanism::GSSAPI,
          false,
        },

        // client: GSSAPI, PLAIN
        // server: GSSAPI, PLAIN
        NegotiationDescriptor {
          EndpointConfig {
            PkiConfig::NONE,
            { SaslMechanism::GSSAPI, SaslMechanism::PLAIN },
            false,
            false,
            RpcEncryption::OPTIONAL,
          },
          EndpointConfig {
            PkiConfig::NONE,
            { SaslMechanism::GSSAPI, SaslMechanism::PLAIN },
            false,
            false,
            RpcEncryption::DISABLED,
          },
          false,
          false,
          Status::OK(),
          Status::OK(),
          AuthenticationType::SASL,
          SaslMechanism::GSSAPI,
          false,
        },

        // client: GSSAPI, PLAIN
        // server: GSSAPI
        NegotiationDescriptor {
          EndpointConfig {
            PkiConfig::NONE,
            { SaslMechanism::GSSAPI, SaslMechanism::PLAIN },
            false,
            false,
            RpcEncryption::OPTIONAL,
          },
          EndpointConfig {
            PkiConfig::NONE,
            { SaslMechanism::GSSAPI },
            false,
            false,
            RpcEncryption::DISABLED,
          },
          false,
          false,
          Status::OK(),
          Status::OK(),
          AuthenticationType::SASL,
          SaslMechanism::GSSAPI,
          false,
        },

        // client: PLAIN
        // server: GSSAPI
        NegotiationDescriptor {
          EndpointConfig {
            PkiConfig::NONE,
            { SaslMechanism::PLAIN },
            false,
            false,
            RpcEncryption::OPTIONAL,
          },
          EndpointConfig {
            PkiConfig::NONE,
            { SaslMechanism::GSSAPI },
            false,
            false,
            RpcEncryption::DISABLED,
          },
          false,
          false,
          Status::NotAuthorized(".*client does not have Kerberos credentials available"),
          Status::NetworkError(""),
          AuthenticationType::INVALID,
          SaslMechanism::INVALID,
          false,
        },

        // client: GSSAPI,
        // server: GSSAPI, self-signed cert
        // loopback encryption
        NegotiationDescriptor {
          EndpointConfig {
            PkiConfig::NONE,
            { SaslMechanism::GSSAPI },
            false,
            false,
            RpcEncryption::OPTIONAL,
          },
          EndpointConfig {
            PkiConfig::SELF_SIGNED,
            { SaslMechanism::GSSAPI },
            false,
            false,
            RpcEncryption::OPTIONAL,
          },
          false,
          true,
          Status::OK(),
          Status::OK(),
          AuthenticationType::SASL,
          SaslMechanism::GSSAPI,
          true,
        },

        // client: GSSAPI, signed-cert
        // server: GSSAPI, self-signed cert
        // This tests that the server will not advertise CERTIFICATE authentication,
        // since it doesn't have a trusted cert.
        NegotiationDescriptor {
          EndpointConfig {
            PkiConfig::SIGNED,
            { SaslMechanism::GSSAPI },
            false,
            false,
            RpcEncryption::OPTIONAL,
          },
          EndpointConfig {
            PkiConfig::SELF_SIGNED,
            { SaslMechanism::GSSAPI },
            false,
            false,
            RpcEncryption::OPTIONAL,
          },
          false,
          false,
          Status::OK(),
          Status::OK(),
          AuthenticationType::SASL,
          SaslMechanism::GSSAPI,
          true,
        },

        // client: PLAIN,
        // server: PLAIN, self-signed cert
        NegotiationDescriptor {
          EndpointConfig {
            PkiConfig::NONE,
            { SaslMechanism::PLAIN },
            false,
            false,
            RpcEncryption::OPTIONAL,
          },
          EndpointConfig {
            PkiConfig::SELF_SIGNED,
            { SaslMechanism::PLAIN },
            false,
            false,
            RpcEncryption::OPTIONAL,
          },
          false,
          false,
          Status::OK(),
          Status::OK(),
          AuthenticationType::SASL,
          SaslMechanism::PLAIN,
          true,
        },

        // client: signed-cert
        // server: signed-cert
        NegotiationDescriptor {
          EndpointConfig {
            PkiConfig::SIGNED,
            { SaslMechanism::GSSAPI },
            false,
            false,
            RpcEncryption::OPTIONAL,
          },
          EndpointConfig {
            PkiConfig::SIGNED,
            { SaslMechanism::GSSAPI },
            false,
            false,
            RpcEncryption::OPTIONAL,
          },
          false,
          false,
          Status::OK(),
          Status::OK(),
          AuthenticationType::CERTIFICATE,
          SaslMechanism::INVALID,
          true,
        },

        // client: token, trusted cert
        // server: token, signed-cert, GSSAPI
        NegotiationDescriptor {
          EndpointConfig {
            PkiConfig::TRUSTED,
            { },
            true,
            false,
            RpcEncryption::OPTIONAL,
          },
          EndpointConfig {
            PkiConfig::SIGNED,
            { SaslMechanism::PLAIN },
            true,
            false,
            RpcEncryption::OPTIONAL,
          },
          false,
          false,
          Status::OK(),
          Status::OK(),
          AuthenticationType::TOKEN,
          SaslMechanism::INVALID,
          true,
        },

        // client: PLAIN, token
        // server: PLAIN, token, signed cert
        // Test that the client won't negotiate token authn if it doesn't have a
        // trusted cert. We aren't expecting this to happen in practice (the
        // token and trusted CA cert should come as a package).
        NegotiationDescriptor {
          EndpointConfig {
            PkiConfig::NONE,
            { SaslMechanism::PLAIN },
            true,
            false,
            RpcEncryption::OPTIONAL,
          },
          EndpointConfig {
            PkiConfig::SIGNED,
            { SaslMechanism::PLAIN },
            true,
            false,
            RpcEncryption::OPTIONAL,
          },
          false,
          false,
          Status::OK(),
          Status::OK(),
          AuthenticationType::SASL,
          SaslMechanism::PLAIN,
          true,
        },

        // client: PLAIN, GSSAPI, signed-cert, token
        // server: PLAIN, GSSAPI, signed-cert, token
        NegotiationDescriptor {
          EndpointConfig {
            PkiConfig::SIGNED,
            { SaslMechanism::PLAIN, SaslMechanism::GSSAPI },
            true,
            false,
            RpcEncryption::OPTIONAL,
          },
          EndpointConfig {
            PkiConfig::SIGNED,
            { SaslMechanism::PLAIN, SaslMechanism::GSSAPI },
            true,
            false,
            RpcEncryption::OPTIONAL,
          },
          false,
          false,
          Status::OK(),
          Status::OK(),
          AuthenticationType::CERTIFICATE,
          SaslMechanism::INVALID,
          true,
        },

        // client: PLAIN, TLS disabled
        // server: PLAIN, TLS required
        NegotiationDescriptor {
          EndpointConfig {
            PkiConfig::NONE,
            { SaslMechanism::PLAIN },
            false,
            false,
            RpcEncryption::DISABLED,
          },
          EndpointConfig {
            PkiConfig::SIGNED,
            { SaslMechanism::PLAIN },
            false,
            false,
            RpcEncryption::REQUIRED,
          },
          false,
          false,
          Status::NotAuthorized(".*client does not support required TLS encryption"),
          Status::NotAuthorized(".*client does not support required TLS encryption"),
          AuthenticationType::SASL,
          SaslMechanism::PLAIN,
          true,
        },

        // client: PLAIN, TLS required
        // server: PLAIN, TLS disabled
        NegotiationDescriptor {
          EndpointConfig {
            PkiConfig::NONE,
            { SaslMechanism::PLAIN },
            false,
            false,
            RpcEncryption::REQUIRED,
          },
          EndpointConfig {
            PkiConfig::SIGNED,
            { SaslMechanism::PLAIN },
            false,
            false,
            RpcEncryption::DISABLED,
          },
          false,
          false,
          Status::NotAuthorized(".*server does not support required TLS encryption"),
          Status::NetworkError(""),
          AuthenticationType::SASL,
          SaslMechanism::PLAIN,
          true,
        },

        // client: GSSAPI, TLS required, externally-signed cert
        // server: GSSAPI, TLS required, externally-signed cert
        NegotiationDescriptor {
          EndpointConfig {
            PkiConfig::EXTERNALLY_SIGNED,
            { SaslMechanism::GSSAPI },
            false,
            false,
            RpcEncryption::REQUIRED,
          },
          EndpointConfig {
            PkiConfig::EXTERNALLY_SIGNED,
            { SaslMechanism::GSSAPI },
            false,
            false,
            RpcEncryption::REQUIRED,
          },
          false,
          false,
          Status::OK(),
          Status::OK(),
          AuthenticationType::SASL,
          SaslMechanism::GSSAPI,
          true,
        },

        // client: GSSAPI, TLS optional, externally-signed cert
        // server: GSSAPI, TLS required, signed cert
        NegotiationDescriptor {
          EndpointConfig {
            PkiConfig::EXTERNALLY_SIGNED,
            { SaslMechanism::GSSAPI },
            false,
            false,
            RpcEncryption::OPTIONAL,
          },
          EndpointConfig {
            PkiConfig::SIGNED,
            { SaslMechanism::GSSAPI },
            false,
            false,
            RpcEncryption::REQUIRED,
          },
          false,
          false,
          Status::OK(),
          Status::OK(),
          AuthenticationType::SASL,
          SaslMechanism::GSSAPI,
          true,
        },

        // client: GSSAPI, TLS required
        // server: GSSAPI, TLS required, externally-signed cert
        NegotiationDescriptor {
          EndpointConfig {
            PkiConfig::NONE,
            { SaslMechanism::GSSAPI },
            false,
            false,
            RpcEncryption::REQUIRED,
          },
          EndpointConfig {
            PkiConfig::EXTERNALLY_SIGNED,
            { SaslMechanism::GSSAPI },
            false,
            false,
            RpcEncryption::REQUIRED,
          },
          false,
          false,
          Status::OK(),
          Status::OK(),
          AuthenticationType::SASL,
          SaslMechanism::GSSAPI,
          true,
        },

        // client: GSSAPI, PLAIN, TLS required, externally-signed cert
        // server: PLAIN, TLS required, externally-signed cert
        NegotiationDescriptor {
          EndpointConfig {
            PkiConfig::EXTERNALLY_SIGNED,
            { SaslMechanism::GSSAPI, SaslMechanism::PLAIN },
            false,
            false,
            RpcEncryption::REQUIRED,
          },
          EndpointConfig {
            PkiConfig::EXTERNALLY_SIGNED,
            { SaslMechanism::PLAIN },
            false,
            false,
            RpcEncryption::REQUIRED,
          },
          false,
          false,
          Status::OK(),
          Status::OK(),
          AuthenticationType::SASL,
          SaslMechanism::PLAIN,
          true,
        },

        // client: GSSAPI, TLS disabled, signed cert
        // server: GSSAPI, TLS required, externally-signed cert
        NegotiationDescriptor {
          EndpointConfig {
            PkiConfig::SIGNED,
            { SaslMechanism::GSSAPI },
            false,
            false,
            RpcEncryption::DISABLED,
          },
          EndpointConfig {
            PkiConfig::EXTERNALLY_SIGNED,
            { SaslMechanism::GSSAPI },
            false,
            false,
            RpcEncryption::REQUIRED,
          },
          false,
          false,
          Status::NotAuthorized(".*client does not support required TLS encryption"),
          Status::NotAuthorized(".*client does not support required TLS encryption"),
          AuthenticationType::SASL,
          SaslMechanism::GSSAPI,
          true,
        },

        // client: GSSAPI, TLS required, signed cert
        // server: GSSAPI, TLS required, externally-signed cert
        NegotiationDescriptor {
          EndpointConfig {
            PkiConfig::SIGNED,
            { SaslMechanism::GSSAPI },
            false,
            false,
            RpcEncryption::REQUIRED,
          },
          EndpointConfig {
            PkiConfig::EXTERNALLY_SIGNED,
            { SaslMechanism::GSSAPI },
            false,
            false,
            RpcEncryption::REQUIRED,
          },
          false,
          false,
          Status::OK(),
          Status::OK(),
          AuthenticationType::SASL,
          SaslMechanism::GSSAPI,
          true,
        },

        // client: PLAIN
        // server: PLAIN
        // connection from public routable IP
        NegotiationDescriptor {
            EndpointConfig {
              PkiConfig::NONE,
              { SaslMechanism::PLAIN },
              false,
              false,
              RpcEncryption::OPTIONAL
            },
            EndpointConfig {
              PkiConfig::NONE,
              { SaslMechanism::PLAIN },
              false,
              false,
              RpcEncryption::OPTIONAL
            },
            true,
            false,
            Status::NotAuthorized(".*unencrypted connections from publicly routable IPs"),
            Status::NotAuthorized(".*unencrypted connections from publicly routable IPs"),
            AuthenticationType::SASL,
            SaslMechanism::PLAIN,
            false,
        },

        // client: GSSAPI, TLS required, externally-signed cert
        // server: GSSAPI, TLS required, externally-signed cert
        // connection from public routable IP
        NegotiationDescriptor {
          EndpointConfig {
            PkiConfig::EXTERNALLY_SIGNED,
            { SaslMechanism::GSSAPI },
            false,
            false,
            RpcEncryption::REQUIRED,
          },
          EndpointConfig {
            PkiConfig::EXTERNALLY_SIGNED,
            { SaslMechanism::GSSAPI },
            false,
            false,
            RpcEncryption::REQUIRED,
          },
          true,
          // true as no longer a loopback connection.
          true,
          Status::OK(),
          Status::OK(),
          AuthenticationType::SASL,
          SaslMechanism::GSSAPI,
          true,
        },


        // client: JWT, TLS required
        // server: JWT, TLS required
        // connecting with JWT only
        NegotiationDescriptor {
          EndpointConfig {
            PkiConfig::EXTERNALLY_SIGNED,
            { },
            false,
            true,
            RpcEncryption::REQUIRED
          },
          EndpointConfig {
            PkiConfig::EXTERNALLY_SIGNED,
            { },
            false,
            true,
            RpcEncryption::REQUIRED
          },
          true,
          true,
          Status::OK(),
          Status::OK(),
          AuthenticationType::JWT,
          SaslMechanism::INVALID,
          true,
        },


        // client: JWT, TLS required, self-signed cert
        // server: JWT, TLS required, self-signed cert
        NegotiationDescriptor {
          EndpointConfig {
            PkiConfig::SELF_SIGNED,
            { },
            false,
            true,
            RpcEncryption::REQUIRED
          },
          EndpointConfig {
            PkiConfig::SELF_SIGNED,
            {  },
            false,
            true,
            RpcEncryption::REQUIRED
          },
          true,
          true,

          // The client isn't sending its JWT to servers whose authenticity
          // it cannot verify, otherwise its authn credentials might be stolen
          // by an impostor. So, even if the client has a JWT handy, it doesn't
          // advertise its JWT authentication capability since the server
          // doesn't have a TLS certificate trusted by the client (the IPKI CA
          // certificate isn't in the client's CA certificate bundle).
          // With that, the server sees no authentication type presented and
          // responds with proper NotAuthorized status code.
          Status::NotAuthorized("client is not configured with an authentication type"),

          Status::NetworkError(""),
          AuthenticationType::JWT,
          SaslMechanism::INVALID,
          true,
        },


        // client: GSSAPI, JWT, TLS required
        // server: JWT, TLS required
        NegotiationDescriptor {
          EndpointConfig {
            PkiConfig::EXTERNALLY_SIGNED,
            { SaslMechanism::GSSAPI },
            false,
            true,
            RpcEncryption::REQUIRED
          },
          EndpointConfig {
            PkiConfig::EXTERNALLY_SIGNED,
            { SaslMechanism::PLAIN },
            false,
            true,
            RpcEncryption::REQUIRED
          },
          true,
          true,
          Status::OK(),
          Status::OK(),
          AuthenticationType::JWT,
          SaslMechanism::INVALID,
          true,
        },


        // client: JWT, TLS disabled
        // server: JWT, TLS required
        NegotiationDescriptor {
          EndpointConfig {
            PkiConfig::EXTERNALLY_SIGNED,
            { },
            false,
            true,
            RpcEncryption::DISABLED
          },
          EndpointConfig {
            PkiConfig::EXTERNALLY_SIGNED,
            { },
            false,
            true,
            RpcEncryption::REQUIRED
          },
          true,
          true,
          Status::NotAuthorized(".*client does not support required TLS encryption"),
          Status::NotAuthorized(""),
          AuthenticationType::JWT,
          SaslMechanism::INVALID,
          true,
        },


        // client: GSSAPI, JWT, TLS required
        // server: JWT, TLS disabled
        NegotiationDescriptor {
          EndpointConfig {
            PkiConfig::EXTERNALLY_SIGNED,
            { SaslMechanism::GSSAPI },
            false,
            true,
            RpcEncryption::REQUIRED
          },
          EndpointConfig {
            PkiConfig::EXTERNALLY_SIGNED,
            { SaslMechanism::PLAIN },
            false,
            true,
            RpcEncryption::DISABLED
          },
          true,
          true,
          Status::NotAuthorized(".*server does not support required TLS encryption"),
          Status::NetworkError(""),
          AuthenticationType::JWT,
          SaslMechanism::INVALID,
          true,
       },


       // client: JWT, TLS required
       // server: JWT, TLS optional
       NegotiationDescriptor {
           EndpointConfig {
               PkiConfig::EXTERNALLY_SIGNED,
               { },
               false,
               true,
               RpcEncryption::REQUIRED
           },
           EndpointConfig {
               PkiConfig::EXTERNALLY_SIGNED,
               { },
               false,
               true,
               RpcEncryption::OPTIONAL
           },
           true,
           true,
           Status::OK(),
           Status::OK(),
           AuthenticationType::JWT,
           SaslMechanism::INVALID,
           true,
       },


       // client: GSSAPI, JWT, TLS optional
       // server: JWT, TLS required
       NegotiationDescriptor {
           EndpointConfig {
               PkiConfig::EXTERNALLY_SIGNED,
               { SaslMechanism::GSSAPI },
               false,
               true,
               RpcEncryption::OPTIONAL
           },
           EndpointConfig {
               PkiConfig::EXTERNALLY_SIGNED,
               { SaslMechanism::PLAIN },
               false,
               true,
               RpcEncryption::REQUIRED
           },
           true,
           true,
           Status::OK(),
           Status::OK(),
           AuthenticationType::JWT,
           SaslMechanism::INVALID,
           true,
       },


       // client: GSSAPI, TLS required
       // server: GSSAPI, JWT, TLS required
       NegotiationDescriptor {
           EndpointConfig {
               PkiConfig::EXTERNALLY_SIGNED,
               { SaslMechanism::GSSAPI },
               false,
               false,
               RpcEncryption::REQUIRED
           },
           EndpointConfig {
               PkiConfig::EXTERNALLY_SIGNED,
               { SaslMechanism::GSSAPI },
               false,
               true,
               RpcEncryption::OPTIONAL
           },
           true,
           true,
           Status::OK(),
           Status::OK(),
               AuthenticationType::SASL,
               SaslMechanism::GSSAPI,
           true,
       },


       // client: GSSAPI, JWT, TLS required
       // server: GSSAPI, TLS required
       NegotiationDescriptor {
           EndpointConfig {
               PkiConfig::EXTERNALLY_SIGNED,
               { SaslMechanism::GSSAPI },
               false,
               true,
               RpcEncryption::REQUIRED
           },
           EndpointConfig {
               PkiConfig::EXTERNALLY_SIGNED,
               { SaslMechanism::GSSAPI },
               false,
               false,
               RpcEncryption::REQUIRED
           },
           true,
           true,
           Status::OK(),
           Status::OK(),
           AuthenticationType::SASL,
           SaslMechanism::GSSAPI,
           true,
       }

));

// A "Callable" that takes a socket for use with starting a thread.
// Can be used for ServerNegotiation or ClientNegotiation threads.
typedef std::function<void(unique_ptr<Socket>)> SocketCallable;

// Call Accept() on the socket, then pass the connection to the server runner
static void RunAcceptingDelegator(Socket* acceptor,
                                  const SocketCallable& server_runner) {
  unique_ptr<Socket> conn(new Socket());
  Sockaddr remote;
  CHECK_OK(acceptor->Accept(conn.get(), &remote, 0));
  server_runner(std::move(conn));
}

// Set up a socket and run a negotiation sequence.
static void RunNegotiationTest(const SocketCallable& server_runner,
                               const SocketCallable& client_runner) {
  Socket server_sock;
  Sockaddr server_bind_addr = Sockaddr::Wildcard();
  CHECK_OK(server_sock.Init(server_bind_addr.family(), 0));
  ASSERT_OK(server_sock.BindAndListen(server_bind_addr, 1));
  ASSERT_OK(server_sock.GetSocketAddress(&server_bind_addr));
  thread server(RunAcceptingDelegator, &server_sock, server_runner);

  unique_ptr<Socket> client_sock(new Socket());
  CHECK_OK(client_sock->Init(server_bind_addr.family(), 0));
  ASSERT_OK(client_sock->Connect(server_bind_addr));
  thread client(client_runner, std::move(client_sock));

  LOG(INFO) << "Waiting for test threads to terminate...";
  client.join();
  LOG(INFO) << "Client thread terminated.";

  server.join();
  LOG(INFO) << "Server thread terminated.";
}

////////////////////////////////////////////////////////////////////////////////

#ifndef __APPLE__

// Run GSSAPI negotiation from the server side. Runs
// 'post_check' after negotiation to verify the result.
static void RunGSSAPINegotiationServer(unique_ptr<Socket> socket,
                                       const std::function<void(const Status&)>& post_check) {
  TlsContext tls_context;
  CHECK_OK(tls_context.Init());
  TokenVerifier token_verifier;
  ServerNegotiation server_negotiation(std::move(socket), &tls_context,
                                       &token_verifier, nullptr, RpcEncryption::OPTIONAL,
                                       /* encrypt_loopback */ false, "kudu");
  server_negotiation.set_server_fqdn("127.0.0.1");
  CHECK_OK(server_negotiation.EnableGSSAPI());
  post_check(server_negotiation.Negotiate());
}

// Run GSSAPI negotiation from the client side. Runs
// 'post_check' after negotiation to verify the result.
static void RunGSSAPINegotiationClient(unique_ptr<Socket> conn,
                                       const std::function<void(const Status&)>& post_check) {
  TlsContext tls_context;
  CHECK_OK(tls_context.Init());
  ClientNegotiation client_negotiation(std::move(conn), &tls_context,
                                       nullopt, nullopt, RpcEncryption::OPTIONAL,
                                       /* encrypt_loopback */ false, "kudu");
  client_negotiation.set_server_fqdn("127.0.0.1");
  CHECK_OK(client_negotiation.EnableGSSAPI());
  post_check(client_negotiation.Negotiate());
}

// Test invalid SASL negotiations using the GSSAPI (kerberos) mechanism over a socket.
// This test is ignored on macOS because the system Kerberos implementation
// (Heimdal) caches the non-existence of client credentials, which causes futher
// tests to fail.
TEST_F(TestNegotiation, TestGSSAPIInvalidNegotiation) {
  MiniKdc kdc;
  ASSERT_OK(kdc.Start());

  // Try to negotiate with no krb5 credentials on either side. It should fail on both
  // sides.
  RunNegotiationTest(
      [](unique_ptr<Socket> socket) {
        RunGSSAPINegotiationServer(
            std::move(socket),
            [](const Status& s) {
              // The client notices there are no credentials and
              // doesn't send any failure message to the server.
              // Instead, it just disconnects.
              //
              // TODO(todd): it might be preferable to have the server
              // fail to start if it has no valid keytab.
              CHECK(s.IsNetworkError());
            });
      },
      [](unique_ptr<Socket> socket) {
        RunGSSAPINegotiationClient(
            std::move(socket),
            [](const Status& s) {
              CHECK(s.IsNotAuthorized());
#ifndef KRB5_VERSION_LE_1_10
              CHECK_GT(s.ToString().find("No Kerberos credentials available"), 0);
#endif
            });
      });

  // Create the server principal and keytab.
  string kt_path;
  ASSERT_OK(kdc.CreateServiceKeytab("kudu/127.0.0.1", &kt_path));
  PCHECK(setenv("KRB5_KTNAME", kt_path.c_str(), 1 /*replace*/) == 0);

  // Try to negotiate with no krb5 credentials on the client. It should fail on both
  // sides.
  RunNegotiationTest(
      [](unique_ptr<Socket> socket) {
        RunGSSAPINegotiationServer(
            std::move(socket),
            [](const Status& s) {
              // The client notices there are no credentials and
              // doesn't send any failure message to the server.
              // Instead, it just disconnects.
              CHECK(s.IsNetworkError());
            });
      },
      [](unique_ptr<Socket> socket) {
        RunGSSAPINegotiationClient(
            std::move(socket),
            [](const Status& s) {
              CHECK(s.IsNotAuthorized());
              ASSERT_STR_MATCHES(s.ToString(),
                                 "Not authorized: server requires authentication, "
                                 "but client does not have Kerberos credentials available");
            });
      });

  // Create and kinit as a client user.
  ASSERT_OK(kdc.CreateUserPrincipal("testuser"));
  ASSERT_OK(kdc.Kinit("testuser"));
  ASSERT_OK(kdc.SetKrb5Environment());

  // Change the server's keytab file so that it has inappropriate
  // credentials.
  // Authentication should now fail.
  ASSERT_OK(kdc.CreateServiceKeytab("otherservice/127.0.0.1", &kt_path));
  PCHECK(setenv("KRB5_KTNAME", kt_path.c_str(), 1 /*replace*/) == 0);

  RunNegotiationTest(
      [](unique_ptr<Socket> socket) {
        RunGSSAPINegotiationServer(
            std::move(socket),
            [](const Status& s) {
              CHECK(s.IsNotAuthorized());
#ifndef KRB5_VERSION_LE_1_10
              ASSERT_STR_CONTAINS(s.ToString(),
                                  "No key table entry found matching kudu/127.0.0.1");
#endif
            });
      },
      [](unique_ptr<Socket> socket) {
        RunGSSAPINegotiationClient(
            std::move(socket),
            [](const Status& s) {
              CHECK(s.IsNotAuthorized());
#ifndef KRB5_VERSION_LE_1_10
              ASSERT_STR_CONTAINS(s.ToString(),
                                  "No key table entry found matching kudu/127.0.0.1");
#endif
            });
      });
}
#endif

#ifndef __APPLE__
// Test that the pre-flight check for servers requiring Kerberos provides
// nice error messages for missing or bad keytabs.
//
// This is ignored on macOS because the system Kerberos implementation does not
// fail the preflight check when the keytab is inaccessible, probably because
// the preflight check passes a 0-length token.
TEST_F(TestNegotiation, TestPreflight) {
  // Try pre-flight with no keytab.
  Status s = ServerNegotiation::PreflightCheckGSSAPI("kudu");
  ASSERT_FALSE(s.ok());
#ifndef KRB5_VERSION_LE_1_10
  ASSERT_STR_MATCHES(s.ToString(), "Key table file.*not found");
#endif
  // Try with a valid krb5 environment and keytab.
  MiniKdc kdc;
  ASSERT_OK(kdc.Start());
  ASSERT_OK(kdc.SetKrb5Environment());
  string kt_path;
  ASSERT_OK(kdc.CreateServiceKeytab("kudu/127.0.0.1", &kt_path));
  PCHECK(setenv("KRB5_KTNAME", kt_path.c_str(), 1 /*replace*/) == 0);

  ASSERT_OK(ServerNegotiation::PreflightCheckGSSAPI("kudu"));

  // Try with an inaccessible keytab.
  CHECK_ERR(chmod(kt_path.c_str(), 0000));
  s = ServerNegotiation::PreflightCheckGSSAPI("kudu");
  if (geteuid() == 0) {
    // The super-user can acess the 'inaccessible' keytab file anyway.
    ASSERT_TRUE(s.ok()) << s.ToString();
  } else {
    ASSERT_FALSE(s.ok()) << s.ToString();
#ifndef KRB5_VERSION_LE_1_10
    ASSERT_STR_MATCHES(s.ToString(), "Permission denied");
#endif
  }
  CHECK_ERR(unlink(kt_path.c_str()));

  // Try with a keytab that has the wrong credentials.
  ASSERT_OK(kdc.CreateServiceKeytab("wrong-service/127.0.0.1", &kt_path));
  PCHECK(setenv("KRB5_KTNAME", kt_path.c_str(), 1 /*replace*/) == 0);
  s = ServerNegotiation::PreflightCheckGSSAPI("kudu");
  ASSERT_FALSE(s.ok());
#ifndef KRB5_VERSION_LE_1_10
  ASSERT_STR_MATCHES(s.ToString(), "No key table entry found matching kudu/.*");
#endif
}
#endif

////////////////////////////////////////////////////////////////////////////////

static void RunTimeoutExpectingServer(unique_ptr<Socket> socket) {
  TlsContext tls_context;
  CHECK_OK(tls_context.Init());
  TokenVerifier token_verifier;
  string jwks_file_name = "keys.jwks";
  string jwt_test_dir = JoinPathSegments(kudu::GetTestDataDirectory(), "jwt");
  string jwt_data = kudu::CreateTestJWT(true);
  ASSERT_OK(kudu::CreateTestJWKSFile(jwt_test_dir, jwks_file_name));
  kudu::KeyBasedJwtVerifier jwt_verifier(JoinPathSegments(jwt_test_dir, jwks_file_name));

  CHECK_OK(jwt_verifier.Init());
  ServerNegotiation server_negotiation(std::move(socket), &tls_context,
                                       &token_verifier, &jwt_verifier, RpcEncryption::OPTIONAL,
                                       /* encrypt_loopback */ false, "kudu");
  CHECK_OK(server_negotiation.EnablePlain());
  Status s = server_negotiation.Negotiate();
  ASSERT_TRUE(s.IsNetworkError()) << "Expected client to time out and close the connection. Got: "
                                  << s.ToString();
}

static void RunTimeoutNegotiationClient(unique_ptr<Socket> sock) {
  TlsContext tls_context;
  CHECK_OK(tls_context.Init());
  ClientNegotiation client_negotiation(std::move(sock), &tls_context,
                                       nullopt, nullopt, RpcEncryption::OPTIONAL,
                                       /* encrypt_loopback */ false, "kudu");
  CHECK_OK(client_negotiation.EnablePlain("test", "test"));
  MonoTime deadline = MonoTime::Now() - MonoDelta::FromMilliseconds(100L);
  client_negotiation.set_deadline(deadline);
  Status s = client_negotiation.Negotiate();
  ASSERT_TRUE(s.IsTimedOut()) << "Expected timeout! Got: " << s.ToString();
  CHECK_OK(client_negotiation.socket()->Shutdown(true, true));
}

// Ensure that the client times out.
TEST_F(TestNegotiation, TestClientTimeout) {
  RunNegotiationTest(RunTimeoutExpectingServer, RunTimeoutNegotiationClient);
}

////////////////////////////////////////////////////////////////////////////////

static void RunTimeoutNegotiationServer(unique_ptr<Socket> socket) {
  TlsContext tls_context;
  CHECK_OK(tls_context.Init());
  TokenVerifier token_verifier;
  string jwks_file_name = "keys.jwks";
  string jwt_test_dir = JoinPathSegments(kudu::GetTestDataDirectory(), "jwt");
  string jwt_data = kudu::CreateTestJWT(true);
  ASSERT_OK(kudu::CreateTestJWKSFile(jwt_test_dir, jwks_file_name));
  kudu::KeyBasedJwtVerifier jwt_verifier(JoinPathSegments(jwt_test_dir, jwks_file_name));

  CHECK_OK(jwt_verifier.Init());
  ServerNegotiation server_negotiation(std::move(socket), &tls_context,
                                       &token_verifier, &jwt_verifier, RpcEncryption::OPTIONAL,
                                       /* encrypt_loopback */ false, "kudu");
  CHECK_OK(server_negotiation.EnablePlain());
  MonoTime deadline = MonoTime::Now() - MonoDelta::FromMilliseconds(100L);
  server_negotiation.set_deadline(deadline);
  Status s = server_negotiation.Negotiate();
  ASSERT_TRUE(s.IsTimedOut()) << "Expected timeout! Got: " << s.ToString();
  CHECK_OK(server_negotiation.socket()->Close());
}

static void RunTimeoutExpectingClient(unique_ptr<Socket> socket) {
  TlsContext tls_context;
  CHECK_OK(tls_context.Init());
  ClientNegotiation client_negotiation(std::move(socket), &tls_context,
                                       nullopt, nullopt, RpcEncryption::OPTIONAL,
                                       /* encrypt_loopback */ false, "kudu");
  CHECK_OK(client_negotiation.EnablePlain("test", "test"));
  Status s = client_negotiation.Negotiate();
  ASSERT_TRUE(s.IsNetworkError()) << "Expected server to time out and close the connection. Got: "
      << s.ToString();
}

// Ensure that the server times out.
TEST_F(TestNegotiation, TestServerTimeout) {
  RunNegotiationTest(RunTimeoutNegotiationServer, RunTimeoutExpectingClient);
}

////////////////////////////////////////////////////////////////////////////////

// This suite of tests ensure that applications that embed the Kudu client are
// able to externally handle the initialization of SASL. See KUDU-1749 and
// IMPALA-4497 for context.
//
// The tests are a bit tricky because the initialization of SASL is static state
// that we can't easily clear/reset between test cases. So, each test invokes
// itself as a subprocess with the appropriate --gtest_filter line as well as a
// special flag to indicate that it is the test child running.
class TestDisableInit : public KuduTest {
 protected:
  // Run the lambda 'f' in a newly-started process, capturing its stderr
  // into 'stderr'.
  template<class TestFunc>
  void DoTest(const TestFunc& f, string* stderr = nullptr) {
    if (FLAGS_is_test_child) {
      f();
      return;
    }

    // Invoke the currently-running test case in a new subprocess.
    string filter_flag = strings::Substitute("--gtest_filter=$0.$1",
                                             CURRENT_TEST_SUITE_NAME(), CURRENT_TEST_NAME());
    string executable_path;
    CHECK_OK(env_->GetExecutablePath(&executable_path));
    string stdout;
    Status s = Subprocess::Call({ executable_path, "test", filter_flag, "--is_test_child" },
                                "" /* stdin */,
                                &stdout,
                                stderr);
    ASSERT_TRUE(s.ok()) << "Test failed: " << stdout;
  }
};

// Test disabling SASL but not actually properly initializing it before usage.
TEST_F(TestDisableInit, TestDisableSasl_NotInitialized) {
  DoTest([]() {
      CHECK_OK(DisableSaslInitialization());
      Status s = SaslInit();
      ASSERT_STR_CONTAINS(s.ToString(), "was disabled, but SASL was not externally initialized");
    });
}

// Test disabling SASL with proper initialization by some other app.
TEST_F(TestDisableInit, TestDisableSasl_Good) {
  DoTest([]() {
      rpc::internal::SaslSetMutex();
      sasl_client_init(NULL);
      CHECK_OK(DisableSaslInitialization());
      ASSERT_OK(SaslInit());
    });
}

// Test a client which inits SASL itself but doesn't remember to disable Kudu's
// SASL initialization.
TEST_F(TestDisableInit, TestMultipleSaslInit) {
  string stderr;
  DoTest([]() {
      rpc::internal::SaslSetMutex();
      sasl_client_init(NULL);
      ASSERT_OK(SaslInit());
    }, &stderr);
  // If we are the parent, we should see the warning from the child that it automatically
  // skipped initialization because it detected that it was already initialized.
  if (!FLAGS_is_test_child) {
    ASSERT_STR_CONTAINS(stderr, "Skipping initialization");
  }
}

// We are not able to detect mutexes not being set with the macOS version of libsasl.
#ifndef __APPLE__
// Test disabling SASL but not remembering to initialize the SASL mutex support. This
// should succeed but generate a warning.
TEST_F(TestDisableInit, TestDisableSasl_NoMutexImpl) {
  string stderr;
  DoTest([]() {
      sasl_client_init(NULL);
      CHECK_OK(DisableSaslInitialization());
      ASSERT_OK(SaslInit());
    }, &stderr);
  // If we are the parent, we should see the warning from the child.
  if (!FLAGS_is_test_child) {
    ASSERT_STR_CONTAINS(stderr, "not provided with a mutex implementation");
  }
}

// Test a client which inits SASL itself but doesn't remember to disable Kudu's
// SASL initialization.
TEST_F(TestDisableInit, TestMultipleSaslInit_NoMutexImpl) {
  string stderr;
  DoTest([]() {
      sasl_client_init(NULL);
      ASSERT_OK(SaslInit());
    }, &stderr);
  // If we are the parent, we should see the warning from the child that it automatically
  // skipped initialization because it detected that it was already initialized.
  if (!FLAGS_is_test_child) {
    ASSERT_STR_CONTAINS(stderr, "Skipping initialization");
    ASSERT_STR_CONTAINS(stderr, "not provided with a mutex implementation");
  }
}
#endif

} // namespace rpc
} // namespace kudu

#if defined(__APPLE__)
#pragma GCC diagnostic pop
#endif // #if defined(__APPLE__)
