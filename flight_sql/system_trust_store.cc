/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#include "system_trust_store.h"


#if defined _WIN32 || defined _WIN64

namespace driver {
namespace flight_sql {
  bool SystemTrustStore::HasNext() {
    p_context_ = CertEnumCertificatesInStore(h_store_, p_context_);

    return p_context_ != nullptr;
  }

  std::string SystemTrustStore::GetNext() const {
    DWORD size = 0;
    CryptBinaryToString(p_context_->pbCertEncoded, p_context_->cbCertEncoded,
                        CRYPT_STRING_BASE64HEADER, nullptr, &size);

    std::string cert;
    cert.resize(size);
    CryptBinaryToString(p_context_->pbCertEncoded,
                        p_context_->cbCertEncoded, CRYPT_STRING_BASE64HEADER,
                        &cert[0], &size);
    cert.resize(size);

    return cert;
  }

  bool SystemTrustStore::SystemHasStore()	{
    return h_store_ != nullptr;
  }

  SystemTrustStore::SystemTrustStore(const char* store) : stores_(store),
    h_store_(CertOpenSystemStore(NULL, store)), p_context_(nullptr) {}

  SystemTrustStore::~SystemTrustStore() {
    if (p_context_) {
      CertFreeCertificateContext(p_context_);
    }
    if (h_store_) {
      CertCloseStore(h_store_, 0);
    }
  }
} // namespace flight_sql
} // namespace driver

#endif
