#include "exceptions.h"

namespace abstraction_layer {

OdbcException::OdbcException(const std::string &message) : message_(message) {}

const char *OdbcException::what() const _GLIBCXX_TXN_SAFE_DYN _GLIBCXX_NOTHROW {
  return message_.c_str();
}

AuthenticationException::AuthenticationException(const std::string &message)
    : OdbcException(message) {}

} // namespace abstraction_layer
