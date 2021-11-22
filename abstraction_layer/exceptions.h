#include <exception>
#include <string>

#pragma once

namespace abstraction_layer {

class OdbcException : public std::exception {
public:
  explicit OdbcException(const std::string &message);

  const char *what() const _GLIBCXX_TXN_SAFE_DYN _GLIBCXX_NOTHROW override;

private:
  const std::string &message_;
};

class AuthenticationException : public OdbcException {
public:
  explicit AuthenticationException(const std::string &message);
};

} // namespace abstraction_layer
