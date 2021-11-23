#include <exception>
#include <string>

#pragma once

namespace driver {
namespace spi {

class OdbcException : public std::exception {
public:
  explicit OdbcException(std::string message);

  const char *what() const _GLIBCXX_TXN_SAFE_DYN _GLIBCXX_NOTHROW override;

private:
  const std::string message_;
};

class AuthenticationException : public OdbcException {
public:
  explicit AuthenticationException(std::string message);
};

} // namespace spi
} // namespace driver
