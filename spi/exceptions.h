#include <exception>
#include <string>

#pragma once

namespace driver {
namespace spi {

/// \brief Base for all driver specific exceptions
class DriverException : public std::exception {
public:
  explicit DriverException(std::string message);

  const char *what() const _GLIBCXX_TXN_SAFE_DYN _GLIBCXX_NOTHROW override;

private:
  const std::string message_;
};

/// \brief Authentication specific exception
class AuthenticationException : public DriverException {
public:
  explicit AuthenticationException(std::string message);
};

} // namespace spi
} // namespace driver
