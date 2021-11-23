#include "exceptions.h"
#include <utility>

namespace driver {
namespace spi {

DriverException::DriverException(std::string message)
    : message_(std::move(message)) {}

const char *
DriverException::what() const _GLIBCXX_TXN_SAFE_DYN _GLIBCXX_NOTHROW {
  return message_.c_str();
}

AuthenticationException::AuthenticationException(std::string message)
    : DriverException(std::move(message)) {}

} // namespace spi
} // namespace driver
