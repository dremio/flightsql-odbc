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
