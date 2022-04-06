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

#include <odbcabstraction/exceptions.h>
#include <odbcabstraction/platform.h> 
#include <utility>

namespace driver {
namespace odbcabstraction {

DriverException::DriverException(std::string message)
    : message_(std::move(message)) {}

const char *DriverException::what() const throw() { return message_.c_str(); }

AuthenticationException::AuthenticationException(std::string message)
    : DriverException(std::move(message)) {}

} // namespace odbcabstraction
} // namespace driver
