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

#pragma once

#include <string>
#include <vector>
#include <memory>

#include <odbcabstraction/exceptions.h>
#include <odbcabstraction/types.h>

namespace driver {
namespace odbcabstraction {
  class Diagnostics {
  public:
    struct DiagnosticsRecord {
      std::string message_;
      std::string sql_state_;
      int32_t native_error_;
    };

  private:
    std::vector<const DiagnosticsRecord*> records_;
    std::vector<std::unique_ptr<DiagnosticsRecord>> owned_records_;
    std::string vendor_;
    std::string data_source_component_;
    OdbcVersion version_;
    bool has_warning_;
    bool has_error_;

  public:
    Diagnostics(std::string vendor, std::string data_source_component, OdbcVersion version);
    void AddError(const DriverException& exception);
    void AddWarning(std::string message, std::string sql_state, int32_t native_error);

    /// \brief Add a pre-existing truncation warning.
    inline void AddTruncationWarning() {
      static const std::unique_ptr<DiagnosticsRecord> TRUNCATION_WARNING(new DiagnosticsRecord {
          "String or binary data, right-truncated.", "01004",
          ODBCErrorCodes_TRUNCATION_WARNING
      });
      records_.push_back(TRUNCATION_WARNING.get());
      has_warning_ = true;
    }

    inline void TrackRecord(const DiagnosticsRecord& record) {
      records_.push_back(&record);
      if (record.sql_state_[0] == '0' && record.sql_state_[1] == '1') {
        has_warning_ = true;
      } else {
        has_error_ = true;
      }
    }

    void SetDataSourceComponent(std::string component);
    std::string GetDataSourceComponent() const;

    inline void Clear() {
      records_.clear();
      owned_records_.clear();
      has_warning_ = false;
      has_error_ = false;
    }

    std::string GetMessageText(uint32_t record_index) const;
    std::string GetSQLState(uint32_t record_index) const {
      return records_[record_index]->sql_state_;
    }

    int32_t GetNativeError(uint32_t record_index) const {
      return records_[record_index]->native_error_;
    }

    inline bool HasRecord(uint32_t record_index) const {
      return records_.size() < record_index;
    }

    inline bool HasWarning() const {
      return has_warning_;
    }

    inline bool HasError() const {
      return has_error_;
    }

    OdbcVersion GetOdbcVersion() const;
  };
}
}
