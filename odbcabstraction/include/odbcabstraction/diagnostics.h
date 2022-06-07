/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

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
      std::string msg_text_;
      std::string sql_state_;
      int32_t native_error_;
    };

  private:
    std::vector<const DiagnosticsRecord*> error_records_;
    std::vector<const DiagnosticsRecord*> warning_records_;
    std::vector<std::unique_ptr<DiagnosticsRecord>> owned_records_;
    std::string vendor_;
    std::string data_source_component_;
    OdbcVersion version_;

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
      warning_records_.push_back(TRUNCATION_WARNING.get());
    }

    inline void TrackRecord(const DiagnosticsRecord& record) {
      if (record.sql_state_[0] == '0' && record.sql_state_[1] == '1') {
        warning_records_.push_back(&record);
      } else {
        error_records_.push_back(&record);
      }
    }

    void SetDataSourceComponent(std::string component);
    std::string GetDataSourceComponent() const;

    std::string GetVendor() const;

    inline void Clear() {
      error_records_.clear();
      warning_records_.clear();
      owned_records_.clear();
    }

    std::string GetMessageText(uint32_t record_index) const;
    std::string GetSQLState(uint32_t record_index) const {
      return GetRecordAtIndex(record_index)->sql_state_;
    }

    int32_t GetNativeError(uint32_t record_index) const {
      return GetRecordAtIndex(record_index)->native_error_;
    }

    inline size_t GetRecordCount() const {
      return error_records_.size() + warning_records_.size();
    }

    inline bool HasRecord(uint32_t record_index) const {
      return error_records_.size() + warning_records_.size() > record_index;
    }

    inline bool HasWarning() const {
      return !warning_records_.empty();
    }

    inline bool HasError() const {
      return !error_records_.empty();
    }

    OdbcVersion GetOdbcVersion() const;

  private:
    inline const DiagnosticsRecord* GetRecordAtIndex(uint32_t record_index) const {
      if (record_index < error_records_.size()) {
        return error_records_[record_index];
      }
      return warning_records_[record_index - error_records_.size()];
    }
  };
}
}
