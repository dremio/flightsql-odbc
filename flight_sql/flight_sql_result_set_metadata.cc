/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#include "flight_sql_result_set_metadata.h"
#include <odbcabstraction/platform.h>
#include <arrow/flight/sql/column_metadata.h>
#include <arrow/util/key_value_metadata.h>
#include "utils.h"

#include <odbcabstraction/types.h>
#include <odbcabstraction/exceptions.h>
#include <utility>

namespace driver {
namespace flight_sql {

using namespace odbcabstraction;
using arrow::DataType;
using arrow::Field;
using arrow::util::make_optional;
using arrow::util::nullopt;

constexpr int32_t DefaultDecimalPrecision = 38;

// This indicates the column length used when the both property StringColumnLength is not specified and
// the server does not provide a length on column metadata.
constexpr int32_t DefaultLengthForVariableLengthColumns = 1024;

namespace {
std::shared_ptr<const arrow::KeyValueMetadata> empty_metadata_map(new arrow::KeyValueMetadata);

inline arrow::flight::sql::ColumnMetadata GetMetadata(const std::shared_ptr<Field> &field) {
  const auto &metadata_map = field->metadata();

  arrow::flight::sql::ColumnMetadata metadata(metadata_map ? metadata_map : empty_metadata_map);
  return metadata;
}

arrow::Result<int32_t> GetFieldPrecision(const std::shared_ptr<Field> &field) {
  return GetMetadata(field).GetPrecision();
}

// TODO: remove using on updating arrow to latest version
arrow::Result<bool> GetMetadataProperty(const arrow::flight::sql::ColumnMetadata &metadata,
                                        const std::string &property_name) {
  std::string result;
  ARROW_ASSIGN_OR_RAISE(result, metadata.metadata_map()->Get(property_name));
  return result == "1";
}
}

size_t FlightSqlResultSetMetadata::GetColumnCount() {
  return schema_->num_fields();
}

std::string FlightSqlResultSetMetadata::GetColumnName(int column_position) {
  return schema_->field(column_position - 1)->name();
}

std::string FlightSqlResultSetMetadata::GetName(int column_position) {
  return schema_->field(column_position - 1)->name();
}

size_t FlightSqlResultSetMetadata::GetPrecision(int column_position) {
  const std::shared_ptr<Field> &field = schema_->field(column_position - 1);

  int32_t column_size = GetFieldPrecision(field).ValueOrElse([] { return 0; });
  SqlDataType data_type_v3 = GetDataTypeFromArrowField_V3(field, metadata_settings_.use_wide_char_);

  return GetColumnSize(data_type_v3, column_size).value_or(0);
}

size_t FlightSqlResultSetMetadata::GetScale(int column_position) {
  const std::shared_ptr<Field> &field = schema_->field(column_position - 1);
  arrow::flight::sql::ColumnMetadata metadata = GetMetadata(field);

  int32_t type_scale = metadata.GetScale().ValueOrElse([] { return 0; });
  SqlDataType data_type_v3 = GetDataTypeFromArrowField_V3(field, metadata_settings_.use_wide_char_);

  return GetTypeScale(data_type_v3, type_scale).value_or(0);
}

uint16_t FlightSqlResultSetMetadata::GetDataType(int column_position) {
  const std::shared_ptr<Field> &field = schema_->field(column_position - 1);
  const SqlDataType conciseType = GetDataTypeFromArrowField_V3(field, metadata_settings_.use_wide_char_);
  return GetNonConciseDataType(conciseType);
}

driver::odbcabstraction::Nullability
FlightSqlResultSetMetadata::IsNullable(int column_position) {
  const std::shared_ptr<Field> &field = schema_->field(column_position - 1);
  return field->nullable() ? odbcabstraction::NULLABILITY_NULLABLE : odbcabstraction::NULLABILITY_NO_NULLS;
}

std::string FlightSqlResultSetMetadata::GetSchemaName(int column_position) {
  arrow::flight::sql::ColumnMetadata metadata = GetMetadata(schema_->field(column_position - 1));

  return metadata.GetSchemaName().ValueOrElse([] { return ""; });
}

std::string FlightSqlResultSetMetadata::GetCatalogName(int column_position) {
  arrow::flight::sql::ColumnMetadata metadata = GetMetadata(schema_->field(column_position - 1));

  return metadata.GetCatalogName().ValueOrElse([] { return ""; });
}

std::string FlightSqlResultSetMetadata::GetTableName(int column_position) {
  arrow::flight::sql::ColumnMetadata metadata = GetMetadata(schema_->field(column_position - 1));

  return metadata.GetTableName().ValueOrElse([] { return ""; });
}

std::string FlightSqlResultSetMetadata::GetColumnLabel(int column_position) {
  return schema_->field(column_position - 1)->name();
}

size_t FlightSqlResultSetMetadata::GetColumnDisplaySize(
    int column_position) {
  const std::shared_ptr<Field> &field = schema_->field(column_position - 1);

  int32_t column_size = metadata_settings_.string_column_length_.value_or(GetFieldPrecision(field).ValueOr(DefaultLengthForVariableLengthColumns));
  SqlDataType data_type_v3 = GetDataTypeFromArrowField_V3(field, metadata_settings_.use_wide_char_);

  return GetDisplaySize(data_type_v3, column_size).value_or(NO_TOTAL);
}

std::string FlightSqlResultSetMetadata::GetBaseColumnName(int column_position) {
  return schema_->field(column_position - 1)->name();
}

std::string FlightSqlResultSetMetadata::GetBaseTableName(int column_position) {
  arrow::flight::sql::ColumnMetadata metadata = GetMetadata(schema_->field(column_position - 1));
  return metadata.GetTableName().ValueOrElse([] { return ""; });
}

uint16_t FlightSqlResultSetMetadata::GetConciseType(int column_position) {
  const std::shared_ptr<Field> &field = schema_->field(column_position -1);

  const SqlDataType sqlColumnType = GetDataTypeFromArrowField_V3(field, metadata_settings_.use_wide_char_);
  return sqlColumnType;
}

size_t FlightSqlResultSetMetadata::GetLength(int column_position) {
  const std::shared_ptr<Field> &field = schema_->field(column_position - 1);

  int32_t column_size = metadata_settings_.string_column_length_.value_or(GetFieldPrecision(field).ValueOr(DefaultLengthForVariableLengthColumns));
  SqlDataType data_type_v3 = GetDataTypeFromArrowField_V3(field, metadata_settings_.use_wide_char_);

  return flight_sql::GetLength(data_type_v3, column_size).value_or(DefaultLengthForVariableLengthColumns);
}

std::string FlightSqlResultSetMetadata::GetLiteralPrefix(int column_position) {
  // TODO: Flight SQL column metadata does not have this, should we add to the spec?
  return "";
}

std::string FlightSqlResultSetMetadata::GetLiteralSuffix(int column_position) {
  // TODO: Flight SQL column metadata does not have this, should we add to the spec?
  return "";
}

std::string FlightSqlResultSetMetadata::GetLocalTypeName(int column_position) {
  arrow::flight::sql::ColumnMetadata metadata = GetMetadata(schema_->field(column_position - 1));

  // TODO: Is local type name the same as type name?
  return metadata.GetTypeName().ValueOrElse([] { return ""; });
}

size_t FlightSqlResultSetMetadata::GetNumPrecRadix(int column_position) {
  const std::shared_ptr<Field> &field = schema_->field(column_position - 1);
  SqlDataType data_type_v3 = GetDataTypeFromArrowField_V3(field, metadata_settings_.use_wide_char_);

  return GetRadixFromSqlDataType(data_type_v3).value_or(NO_TOTAL);
}

size_t FlightSqlResultSetMetadata::GetOctetLength(int column_position) {
  const std::shared_ptr<Field> &field = schema_->field(column_position - 1);
  arrow::flight::sql::ColumnMetadata metadata = GetMetadata(field);

  int32_t column_size = metadata_settings_.string_column_length_.value_or(GetFieldPrecision(field).ValueOr(DefaultLengthForVariableLengthColumns));
  SqlDataType data_type_v3 = GetDataTypeFromArrowField_V3(field, metadata_settings_.use_wide_char_);

  // Workaround to get the precision for Decimal and Numeric types, since server doesn't return it currently.
  // TODO: Use the server precision when its fixed.
  std::shared_ptr<DataType> arrow_type = field->type();
  if (arrow_type->id() == arrow::Type::DECIMAL128){
    int32_t precision = GetDecimalTypePrecision(arrow_type);
    return GetCharOctetLength(data_type_v3, column_size, precision).value_or(DefaultDecimalPrecision+2);
  }

  return GetCharOctetLength(data_type_v3, column_size).value_or(DefaultLengthForVariableLengthColumns);
}

std::string FlightSqlResultSetMetadata::GetTypeName(int column_position) {
  arrow::flight::sql::ColumnMetadata metadata = GetMetadata(schema_->field(column_position - 1));

  return metadata.GetTypeName().ValueOrElse([] { return ""; });
}

driver::odbcabstraction::Updatability
FlightSqlResultSetMetadata::GetUpdatable(int column_position) {
  return odbcabstraction::UPDATABILITY_READWRITE_UNKNOWN;
}

bool FlightSqlResultSetMetadata::IsAutoUnique(int column_position) {
  arrow::flight::sql::ColumnMetadata metadata = GetMetadata(schema_->field(column_position - 1));

  // TODO: Is AutoUnique equivalent to AutoIncrement?
  return metadata.GetIsAutoIncrement().ValueOrElse([] { return false; });
}

bool FlightSqlResultSetMetadata::IsCaseSensitive(int column_position) {
  arrow::flight::sql::ColumnMetadata metadata = GetMetadata(schema_->field(column_position - 1));

  return GetMetadataProperty(metadata, arrow::flight::sql::ColumnMetadata::kIsCaseSensitive).ValueOrElse([] { return false; });
}

driver::odbcabstraction::Searchability
FlightSqlResultSetMetadata::IsSearchable(int column_position) {
  arrow::flight::sql::ColumnMetadata metadata = GetMetadata(schema_->field(column_position - 1));

  bool is_searchable = GetMetadataProperty(metadata, arrow::flight::sql::ColumnMetadata::kIsSearchable).ValueOrElse([] { return false; });
  return is_searchable ? odbcabstraction::SEARCHABILITY_ALL : odbcabstraction::SEARCHABILITY_NONE;
}

bool FlightSqlResultSetMetadata::IsUnsigned(int column_position) {
  const std::shared_ptr<Field> &field = schema_->field(column_position - 1);

  switch (field->type()->id()) {
    case arrow::Type::UINT8:
    case arrow::Type::UINT16:
    case arrow::Type::UINT32:
    case arrow::Type::UINT64:
      return true;
    default:
      return false;
  }
}

bool FlightSqlResultSetMetadata::IsFixedPrecScale(int column_position) {
  // TODO: Flight SQL column metadata does not have this, should we add to the spec?
  return false;
}

FlightSqlResultSetMetadata::FlightSqlResultSetMetadata(
    std::shared_ptr<arrow::Schema> schema,
    const odbcabstraction::MetadataSettings& metadata_settings)
    :
    metadata_settings_(metadata_settings),
    schema_(std::move(schema)) {}

FlightSqlResultSetMetadata::FlightSqlResultSetMetadata(
    const std::shared_ptr<arrow::flight::FlightInfo> &flight_info,
    const odbcabstraction::MetadataSettings& metadata_settings)
    :
    metadata_settings_(metadata_settings){
  arrow::ipc::DictionaryMemo dict_memo;

  ThrowIfNotOK(flight_info->GetSchema(&dict_memo, &schema_));
}

} // namespace flight_sql
} // namespace driver