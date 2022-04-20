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

#include "json_converter.h"

#include <arrow/scalar.h>
#include <arrow/builder.h>
#include <arrow/visitor.h>
#include <iostream>
#include <rapidjson/rapidjson.h>
#include <rapidjson/writer.h>
#include "utils.h"
#include <boost/beast/core/detail/base64.hpp>

using namespace arrow;
using namespace boost::beast::detail;
using driver::flight_sql::ThrowIfNotOK;

namespace {
class ScalarToJson : public arrow::ScalarVisitor {
private:
  rapidjson::StringBuffer string_buffer_;
  rapidjson::Writer<rapidjson::StringBuffer> writer_{string_buffer_};

public:
  void Reset() {
    string_buffer_.Clear();
    writer_.Reset(string_buffer_);
  }

  std::string ToString() {
    return string_buffer_.GetString();
  }

  Status Visit(const NullScalar &scalar) override {
    writer_.Null();

    return Status::OK();
  }

  Status Visit(const BooleanScalar &scalar) override {
    writer_.Bool(scalar.value);

    return Status::OK();
  }

  Status Visit(const Int8Scalar &scalar) override {
    writer_.Int(scalar.value);

    return Status::OK();
  }

  Status Visit(const Int16Scalar &scalar) override {
    writer_.Int(scalar.value);

    return Status::OK();
  }

  Status Visit(const Int32Scalar &scalar) override {
    writer_.Int(scalar.value);

    return Status::OK();
  }

  Status Visit(const Int64Scalar &scalar) override {
    writer_.Int64(scalar.value);

    return Status::OK();
  }

  Status Visit(const UInt8Scalar &scalar) override {
    writer_.Uint(scalar.value);

    return Status::OK();
  }

  Status Visit(const UInt16Scalar &scalar) override {
    writer_.Uint(scalar.value);

    return Status::OK();
  }

  Status Visit(const UInt32Scalar &scalar) override {
    writer_.Uint(scalar.value);

    return Status::OK();
  }

  Status Visit(const UInt64Scalar &scalar) override {
    writer_.Uint64(scalar.value);

    return Status::OK();
  }

  Status Visit(const HalfFloatScalar &scalar) override {
    return Status::NotImplemented("Cannot convert HalfFloatScalar to JSON.");
  }

  Status Visit(const FloatScalar &scalar) override {
    writer_.Double(scalar.value);

    return Status::OK();
  }

  Status Visit(const DoubleScalar &scalar) override {
    writer_.Double(scalar.value);

    return Status::OK();
  }

  Status Visit(const StringScalar &scalar) override {
    const auto &view = scalar.view();
    writer_.String(view.data(), view.length());

    return Status::OK();
  }

  Status Visit(const BinaryScalar &scalar) override {
    const auto &view = scalar.view();
    size_t encoded_size = base64::encoded_size(view.length());
    char* encoded = new char[encoded_size];
    base64::encode(encoded, view.data(), view.length());
    writer_.String(encoded, encoded_size);

    return Status::OK();
  }

  Status Visit(const LargeStringScalar &scalar) override {
    const auto &view = scalar.view();
    writer_.String(view.data(), view.length());

    return Status::OK();
  }

  Status Visit(const LargeBinaryScalar &scalar) override {
    const auto &view = scalar.view();
    size_t encoded_size = base64::encoded_size(view.length());
    char* encoded = new char[encoded_size];
    base64::encode(encoded, view.data(), view.length());
    writer_.String(encoded, encoded_size);

    return Status::OK();
  }

  Status Visit(const FixedSizeBinaryScalar &scalar) override {
    const auto &view = scalar.view();
    size_t encoded_size = base64::encoded_size(view.length());
    char* encoded = new char[encoded_size];
    base64::encode(encoded, view.data(), view.length());
    writer_.String(encoded, encoded_size);

    return Status::OK();
  }

  Status Visit(const Date64Scalar &scalar) override {
    ARROW_ASSIGN_OR_RAISE(auto string_scalar, scalar.CastTo(utf8()))
    return string_scalar->Accept(this);
  }

  Status Visit(const Date32Scalar &scalar) override {
    ARROW_ASSIGN_OR_RAISE(auto string_scalar, scalar.CastTo(utf8()))
    return string_scalar->Accept(this);
  }

  Status Visit(const Time32Scalar &scalar) override {
    ARROW_ASSIGN_OR_RAISE(auto string_scalar, scalar.CastTo(utf8()))
    return string_scalar->Accept(this);
  }

  Status Visit(const Time64Scalar &scalar) override {
    ARROW_ASSIGN_OR_RAISE(auto string_scalar, scalar.CastTo(utf8()))
    return string_scalar->Accept(this);
  }

  Status Visit(const TimestampScalar &scalar) override {
    ARROW_ASSIGN_OR_RAISE(auto string_scalar, scalar.CastTo(utf8()))
    return string_scalar->Accept(this);
  }

  Status Visit(const DayTimeIntervalScalar &scalar) override {
    ARROW_ASSIGN_OR_RAISE(auto string_scalar, scalar.CastTo(utf8()))
    return string_scalar->Accept(this);
  }

  Status Visit(const MonthDayNanoIntervalScalar &scalar) override {
    ARROW_ASSIGN_OR_RAISE(auto string_scalar, scalar.CastTo(utf8()))
    return string_scalar->Accept(this);
  }

  Status Visit(const MonthIntervalScalar &scalar) override {
    ARROW_ASSIGN_OR_RAISE(auto string_scalar, scalar.CastTo(utf8()))
    return string_scalar->Accept(this);
  }

  Status Visit(const DurationScalar &scalar) override {
    // TODO: Append TimeUnit on conversion
    ARROW_ASSIGN_OR_RAISE(auto string_scalar, scalar.CastTo(utf8()))
    return string_scalar->Accept(this);
  }

  Status Visit(const Decimal128Scalar &scalar) override {
    return Status::NotImplemented("Cannot convert Decimal128Scalar to JSON.");
  }

  Status Visit(const Decimal256Scalar &scalar) override {
    return Status::NotImplemented("Cannot convert Decimal256Scalar to JSON.");
  }

  Status Visit(const ListScalar &scalar) override {
    writer_.StartArray();
    for (int i = 0; i < scalar.value->length(); ++i) {
      const auto &result = scalar.value->GetScalar(i);
      ThrowIfNotOK(result.status());
      ThrowIfNotOK(result.ValueOrDie()->Accept(this));
    }

    writer_.EndArray();

    return Status::OK();
  }

  Status Visit(const LargeListScalar &scalar) override {
    writer_.StartArray();
    for (int i = 0; i < scalar.value->length(); ++i) {
      const auto &result = scalar.value->GetScalar(i);
      ThrowIfNotOK(result.status());
      ThrowIfNotOK(result.ValueOrDie()->Accept(this));
    }

    writer_.EndArray();

    return Status::OK();
  }

  Status Visit(const MapScalar &scalar) override {
    writer_.StartArray();
    for (int i = 0; i < scalar.value->length(); ++i) {
      const auto &result = scalar.value->GetScalar(i);
      ThrowIfNotOK(result.status());
      ThrowIfNotOK(result.ValueOrDie()->Accept(this));
    }

    writer_.EndArray();

    return Status::OK();
  }

  Status Visit(const FixedSizeListScalar &scalar) override {
    writer_.StartArray();
    for (int i = 0; i < scalar.value->length(); ++i) {
      const auto &result = scalar.value->GetScalar(i);
      ThrowIfNotOK(result.status());
      ThrowIfNotOK(result.ValueOrDie()->Accept(this));
    }

    writer_.EndArray();

    return Status::OK();
  }

  Status Visit(const StructScalar &scalar) override {
    writer_.StartObject();

    const std::shared_ptr<StructType> &data_type = std::static_pointer_cast<StructType>(scalar.type);
    for (int i = 0; i < data_type->num_fields(); ++i) {
      writer_.Key(data_type->field(i)->name().c_str());

      const auto &result = scalar.field(i);
      ThrowIfNotOK(result.status());
      ThrowIfNotOK(result.ValueOrDie()->Accept(this));
    }

    writer_.EndObject();

    return Status::OK();
  }

  Status Visit(const DictionaryScalar &scalar) override {
    return Status::NotImplemented("Cannot convert DictionaryScalar to JSON.");
  }

  Status Visit(const SparseUnionScalar &scalar) override {
    return scalar.value->Accept(this);
  }

  Status Visit(const DenseUnionScalar &scalar) override {
    return scalar.value->Accept(this);
  }

  Status Visit(const ExtensionScalar &scalar) override {
    return Status::NotImplemented("Cannot convert ExtensionScalar to JSON.");
  }
};
}

namespace driver {
namespace flight_sql {

std::string ConvertToJson(const arrow::Scalar &scalar) {
  static thread_local ScalarToJson converter;
  converter.Reset();
  ThrowIfNotOK(scalar.Accept(&converter));

  return converter.ToString();
}

arrow::Result<std::shared_ptr<arrow::Array>> ConvertToJson(const std::shared_ptr<arrow::Array>& input) {
  arrow::StringBuilder builder;
  int64_t length = input->length();
  RETURN_NOT_OK(builder.ReserveData(length));

  for (int i = 0; i < length; ++i) {
    ARROW_ASSIGN_OR_RAISE(auto scalar, input->GetScalar(i))
    RETURN_NOT_OK(builder.Append(ConvertToJson(*scalar)));
  }
  
  return builder.Finish();
}

}
}
