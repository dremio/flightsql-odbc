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

#include <memory>
#include <arrow/flight/client.h>
#include <arrow/type.h>

class RecordBatchTransformer {

private:
//  std::vector<std::function<void(const arrow::RecordBatch &original)>> task;
  std::vector<std::function<std::shared_ptr<arrow::Array>(const std::shared_ptr<arrow::RecordBatch> &originalRecord,
                                 const std::shared_ptr<arrow::Schema> &transformedSchema)>> transformTaskCollection;
  std::vector<std::shared_ptr<arrow::Field>> fields;


public:
  std::shared_ptr<arrow::RecordBatch> transform(
    const std::shared_ptr<arrow::RecordBatch> &original);

public:
  class Builder {
  public:
    friend class RecordBatchTransformer;

    Builder& renameRecord(const std::string& originalName, const std::string& transformedName);

    Builder& addEmptyFields(const std::string& fieldName, const std::shared_ptr<arrow::DataType>& dataType);

    RecordBatchTransformer Build() const;

    Builder(std::shared_ptr<arrow::Schema> schema);

  private:
    std::vector<std::shared_ptr<arrow::Field>> newFields;
    std::vector<std::function<std::shared_ptr<arrow::Array>(const std::shared_ptr<arrow::RecordBatch> &originalRecord,
                                   const std::shared_ptr<arrow::Schema> &transformedSchema)>> taskCollection;
    std::shared_ptr<arrow::Schema> newSchema;
    std::shared_ptr<arrow::Schema> schema;
  };

};


