#include "flight_sql_connection.h"
#include "gtest/gtest.h"
#include <arrow/flight/types.h>

namespace driver {
namespace flight_sql {

using arrow::flight::Location;
using arrow::flight::TimeoutDuration;
using spi::Connection;

TEST(AttributeTests, SetAndGetAttribute) {
  FlightSqlConnection connection(spi::V_3);

  connection.SetAttribute(Connection::CONNECTION_TIMEOUT, 200);
  const boost::optional<Connection::Attribute> firstValue =
      connection.GetAttribute(Connection::CONNECTION_TIMEOUT);

  EXPECT_TRUE(firstValue.has_value());

  EXPECT_EQ(boost::get<int>(firstValue.value()), 200);

  connection.SetAttribute(Connection::CONNECTION_TIMEOUT, 300);

  const boost::optional<Connection::Attribute> changeValue =
      connection.GetAttribute(Connection::CONNECTION_TIMEOUT);

  EXPECT_TRUE(changeValue.has_value());
  EXPECT_EQ(boost::get<int>(changeValue.value()), 300);
}

TEST(AttributeTests, GetAttributeWithoutSetting) {
  FlightSqlConnection connection(spi::V_3);

  const boost::optional<Connection::Attribute> anOptional =
      connection.GetAttribute(Connection::CONNECTION_TIMEOUT);

  EXPECT_FALSE(anOptional.has_value());

  connection.Close();
}

TEST(ConnectTests, GetLocationTcp) {
  const Location &actual_location1 = FlightSqlConnection::GetLocation({
      {Connection::HOST, std::string("localhost")},
      {Connection::PORT, 32010},
  });
  const Location &actual_location2 = FlightSqlConnection::GetLocation({
      {Connection::HOST, std::string("localhost")},
      {Connection::PORT, 32011},
  });

  Location expected_location;
  ASSERT_TRUE(
      Location::ForGrpcTcp("localhost", 32010, &expected_location).ok());
  ASSERT_EQ(expected_location, actual_location1);
  ASSERT_NE(expected_location, actual_location2);

  // TODO: Add tests for SSL
}

TEST(ConnectTests, BuildCallOptions) {
  FlightSqlConnection connection(spi::V_3);

  ASSERT_EQ(arrow::flight::TimeoutDuration{-1.0},
            connection.BuildCallOptions().timeout);

  connection.SetAttribute(Connection::CONNECTION_TIMEOUT, 10.0);
  ASSERT_EQ(TimeoutDuration{10.0}, connection.BuildCallOptions().timeout);
}

} // namespace flight_sql
} // namespace driver
