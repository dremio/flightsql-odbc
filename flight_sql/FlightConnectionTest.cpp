#include "gtest/gtest.h"
#include "FlightSqlDriver.h"

TEST(AttributeTests, SetAndGetAttribute) {
  FlightSqlDriver driver;

  const std::shared_ptr<Connection> &connection = driver.CreateConnection();

  connection->SetAttribute(Connection::CONNECTION_TIMEOUT, 200);
  const boost::optional<Connection::Attribute> firstValue = connection->GetAttribute(
    Connection::CONNECTION_TIMEOUT);

  EXPECT_TRUE(firstValue.has_value());

  EXPECT_EQ(boost::get<int>(firstValue.value()), 200);

  connection->SetAttribute(Connection::CONNECTION_TIMEOUT, 300);

  const boost::optional<Connection::Attribute> changeValue = connection->GetAttribute(
    Connection::CONNECTION_TIMEOUT);

  EXPECT_TRUE(changeValue.has_value());
  EXPECT_EQ(boost::get<int>(changeValue.value()), 300);
}

TEST(AttributeTests, GetAttributeWithoutSetting) {
  FlightSqlDriver driver;

  const std::shared_ptr<Connection> &connection = driver.CreateConnection();

  const boost::optional<Connection::Attribute> anOptional = connection->GetAttribute(
    Connection::CONNECTION_TIMEOUT);

  bool b = anOptional.has_value();

  EXPECT_FALSE(b);
}