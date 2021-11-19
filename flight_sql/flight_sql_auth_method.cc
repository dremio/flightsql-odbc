#include "flight_sql_auth_method.h"
#include "flight_sql_connection.h"

#include <arrow/flight/client.h>
#include <arrow/result.h>

#include <utility>

using arrow::Result;
using arrow::flight::FlightClient;
using arrow::flight::TimeoutDuration;

class NoOpAuthMethod : public FlightSqlAuthMethod {
public:
  void Authenticate(FlightSqlConnection &connection,
                    FlightCallOptions &call_options) override {
    // Do nothing
  }
};

class UserPasswordAuthMethod : public FlightSqlAuthMethod {
public:
  UserPasswordAuthMethod(FlightClient &client, std::string user,
                         std::string password)
      : client_(client), user_(std::move(user)),
        password_(std::move(password)) {}

  void Authenticate(FlightSqlConnection &connection,
                    FlightCallOptions &call_options) override {
    FlightCallOptions auth_call_options;
    const boost::optional<Connection::Attribute> &login_timeout =
        connection.GetAttribute(Connection::LOGIN_TIMEOUT);
    if (login_timeout.has_value()) {
      auth_call_options.timeout =
          TimeoutDuration{boost::get<double>(login_timeout.value())};
    }

    Result<std::pair<std::string, std::string>> bearer_result =
        client_.AuthenticateBasicToken(auth_call_options, user_, password_);
    if (!bearer_result.ok()) {
      throw std::runtime_error(
          "Failed to authenticate with user and password: " +
          bearer_result.status().ToString());
    }

    call_options.headers.push_back(bearer_result.ValueOrDie());
  }

private:
  FlightClient &client_;
  std::string user_;
  std::string password_;
};

std::unique_ptr<FlightSqlAuthMethod> FlightSqlAuthMethod::FromProperties(
    const std::unique_ptr<FlightClient> &client,
    const std::map<std::string, Connection::Property> &properties) {

  // Check if should use user-password authentication
  if (properties.count(Connection::USER) ||
      properties.count(Connection::PASSWORD)) {
    const std::string &user =
        properties.count(Connection::USER)
            ? boost::get<std::string>(properties.at(Connection::USER))
            : "";
    const std::string &password =
        properties.count(Connection::PASSWORD)
            ? boost::get<std::string>(properties.at(Connection::PASSWORD))
            : "";

    return std::unique_ptr<FlightSqlAuthMethod>(
        new UserPasswordAuthMethod(*client, user, password));
  }

  return std::unique_ptr<FlightSqlAuthMethod>(new NoOpAuthMethod);
}
