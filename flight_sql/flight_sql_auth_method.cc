#include "flight_sql_auth_method.h"

#include <arrow/result.h>

#include <utility>

using arrow::Result;
using arrow::flight::FlightClient;

class NoOpAuthMethod : public FlightSqlAuthMethod {
public:
  void Authenticate(FlightCallOptions &call_options) override {
    // Do nothing
  }
};

class UserPasswordAuthMethod : public FlightSqlAuthMethod {
public:
  UserPasswordAuthMethod(FlightClient &client, std::string user,
                         std::string password)
      : client_(client), user_(std::move(user)),
        password_(std::move(password)) {}

  void Authenticate(FlightCallOptions &call_options) override {
    Result<std::pair<std::string, std::string>> bearer_result =
        client_.AuthenticateBasicToken(call_options, user_, password_);
    if (!bearer_result.ok()) {
      throw std::runtime_error(
          "Failed to autenticate with user and password: " +
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
