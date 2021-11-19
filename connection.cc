#include "connection.h"

const std::string Connection::HOST = "HOST";
const std::string Connection::PORT = "PORT";
const std::string Connection::USER = "USER";
const std::string Connection::PASSWORD = "PASSWORD";
const std::string Connection::USE_SSL = "USE_SSL";

Connection::Connection(OdbcVersion odbc_version)
    : odbc_version_(odbc_version) {}
