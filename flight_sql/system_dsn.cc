/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#include <odbcabstraction/platform.h>
#include <winuser.h>
#include "flight_sql_connection.h"
#include "config/configuration.h"
#include "config/connection_string_parser.h"
#include "ui/window.h"
#include "ui/dsn_configuration_window.h"
#include <odbcabstraction/exceptions.h>
#include <odbcabstraction/platform.h>
#include <utility>

#include <odbcinst.h>
#include <sstream>
#include <locale>
#include <codecvt>

using namespace std;
using namespace driver::flight_sql;
using namespace driver::flight_sql::config;

BOOL CALLBACK ConfigDriver(
    HWND    hwndParent,
    WORD    fRequest,
    LPCSTR  lpszDriver,
    LPCSTR  lpszArgs,
    LPSTR   lpszMsg,
    WORD    cbMsgMax,
    WORD* pcbMsgOut) {
    return false;
}

bool DisplayConnectionWindow(void* windowParent, Configuration& config)
{
    HWND hwndParent = (HWND)windowParent;

    if (!hwndParent)
        return true;

    try
    {
        Window parent(hwndParent);
        DsnConfigurationWindow window(&parent, config);

        window.Create();

        window.Show();
        window.Update();

        return ProcessMessages(window) == Result::OK;
   }
    catch (driver::odbcabstraction::DriverException& err)
    {
        std::stringstream buf;
        buf << "Message: " << err.GetMessageText() << ", Code: " << err.GetNativeError();
        std::string message = buf.str();
        MessageBox(NULL, message.c_str(), "Error!", MB_ICONEXCLAMATION | MB_OK);

        SQLPostInstallerError(err.GetNativeError(), err.GetMessageText().c_str());
    }

    return false;
}

void PostLastInstallerError() {

    #define BUFFER_SIZE (1024)
    DWORD code;
    char msg[BUFFER_SIZE];
    SQLInstallerError(1, &code, msg, BUFFER_SIZE, NULL);

    std::stringstream buf;
    buf << "Message: \"" << msg << "\", Code: " << code;
    std::string errorMsg = buf.str();

    MessageBox(NULL, errorMsg.c_str(), "Error!", MB_ICONEXCLAMATION | MB_OK);
    SQLPostInstallerError(code, errorMsg.c_str());
}

/**
 * Unregister specified DSN.
 *
 * @param dsn DSN name.
 * @return True on success and false on fail.
 */
bool UnregisterDsn(const std::string& dsn)
{
    if (SQLRemoveDSNFromIni(dsn.c_str())) {
        return true;
    }

    PostLastInstallerError();
    return false;
}

/**
 * Register DSN with specified configuration.
 *
 * @param config Configuration.
 * @param driver Driver.
 * @return True on success and false on fail.
 */
bool RegisterDsn(const Configuration& config, LPCSTR driver)
{
    const std::string& dsn = config.Get(FlightSqlConnection::DSN);

    if (!SQLWriteDSNToIni(dsn.c_str(), driver))
    {
        PostLastInstallerError();
        return false;
    }

    const auto& map = config.GetProperties();
    for (auto it = map.begin(); it != map.end(); ++it)
    {
        const std::string& key = it->first;
        if (boost::iequals(FlightSqlConnection::DSN, key) || boost::iequals(FlightSqlConnection::DRIVER, key)) {
            continue;
        }

        if (!SQLWritePrivateProfileString(dsn.c_str(), key.c_str(), it->second.c_str(), "ODBC.INI")) {
            PostLastInstallerError();
            return false;
        }
    }

    return true;
}

BOOL INSTAPI ConfigDSN(HWND hwndParent, WORD req, LPCSTR driver, LPCSTR attributes)
{
    Configuration config;
    ConnectionStringParser parser(config);
    parser.ParseConfigAttributes(attributes);

    switch (req)
    {
        case ODBC_ADD_DSN:
        {
            config.LoadDefaults();
            if (!DisplayConnectionWindow(hwndParent, config) || !RegisterDsn(config, driver))
                return FALSE;

            break;
        }

        case ODBC_CONFIG_DSN:
        {
            const std::string& dsn = config.Get(FlightSqlConnection::DSN);
            if (!SQLValidDSN(dsn.c_str()))
                return FALSE;

            Configuration loaded(config);
            loaded.LoadDsn(dsn);

            if (!DisplayConnectionWindow(hwndParent, loaded) || !UnregisterDsn(dsn.c_str()) || !RegisterDsn(loaded, driver))
                return FALSE;

            break;
        }

        case ODBC_REMOVE_DSN:
        {
            const std::string& dsn = config.Get(FlightSqlConnection::DSN);
            if (!SQLValidDSN(dsn.c_str()) || !UnregisterDsn(dsn))
                return FALSE;

            break;
        }

        default:
            return FALSE;
    }

    return TRUE;
}
