#include "windows.h"
#include "odbcinst.h"
#include "winuser.h"
#include <flight_sql/config/configuration.h>
#include "flight_sql/config/connection_string_parser.h"
#include "flight_sql/ui/window.h"
#include "flight_sql/ui/dsn_configuration_window.h"
#include "flight_sql/common/odbc_constants.h"
//#include "flight_sql/config/diagnosable_adapter.h"
// TODO: Incorrect referencing (should be "config/configuration.h")


using ignite::odbc::config::Configuration;
using namespace std;

int DisplayResourceNAMessageBox()
{
    int msgboxID = MessageBoxW(
        NULL,
        (LPCWSTR)L"Resource not available here\nDo you try again?",
        (LPCWSTR)L"Account Details",
        MB_ICONWARNING | MB_CANCELTRYCONTINUE | MB_DEFBUTTON2
    );

    switch (msgboxID)
    {
    case IDCANCEL:
        // TODO: add code
        break;
    case IDTRYAGAIN:
        // TODO: add code
        break;
    case IDCONTINUE:
        // TODO: add code
        break;
    }

    return msgboxID;
}

bool DisplayConnectionWindow(void* windowParent, Configuration& config)
{
    using namespace ignite::odbc::system::ui;

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
        DisplayResourceNAMessageBox();
   }
    catch (int err)
    {
    //  /*  std::stringstream buf;

    //    buf << "Message: " << err.GetText() << ", Code: " << err.GetCode();

    //    std::string message = buf.str();

    //    MessageBox(NULL, message.c_str(), "Error!", MB_ICONEXCLAMATION | MB_OK);*/

    //    SQLPostInstallerError(err.GetCode(), err.GetText());
    }

    return false;
}

/**
 * Register DSN with specified configuration.
 *
 * @param config Configuration.
 * @param driver Driver.
 * @return True on success and false on fail.
 */
bool RegisterDsn(const Configuration& config, LPCWSTR driver)
{
    using namespace ignite::odbc::config;

    typedef Configuration::ArgumentMap ArgMap;

    const char* dsn = config.GetDsn().c_str();

    try
    {
        if (!SQLWriteDSNToIniW((LPCWSTR)dsn, driver))
        {
            throw;
        }
        ArgMap map;

        config.ToMap(map);

        map.erase(ConnectionStringParser::Key::dsn);
        map.erase(ConnectionStringParser::Key::driver);



        for (ArgMap::const_iterator it = map.begin(); it != map.end(); ++it)
        {
            const std::string& key = it->first;
            const std::string& value = it->second;
            SQLWritePrivateProfileStringW((LPCWSTR)dsn, (LPCWSTR)key.c_str(), (LPCWSTR)value.c_str(), (LPCWSTR)"ODBC.INI");
            //ignite::odbc::WriteDsnString(dsn, key.c_str(), value.c_str());
        }

        return true;
    }
    catch (int err)
    {
        MessageBox(NULL, NULL, "Error!", MB_ICONEXCLAMATION | MB_OK);

        //  SQLPostInstallerError(err.GetCode(), err.GetText());
    }

    return false;
}

BOOL INSTAPI ConfigDSN(HWND hwndParent, WORD req, LPCSTR driver, LPCSTR attributes)
{
    DisplayResourceNAMessageBox();
    using namespace ignite::odbc;
    Configuration config;


    config::ConnectionStringParser parser(config);


    diagnostic::DiagnosticRecordStorage diag;

    parser.ParseConfigAttributes((const char*)attributes, &diag);

    if (!SQLValidDSN(config.GetDsn().c_str()))
        return FALSE;

    switch (req)
    {
    case ODBC_ADD_DSN:
    {

        if (!DisplayConnectionWindow(hwndParent, config))
            return FALSE;
        break;
    }

    SQLWritePrivateProfileString((LPCSTR)"myNewDSN", (LPCSTR)"Driver", (LPCSTR)"JoyDriver", (LPCSTR)"ODBC.INI");

    break;
    // }
         /*
     ////////////////////////

     //case ODBC_CONFIG_DSN:
     //{
     //    std::string dsn = config.GetDsn();

     //    Configuration loaded(config);

     //    ReadDsnConfiguration(dsn.c_str(), loaded, &diag);

     //    if (!DisplayConnectionWindow(hwndParent, loaded))
     //        return FALSE;

     //    if (!RegisterDsn(loaded, driver))
     //        return FALSE;

     //    if (loaded.GetDsn() != dsn && !UnregisterDsn(dsn.c_str()))
     //        return FALSE;

     //    break;
     //}

     //case ODBC_REMOVE_DSN:
     //{

     //    if (!UnregisterDsn(config.GetDsn().c_str()))
     //        return FALSE;

     //    break;
     //}*/

    default:
        return FALSE;
    }
}


BOOL INSTAPI ConfigDSNW(HWND hwndParent, WORD req, LPCWSTR driver, LPCWSTR attributes)
{
    DisplayResourceNAMessageBox();
    using namespace ignite::odbc;
    Configuration config;


    config::ConnectionStringParser parser(config);

    
    diagnostic::DiagnosticRecordStorage diag;
    
    parser.ParseConfigAttributes((const char*)attributes, &diag);
    
    if (!SQLValidDSN(config.GetDsn().c_str()))
        return FALSE;
 
    switch (req)
    {
    case ODBC_ADD_DSN:
    {

        if (!DisplayConnectionWindow(hwndParent, config))
            return FALSE;
        break;
    }
    
       if (!RegisterDsn(config, driver))
            return FALSE;

        break;
   // }
        /*
    ////////////////////////

    //case ODBC_CONFIG_DSN:
    //{
    //    std::string dsn = config.GetDsn();

    //    Configuration loaded(config);

    //    ReadDsnConfiguration(dsn.c_str(), loaded, &diag);

    //    if (!DisplayConnectionWindow(hwndParent, loaded))
    //        return FALSE;

    //    if (!RegisterDsn(loaded, driver))
    //        return FALSE;

    //    if (loaded.GetDsn() != dsn && !UnregisterDsn(dsn.c_str()))
    //        return FALSE;

    //    break;
    //}

    //case ODBC_REMOVE_DSN:
    //{

    //    if (!UnregisterDsn(config.GetDsn().c_str()))
    //        return FALSE;

    //    break;
    //}*/

   default:
        return FALSE;
        }
    

    return TRUE;
}



