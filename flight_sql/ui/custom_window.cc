/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#include <odbcabstraction/platform.h>
#include <Windowsx.h>
#include <commctrl.h>
#include <wtypes.h>
#include <sstream>

#include "ui/custom_window.h"
#include <odbcabstraction/exceptions.h>

namespace driver {
namespace flight_sql {
namespace config {

Result::Type ProcessMessages(Window& window)
{
    MSG msg;

    while (GetMessage(&msg, NULL, 0, 0) > 0)
    {
        if (!IsDialogMessage(window.GetHandle(), &msg))
        {
            TranslateMessage(&msg);

            DispatchMessage(&msg);
        }
    }

    return static_cast<Result::Type>(msg.wParam);
}

LRESULT CALLBACK CustomWindow::WndProc(HWND hwnd, UINT msg, WPARAM wParam, LPARAM lParam)
{
    CustomWindow* window = reinterpret_cast<CustomWindow*>(GetWindowLongPtr(hwnd, GWLP_USERDATA));

    switch (msg)
    {
        case WM_NCCREATE:
        {
            _ASSERT(lParam != NULL);

            CREATESTRUCT* createStruct = reinterpret_cast<CREATESTRUCT*>(lParam);

            LONG_PTR longSelfPtr = reinterpret_cast<LONG_PTR>(createStruct->lpCreateParams);

            SetWindowLongPtr(hwnd, GWLP_USERDATA, longSelfPtr);

            return DefWindowProc(hwnd, msg, wParam, lParam);
        }

        case WM_CREATE:
        {
            _ASSERT(window != NULL);

            window->SetHandle(hwnd);

            window->OnCreate();

            return 0;
        }

        default:
            break;
    }

    if (window && window->OnMessage(msg, wParam, lParam))
        return 0;

    return DefWindowProc(hwnd, msg, wParam, lParam);
}

CustomWindow::CustomWindow(Window* parent, const char* className, const char* title) :
    Window(parent, className, title)
{
    WNDCLASS wcx;

    wcx.style = CS_HREDRAW | CS_VREDRAW;
    wcx.lpfnWndProc = WndProc;
    wcx.cbClsExtra = 0;
    wcx.cbWndExtra = 0;
    wcx.hInstance = GetHInstance();
    wcx.hIcon = NULL;
    wcx.hCursor = LoadCursor(NULL, IDC_ARROW);
    wcx.hbrBackground = (HBRUSH)COLOR_WINDOW;
    wcx.lpszMenuName = NULL;
    wcx.lpszClassName = className;

    if (!RegisterClass(&wcx))
    {
        std::stringstream buf;
        buf << "Can not register window class, error code: " << GetLastError();
        throw odbcabstraction::DriverException(buf.str());
    }
}

CustomWindow::~CustomWindow()
{
    UnregisterClass(className.c_str(), GetHInstance());
}

} // namespace config
} // namespace flight_sql
} // namespace driver
