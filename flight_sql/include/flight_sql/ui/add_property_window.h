/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#pragma once

#include <odbcabstraction/platform.h>
#include "ui/custom_window.h"

namespace driver {
namespace flight_sql {
namespace config {
/**
    * Add property window class.
    */
class AddPropertyWindow : public CustomWindow
{
    /**
        * Children windows ids.
        */
    struct ChildId
    {
        enum Type
        {
            KEY_EDIT = 100,
            KEY_LABEL,
            VALUE_EDIT,
            VALUE_LABEL,
            OK_BUTTON,
            CANCEL_BUTTON
        };
    };

public:
    /**
        * Constructor.
        *
        * @param parent Parent window handle.
        */
    explicit AddPropertyWindow(Window* parent);

    /**
        * Destructor.
        */
    virtual ~AddPropertyWindow();

    /**
        * Create window in the center of the parent window.
        */
    void Create();

    /**
    * @copedoc ignite::odbc::system::ui::CustomWindow::OnCreate
    */
    virtual void OnCreate() override;

    /**
        * @copedoc ignite::odbc::system::ui::CustomWindow::OnMessage
        */
    virtual bool OnMessage(UINT msg, WPARAM wParam, LPARAM lParam) override;

    /**
        * Get the property from the dialog.
        * 
        * @return true if the dialog was OK'd, false otherwise.
        */
    bool GetProperty(std::string& key, std::string& value);

private:
    /**
        * Create property edit boxes.
        *
        * @param posX X position.
        * @param posY Y position.
        * @param sizeX Width.
        * @return Size by Y.
        */
    int CreateEdits(int posX, int posY, int sizeX);

    void CheckEnableOk();

    std::vector<std::unique_ptr<Window> > labels;

    /** Ok button. */
    std::unique_ptr<Window> okButton;

    /** Cancel button. */
    std::unique_ptr<Window> cancelButton;

    std::unique_ptr<Window> keyEdit;

    std::unique_ptr<Window> valueEdit;

    std::string key;

    std::string value;

    /** Window width. */
    int width;

    /** Window height. */
    int height;

    /** Flag indicating whether OK option was selected. */
    bool accepted;

    bool isInitialized;
};

} // namespace config
} // namespace flight_sql
} // namespace driver
