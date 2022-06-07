/*
 * Copyright (C) 2020-2022 Dremio Corporation
 *
 * See "LICENSE" for license information.
 */

#pragma once

#include <string>
#include <memory>
#include <vector>

namespace driver {
namespace flight_sql {
namespace config {

/**
    * Get handle for the current module.
    *
    * @return Handle for the current module.
    */
HINSTANCE GetHInstance();

/**
    * Window class.
    */
class Window
{
public:
    /**
        * Constructor for a new window that is going to be created.
        *
        * @param parent Parent window handle.
        * @param className Window class name.
        * @param title Window title.
        * @param callback Event processing function.
        */
    Window(Window* parent, const char* className, const char* title);

    /**
        * Constructor for the existing window.
        *
        * @param handle Window handle.
        */
    explicit Window(HWND handle);

    /**
        * Destructor.
        */
    virtual ~Window();

    /**
        * Create window.
        *
        * @param style Window style.
        * @param posX Window x position.
        * @param posY Window y position.
        * @param width Window width.
        * @param height Window height.
        * @param id ID for child window.
        */
    void Create(DWORD style, int posX, int posY, int width, int height, int id);

    /**
        * Create child tab controlwindow.
        *
        * @param id ID to be assigned to the created window.
        * @return Auto pointer containing new window.
        */
    std::unique_ptr<Window> CreateTabControl(int id);

    /**
        * Create child list view window.
        *
        * @param posX Position by X coordinate.
        * @param posY Position by Y coordinate.
        * @param sizeX Size by X coordinate.
        * @param sizeY Size by Y coordinate.
        * @param id ID to be assigned to the created window.
        * @return Auto pointer containing new window.
        */
    std::unique_ptr<Window> CreateList(int posX, int posY,
        int sizeX, int sizeY, int id);

    /**
        * Create child group box window.
        *
        * @param posX Position by X coordinate.
        * @param posY Position by Y coordinate.
        * @param sizeX Size by X coordinate.
        * @param sizeY Size by Y coordinate.
        * @param title Title.
        * @param id ID to be assigned to the created window.
        * @return Auto pointer containing new window.
        */
    std::unique_ptr<Window> CreateGroupBox(int posX, int posY,
        int sizeX, int sizeY, const char* title, int id);

    /**
        * Create child label window.
        *
        * @param posX Position by X coordinate.
        * @param posY Position by Y coordinate.
        * @param sizeX Size by X coordinate.
        * @param sizeY Size by Y coordinate.
        * @param title Title.
        * @param id ID to be assigned to the created window.
        * @return Auto pointer containing new window.
        */
    std::unique_ptr<Window> CreateLabel(int posX, int posY,
        int sizeX, int sizeY, const char* title, int id);

    /**
        * Create child Edit window.
        *
        * @param posX Position by X coordinate.
        * @param posY Position by Y coordinate.
        * @param sizeX Size by X coordinate.
        * @param sizeY Size by Y coordinate.
        * @param title Title.
        * @param id ID to be assigned to the created window.
        * @return Auto pointer containing new window.
        */
    std::unique_ptr<Window> CreateEdit(int posX, int posY,
        int sizeX, int sizeY, const char* title, int id, int style = 0);

    /**
        * Create child button window.
        *
        * @param posX Position by X coordinate.
        * @param posY Position by Y coordinate.
        * @param sizeX Size by X coordinate.
        * @param sizeY Size by Y coordinate.
        * @param title Title.
        * @param id ID to be assigned to the created window.
        * @return Auto pointer containing new window.
        */
    std::unique_ptr<Window> CreateButton(int posX, int posY,
        int sizeX, int sizeY, const char* title, int id, int style = 0);

    /**
        * Create child CheckBox window.
        *
        * @param posX Position by X coordinate.
        * @param posY Position by Y coordinate.
        * @param sizeX Size by X coordinate.
        * @param sizeY Size by Y coordinate.
        * @param title Title.
        * @param id ID to be assigned to the created window.
        * @return Auto pointer containing new window.
        */
    std::unique_ptr<Window> CreateCheckBox(int posX, int posY,
        int sizeX, int sizeY, const char* title, int id, bool state);

    /**
        * Create child ComboBox window.
        *
        * @param posX Position by X coordinate.
        * @param posY Position by Y coordinate.
        * @param sizeX Size by X coordinate.
        * @param sizeY Size by Y coordinate.
        * @param title Title.
        * @param id ID to be assigned to the created window.
        * @return Auto pointer containing new window.
        */
    std::unique_ptr<Window> CreateComboBox(int posX, int posY,
        int sizeX, int sizeY, const char* title, int id);

    /**
        * Show window.
        */
    void Show();

    /**
        * Update window.
        */
    void Update();

    /**
        * Destroy window.
        */
    void Destroy();

    /**
        * Get window handle.
        *
        * @return Window handle.
        */
    HWND GetHandle() const
    {
        return handle;
    }

    void SetVisible(bool isVisible);

    void ListAddColumn(const std::string& name, int index, int width);

    void ListAddItem(const std::vector<std::string >& items);

    void ListDeleteSelectedItem();

    std::vector<std::vector<std::string> > ListGetAll();

    void AddTab(const std::string& name, int index);

    bool IsTextEmpty() const;

    /**
        * Get window text.
        *
        * @param text Text.
        */
    void GetText(std::string& text) const;

    /**
        * Set window text.
        *
        * @param text Text.
        */
    void SetText(const std::string& text) const;

    /**
        * Get CheckBox state.
        *
        * @param True if checked.
        */
    bool IsChecked() const;

    /**
        * Set CheckBox state.
        *
        * @param state True if checked.
        */
    void SetChecked(bool state);

    /**
        * Add string.
        *
        * @param str String.
        */
    void AddString(const std::string& str);

    /**
        * Set current ComboBox selection.
        *
        * @param idx List index.
        */
    void SetSelection(int idx);

    /**
        * Get current ComboBox selection.
        *
        * @return idx List index.
        */
    int GetSelection() const;

    /**
        * Set enabled.
        *
        * @param enabled Enable flag.
        */
    void SetEnabled(bool enabled);

    /**
        * Check if the window is enabled.
        *
        * @return True if enabled.
        */
    bool IsEnabled() const;

protected:
    /**
        * Set window handle.
        *
        * @param value Window handle.
        */
    void SetHandle(HWND value)
    {
        handle = value;
    }

    /** Window class name. */
    std::string className;

    /** Window title. */
    std::string title;

    /** Window handle. */
    HWND handle;

    /** Window parent. */
    Window* parent;

    /** Specifies whether window has been created by the thread and needs destruction. */
    bool created;

private:
    Window(const Window& window) = delete;
};

} // namespace config
} // namespace flight_sql
} // namespace driver
