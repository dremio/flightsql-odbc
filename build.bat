@ECHO OFF

cd ..\flightsql-odbc

C:\Src\vcpkg\vcpkg.exe install --triplet x64-windows

if exist ".\build" del build /q

mkdir build

cd build

cmake ..^
    -DARROW_GIT_REPOSITORY="C:/Users/Alex McRae/source/repos/arrow"^
    -DCMAKE_TOOLCHAIN_FILE=C:/Src/vcpkg/scripts/buildsystems/vcpkg.cmake^
    -DVCPKG_TARGET_TRIPLET=x64-windows^
    -A x64^
    -DCMAKE_BUILD_TYPE=release

cmake --build . --parallel 8 --config Release

cmake --install .