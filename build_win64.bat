@rem
@rem Copyright (C) 2020-2022 Dremio Corporation
@rem
@rem See "LICENSE" for license information.
@rem

@REM Please define ARROW_GIT_REPOSITORY to be the arrow repository. If this is a local repo,
@REM use forward slashes instead of backslashes.

@REM Please define VCPKG_ROOT to be the directory with a built vcpkg. This path should use
@REM forward slashes instead of backslashes.

@ECHO OFF

%VCPKG_ROOT%\vcpkg.exe install --triplet x64-windows --x-install-root=%VCPKG_ROOT%/installed

if exist ".\build" del build /q

mkdir build

cd build

if NOT DEFINED ARROW_GIT_REPOSITORY SET ARROW_GIT_REPOSITORY = "https://github.com/apache/arrow"

cmake ..^
    -DARROW_GIT_REPOSITORY=%ARROW_GIT_REPOSITORY%^
    -DCMAKE_TOOLCHAIN_FILE=%VCPKG_ROOT%/scripts/buildsystems/vcpkg.cmake^
    -DVCPKG_TARGET_TRIPLET=x64-windows^
    -DVCPKG_MANIFEST_MODE=OFF^
    -G"Visual Studio 17 2022"^
    -A x64^
    -DCMAKE_BUILD_TYPE=release

cmake --build . --parallel 8 --config Release

cd ..
