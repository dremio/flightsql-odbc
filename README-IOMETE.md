## IOMETE Arrow ODBC SPI

### Does this package buildable?
Yes, it is buildable, at least on my machine. I am using CLion IDE and CMake build system.

### You need to install vcpkg
 https://github.com/microsoft/vcpkg - This is a package manager for C++ libraries. 
 You can install it on your machine and install the required libraries. 
 We will be using CMake Toolchain file to find the libraries installed by vcpkg.

### You need to install odbc dependencies (libiodbc) for your OS

* Right now this package works on macOS arm, but libiodbc should be installed as MacOS intel.
* The libiodbc should be installed by brew, you need to run terminal as intel arch and install libiodbc.

### Setup on CLion
You need to use installed vcpkg and set installed packages to it, as if you will clean and rebuild the project, 
 you will have to install all again (which is time-consuming)

Here are the parameters you need to specify for the CLion CMake configuration:

1. Enable vcpkg integration in CLion
2. Add those parameters to CMake configuration:
```shell
-DCMAKE_TOOLCHAIN_FILE=[PATH_TO_VCPKG]/scripts/buildsystems/vcpkg.cmake \
-DVCPKG_INSTALLED_DIR=[PATH_TO_VCPKG]/installed \
-DCMAKE_VERBOSE_MAKEFILE=ON \
-DVCPKG_TARGET_TRIPLET=arm64-osx
```

### How to use the Arrow ODBC SPI CLI (kind of integration test)
```shell
cd [PACKAGE_ROOT_PATH]/cmake-build-debug/Debug/bin

./arrow_odbc_spi_impl_cli \
--host dev.iomete.cloud \
--port 443 \
--user admin \
--password <token> -\
-data-plane spark-resources \
--cluster arrow
```

You need to have an output like this:
```shell
1
2
3
4
5
6
```