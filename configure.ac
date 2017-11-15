#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

# Basic Config needed
AC_PREREQ([2.68])
AC_INIT([sprocket], [0.01], [liao@eng.ucsd.edu])
AM_INIT_AUTOMAKE([foreign])
AM_PATH_PYTHON([2.7])
AC_CONFIG_SRCDIR([src/pipeline])
AC_CONFIG_MACRO_DIR([m4])

# Add C++ flags
CXX14_FLAGS="-std=c++14 -pthread"
PICKY_CXXFLAGS="-Wall -Wpedantic -Wextra -Weffc++ -Werror"
AC_SUBST([CXX14_FLAGS])
AC_SUBST([PICKY_CXXFLAGS])

# Checks for programs.
AC_PROG_CXX
AC_PROG_RANLIB
LT_INIT
AC_CHECK_PROG([PROTOC], [protoc], [protoc])
AS_IF([test "x${PROTOC}" == "x"],
          [AC_MSG_ERROR([ProtoBuf compiler "protoc" not found.])])

# Checks for libraries.
PKG_CHECK_MODULES([OpenSSL], [libssl libcrypto])
PKG_CHECK_MODULES(PROTOBUF, protobuf >= 3.4.0)
AC_SUBST(PROTOBUF_LIBS)
AC_SUBST(PROTOBUF_CFLAGS)
AC_SUBST(PROTOBUF_VERSION)

# Checks for header files.

# Checks for typedefs, structures, and compiler characteristics.
AC_TYPE_UINT16_T

# Checks for library functions.

# Check for Python
AM_CHECK_PYTHON_HEADERS(,[AC_MSG_ERROR(could not find Python headers)])

AC_CONFIG_FILES([
        Makefile
        src/Makefile
        src/pipeline/Makefile])

AC_OUTPUT
