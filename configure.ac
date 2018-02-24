#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

# Basic Config needed
AC_PREREQ([2.68])
AC_INIT([sprocket], [0.01], [liao@eng.ucsd.edu])
AM_INIT_AUTOMAKE([foreign])
AM_PATH_PYTHON([2.7])
AC_CONFIG_SRCDIR([sprocket])
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

# Checks for libraries.
PKG_CHECK_MODULES([OpenSSL], [libssl libcrypto])

# Checks for header files.

# Checks for typedefs, structures, and compiler characteristics.
AC_TYPE_UINT16_T

# Checks for library functions.

# Check for Python
AM_CHECK_PYTHON_HEADERS(,[AC_MSG_ERROR(could not find Python headers)])

AC_CONFIG_FILES([
        Makefile
        sprocket/Makefile
        ])

AC_OUTPUT
