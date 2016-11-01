#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

# Basic Config needed
AC_PREREQ([2.68])
AC_INIT([pipeline], [0.01], [kvasukib@eng.ucsd.edu])
AM_INIT_AUTOMAKE([foreign])
AM_PATH_PYTHON([2.7])
AC_CONFIG_SRCDIR([src/pipeline])
AC_CONFIG_MACRO_DIR([m4])

# Add picky CXXFLAGS
CXX11_FLAGS="-std=c++11 -pthread"
PICKY_CXXFLAGS="-pedantic -Wall -Wextra -Weffc++ -Werror"
AC_SUBST([CXX11_FLAGS])
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
        src/Makefile])

AC_OUTPUT
