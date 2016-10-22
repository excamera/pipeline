#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

# Basic Config needed
AC_PREREQ([2.68])
AC_INIT([pipeline], [0.01], [kvasukib@eng.ucsd.edu])
AM_INIT_AUTOMAKE([foreign])
AM_PATH_PYTHON([2.7])

# Add picky CXXFLAGS
CXX11_FLAGS="-std=c++11 -pthread"
PICKY_CXXFLAGS="-pedantic -Wall -Wextra -Weffc++ -Werror"
AC_SUBST([CXX11_FLAGS])
AC_SUBST([PICKY_CXXFLAGS])

# Add Python FLAGS

# Checks for programs.
AC_PROG_CXX
AC_PROG_RANLIB

# Checks for libraries.

# Checks for header files.

# Checks for typedefs, structures, and compiler characteristics.
AC_TYPE_UINT16_T

# Checks for library functions.
