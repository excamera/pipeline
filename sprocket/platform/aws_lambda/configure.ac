#                                               -*- Autoconf -*-
# Process this file with ./autogen.sh to produce a configure script

AC_PREREQ([2.68])
AC_INIT([mu], [0.01], [keithw@cs.stanford.edu])
AM_INIT_AUTOMAKE([foreign])
AC_CONFIG_SRCDIR([cpp/frontend/hello.cc])
AC_CONFIG_HEADERS([config.h])
AC_CONFIG_MACRO_DIRS([m4])

# Add C++ flags
CXX14_FLAGS="-std=c++14 -pthread"
#PICKY_CXXFLAGS="-Wall -Wpedantic -Wextra -Weffc++ -Werror" # somehow grpc won't compile with -Weffc++
PICKY_CXXFLAGS="-Wall -Wpedantic -Wextra"
AC_SUBST([CXX14_FLAGS])
AC_SUBST([PICKY_CXXFLAGS])

# Checks for programs.
AC_PROG_CXX
LT_INIT
AC_CHECK_PROG([PROTOC], [protoc], [protoc])
AS_IF([test "x${PROTOC}" == "x"],
          [AC_MSG_ERROR([ProtoBuf compiler "protoc" not found.])])

# Checks for libraries.
PKG_CHECK_MODULES([OpenSSL], [libssl libcrypto])
PKG_CHECK_MODULES([PROTOBUF], [protobuf])
PKG_CHECK_MODULES([GRPC], [grpc++])

# Checks for header files.

# Checks for typedefs, structures, and compiler characteristics.

# Checks for library functions.

# Check for Python
AM_PATH_PYTHON(2.7)
AM_CHECK_PYTHON_HEADERS(,[AC_MSG_ERROR(could not find Python headers)])

AC_CONFIG_FILES([
	Makefile
	cpp/Makefile
	cpp/util/Makefile
	cpp/net/Makefile
	cpp/http/Makefile
	cpp/frontend/Makefile
	cpp/lambdareq/Makefile
	cpp/launch/Makefile
	cpp/pylaunch/Makefile
	cpp/tests/Makefile])

AC_OUTPUT
