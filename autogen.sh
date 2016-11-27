#!/bin/sh

exec aclocal
exec autoconf
exec automake --add-missing
exec autoreconf --force --install -I m4
exec autoreconf -fi
