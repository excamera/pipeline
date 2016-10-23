#!/bin/sh

exec autoreconf --force --install -I config -I m4
exec autoreconf -fi
