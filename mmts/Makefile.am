bin_PROGRAMS = mmts
AM_CC = cc
AM_CPPFLAGS = -I../../../include -I/usr/local/ssl/include
AM_LDFLAGS  = ${MYSQL_LIBS} -L/usr/local/ssl/lib   
mmts_SOURCES = \
mmts.c \
mmts-contract.c \
mmts-message.c \
mmts-utils.c \
varset.c

LDADD = ../../../lib/sys/xstring/libxstring.a ../../../lib/sys/messaging/libnpfit.a ../../../lib/sys/mtutil/libmtutil.a -lssl -lcrypto -lz -ldl
