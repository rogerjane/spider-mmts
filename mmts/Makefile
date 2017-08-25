# makefile for mmts/spider

UNAME := $(shell uname)
ifeq ($(UNAME),SCO_SV)
	CC:=CC
	CFLAGS=$(DEBUG) $(INCLUDES) -L$(LIBPATH)
	LIBS=-lssl -lcrypto -lhydra -lz -lm -ldl -lpthread -lsocket
endif

ifeq ($(UNAME),Linux)
	CC:=g++
	CFLAGS=-Wall $(DEBUG) $(INCLUDES) -L$(LIBPATH)
	LIBS=/usr/local/lib/libssl.a /usr/local/lib/libcrypto.a -lhydra -lz -lm -ldl -lpthread
endif

ifeq ($(UNAME),Darwin)
	CC:=g++
	CFLAGS=-Wall $(DEBUG) $(INCLUDES) -L$(LIBPATH)
	LIBS=-lssl -lcrypto -lhydra -lz -lm -ldl -lpthread
endif

ID=mmts

BASE=..

INCPATH=$(BASE)/hydra
LIBPATH=$(BASE)/lib
BINPATH=$(BASE)/bin
SQLITE3=$(BASE)/sqlite3
BIN=$(BINPATH)/$(ID)

INCLUDES=-I. -I$(INCPATH) -I$(SQLITE3)
DEPENDS=$(LIBPATH)/libhydra.a $(SQLITE3)/sqlite3.o

OBJ=mmts.o mmts-contract.o mmts-message.o mmts-utils.o

$(BIN): $(OBJ) $(DEPENDS)
	$(CC) $(OBJ) -o $(BIN) $(CFLAGS) $(SQLITE3)/sqlite3.o $(LIBS)

clean:
	rm -f $(OBJ) *.ti

spotless: clean
	rm -f $(BIN)
