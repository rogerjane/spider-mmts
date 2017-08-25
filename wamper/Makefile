
# makefile for mmts/spider

UNAME := $(shell uname)
ifeq ($(UNAME),SCO_SV)
	CC:=CC
	CFLAGS=$(DEBUG) $(INCLUDES) $(LIBS)
	LIBS=-lhydra -lz -lm -ldl -lpthread -lsocket -Bstatic -lssl -lcrypto -Bdynamic
endif

ifeq ($(UNAME),Linux)
	CC:=g++
	CFLAGS=-Wall $(DEBUG) $(INCLUDES) $(LIBS)
	LIBS=/usr/local/lib/libssl.a /usr/local/lib/libcrypto.a -lhydra -lz -lm -ldl -lpthread
endif

ifeq ($(UNAME),Darwin)
	CC:=g++
	CFLAGS=-Wall $(DEBUG) $(INCLUDES) $(LIBS)
	LIBS=-B,static -lssl -lcrypto -B,dynamic -lhydra -lz -lm -ldl -lpthread
endif

ID=wamper

BASE=..

INCPATH=$(BASE)/hydra
LIBPATH=$(BASE)/lib
BINPATH=$(BASE)/bin
SQLITE3=$(BASE)/sqlite3
BIN=$(BINPATH)/$(ID)

INCLUDES=-I. -I$(INCPATH) -I$(SQLITE3)
DEPENDS=$(LIBPATH)/libhydra.a $(SQLITE3)/sqlite3.o

LDFLAGS=$(DEBUG) -L$(LIBPATH)

OBJ= $(ID).o

all: $(BIN)

.c.o:
	$(CC) -c $(CFLAGS) -o $@ $<

$(BIN): $(OBJ) $(DEPENDS)
	$(CC) $(OBJ) -o $(BIN) $(LDFLAGS) $(SQLITE3)/sqlite3.o $(LIBS)

clean:
	rm -f $(OBJ) *.ti

spotless: clean
	rm -f $(BIN)
