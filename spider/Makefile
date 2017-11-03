
# makefile for mmts/spider

UNAME := $(shell uname)
ifeq ($(UNAME),SCO_SV)
	CC:=CC
	LIBS=-lhydra -lz -lm -ldl -lpthread -lsocket -Bstatic -lssl -lcrypto -Bdynamic
	CFLAGS=$(DEBUG) $(INCLUDES)
	LDFLAGS=$(DEBUG) -L$(LIBPATH)
endif

ifeq ($(UNAME),Linux)
	CC:=g++
	LIBS=/usr/local/lib/libssl.a /usr/local/lib/libcrypto.a -lhydra -lz -lm -ldl -lpthread
	CFLAGS=-Wall $(DEBUG) $(INCLUDES)
	LDFLAGS=$(DEBUG) -L$(LIBPATH)
endif

ifeq ($(UNAME),Darwin)
	CC:=g++
	LIBS=-B,static -lssl -lcrypto -B,dynamic -lhydra -lz -lm -ldl -lpthread
	CFLAGS=-Wall $(DEBUG) $(INCLUDES)
	LDFLAGS=$(DEBUG) -L$(LIBPATH)
endif

ID=spider

BASE=..

INCPATH=$(BASE)/hydra
LIBPATH=$(BASE)/lib
BINPATH=$(BASE)/bin
SQLITE3=$(BASE)/sqlite3
BIN=$(BINPATH)/$(ID)

INCLUDES=-I. -I$(INCPATH) -I$(SQLITE3)
DEPENDS=$(LIBPATH)/libhydra.a $(SQLITE3)/sqlite3.o

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
