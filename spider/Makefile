
# makefile for mmts/spider

ID=spider

BASE=..

INCPATH=$(BASE)/support/include
LIBPATH=$(BASE)/support/lib
BINPATH=$(BASE)/bin
SQLITE3=$(BASE)/sqlite3
MTWAMP=../mtwamp
BIN=$(BINPATH)/$(ID)

INCLUDES=-I. -I$(INCPATH) -I$(SQLITE3) -I$(MTWAMP)
LIBS=-lssl -lcrypto -lhydra -lmtwamp -lz -lm -ldl

CFLAGS=-g $(INCLUDES)
LDFLAGS=-g -L$(LIBPATH) -L$(MTWAMP)/lib

OBJ= $(ID).o

.c.o:
	gcc -Wall -c $(CFLAGS) -o $@ $<

$(BIN): $(OBJ) ../mtwamp/lib/libmtwamp.a ../support/lib/libhydra.a
	gcc $(OBJ) -o $(BIN) $(LDFLAGS) $(SQLITE3)/sqlite3.o $(LIBS)

clean:
	rm -f  $(OBJ)

