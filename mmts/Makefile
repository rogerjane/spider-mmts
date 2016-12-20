
# makefile for mmts/spider

ID=mmts

BASE=..

INCPATH=$(BASE)/support/include
LIBPATH=$(BASE)/support/lib
BINPATH=$(BASE)/bin
SQLITE3=$(BASE)/sqlite3
MTWAMP=../mtwamp

INCLUDES=-I. -I$(INCPATH) -I$(SQLITE3) -I$(MTWAMP)
LIBS=-lssl -lcrypto -lhydra -lmtwamp -lz -lm -ldl

CFLAGS=$(INCLUDES)   -I/usr/local/include -L$(LIBPATH) -L$(MTWAMP)/lib -DEHR_UNIXWARE

OBJ= $(ID).o mmts-contract.o mmts-message.o mmts-utils.o

$(ID): $(OBJ)
	cc $(OBJ) -o $(BINPATH)/$(ID) $(CFLAGS) $(SQLITE3)/sqlite3.o $(LIBS)

clean:
	rm -f  $(OBJ)

