
# makefile for wampclient

ID=wampclient

BASE=..

INCPATH=$(BASE)/support/include
LIBPATH=$(BASE)/support/lib
BINPATH=$(BASE)/bin
SQLITE3=$(BASE)/sqlite3
MTWAMP=../mtwamp

INCLUDES=-I. -I$(INCPATH) -I$(SQLITE3) -I$(MTWAMP)
LIBS=-lssl -lcrypto -lhydra -lmtwamp -lsocket -lz -lm

CFLAGS=$(INCLUDES)   -I/usr/local/include -L$(LIBPATH) -L$(MTWAMP)/lib -DEHR_UNIXWARE

OBJ= $(ID).o

$(ID): $(OBJ)
	cc $(OBJ) -o $(BINPATH)/$(ID) $(CFLAGS) $(LIBS) $(SQLITE3)/sqlite3.o $(LIBS)

wampwatcher: wampwatcher.o
	cc wampwatcher.o -o $(BINPATH)/wampwatcher $(CFLAGS) $(LIBS) $(SQLITE3)/sqlite3.o $(LIBS)

wamppublish: wamppublish.o
	cc wamppublish.o -o $(BINPATH)/wamppublish $(CFLAGS) $(LIBS) $(SQLITE3)/sqlite3.o $(LIBS)

wampcall: wampcall.o
	cc wampcall.o -o $(BINPATH)/wampcall $(CFLAGS) $(LIBS) $(SQLITE3)/sqlite3.o $(LIBS)

clean:
	rm -f  $(OBJ)

