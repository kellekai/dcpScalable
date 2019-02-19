CFLAGS := -g -fPIC
CC := mpiicc
CWD := $(shell pwd)
LDIR := $(CWD)
CLFLAGS := -lcrypto

HEADER := dcp_lib.h dcp_lib_int.h
OBJECTS := dcp_lib.o tools.o

all: libdcp.so

tools.o: tools.c $(HEADER)
	$(CC) -c $(CFLAGS) $< -o $@

dcp_lib.o: dcp_lib.c $(HEADER)
	$(CC) -c $(CFLAGS) $< -o $@

libdcp.so: $(OBJECTS)
	$(CC) -shared -o $@ $(OBJECTS) $(CLFLAGS)
	rm *.o

app: main.c libdcp.so
	$(CC) -L$(LDIR) -Wl,-rpath=$(LDIR) -g -o app.e $< -ldcp

clean:
	rm -rf *.o *.so app.e

.PHONY: clean
