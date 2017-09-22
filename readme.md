sm - Both Spider and MMTS (with the supporting 'hydra' library)
===============================================================

Everything you need to know *should* be below.

## Directories

* mmts	- The MMTS source code
* spider	- The SPIDER source code
* wamper	- The wamper utility source code
* hydra	- The source of the 'hydra' library
* lib	- Empty directory where libhydra.a is build (also a copy of the 'makeh' script)
* sqlite3 - source of sqlite3, which is referenced within hydra/mtsl3, spider and MMTS.

## Files

* readme.md	- This
* Makefile - A makefile that make everything (sqlite3, hydra, spider, mmts and wamper)
* build - Uses the Makefile to clean, then build from scratch
* release - Uses the Makefile to create either SCO or Linux (depending on what we've checked out to) and store on devsys:/sm

## Notes

The 'hydra' library is referenced directly by the Makefiles within.  However, for use by APIs etc. it will need to exist
in /usr/local/lib and hence will need to be copied there using 'sudo' or equivalent.

## To build
In the following, for a SCO build, use gmake instead of make as the default sco 'make' is brain-dead.

To build everything, sit in the top directory (where this file is) and type:

```bash
$ make               # Make everything
$ make clean         # Remove intermediate object files
$ make spotless      # 'clean' plus binaries, libraries and generated header files
```

Each individual 'thing' can be compiled by sitting in its directory and using 'make' as above.

# IMPORTANT!

Almost all of the .h files within the hydra directory are created using 'makeh' - DO NOT edit them directly.
Instead, update the .cpp file and 'make' will run 'makeh' to create them.  The rules are simple within the .cpp file:

* Prefix a function definition with API to make it appear in the .h (along with immediately succeeding comments)
* If an API function has default parameters, enclose them in /* */ - for example: API func(int a, int b /* =12 */)
* Enclose a block in //START HEADER, //END HEADER to include the block in the .h verbatim

Look in the .cpp for further examples.
