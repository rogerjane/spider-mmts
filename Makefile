
all:
#	@cd support && $(MAKE)
#	@cd mtwamp && $(MAKE)
	@cd hydra && $(MAKE)
	@cd sqlite3 && $(MAKE)
	@cd spider && $(MAKE)
	@cd mmts && $(MAKE)
	@cd wamper && $(MAKE)

spotless:
	@cd spider && $(MAKE) $@
	@cd mmts && $(MAKE) $@
	@cd hydra && $(MAKE) $@
#	@cd mtwamp && $(MAKE) $@
#	@cd support && $(MAKE) $@
	@cd sqlite3 && $(MAKE) $@
	@cd wamper && $(MAKE) $@

clean:
	@cd spider && $(MAKE) $@
	@cd mmts && $(MAKE) $@
	@cd hydra && $(MAKE) $@
#	@cd mtwamp && $(MAKE) $@
#	@cd support && $(MAKE) $@
	@cd sqlite3 && $(MAKE) $@
	@cd wamper && $(MAKE) $@
