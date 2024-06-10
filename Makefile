default: help

## Install core Kestrel package
kestrel_core:
	cd packages/kestrel_core; pip install .

## Install STIX bundle data source package
kestrel_interface_opensearch: kestrel_core
	cd packages/kestrel_interface_opensearch; pip install .

## Install STIX-Shifter data source package
kestrel_interface_sqlalchemy: kestrel_core
	cd packages/kestrel_interface_sqlalchemy; pip install .

## Install Kestrel kernel for Jupyter
kestrel_jupyter: kestrel_interface_opensearch kestrel_interface_sqlalchemy
	cd packages/kestrel_jupyter; pip install .; kestrel_jupyter_setup

## Install Kestrel kernel for Jupyter
install: kestrel_jupyter

## This help screen
help:
	@printf "Available targets:\n\n"
	@awk '/^[a-zA-Z\-\_0-9%:\\]+/ { \
          helpMessage = match(lastLine, /^## (.*)/); \
          if (helpMessage) { \
            helpCommand = $$1; \
            helpMessage = substr(lastLine, RSTART + 3, RLENGTH); \
      gsub("\\\\", "", helpCommand); \
      gsub(":+$$", "", helpCommand); \
            printf "  \x1b[32;01m%-35s\x1b[0m %s\n", helpCommand, helpMessage; \
          } \
        } \
        { lastLine = $$0 }' $(MAKEFILE_LIST) | sort -u
	@printf "\n"


PKG_DIRS = $(wildcard packages/kestrel_*)

test:
	for d in $(PKG_DIRS); do pytest $$d || break; done
