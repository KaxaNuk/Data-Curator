# Makefile for Sphinx on Linux/Unix

# Configurable variables
SPHINXBUILD   = sphinx-build
SOURCEDIR     = source
BUILDDIR      = _build

# Default target: display help
help:
	@$(SPHINXBUILD) -M help "$(SOURCEDIR)" "$(BUILDDIR)"

.PHONY: help clean html latexpdf epub

# Clean up build files
clean:
	@echo "Cleaning build directory..."
	rm -rf "$(BUILDDIR)"/*
	@echo "Build directory cleaned."

# Generate HTML documentation
html:
	@echo "Building HTML documentation..."
	$(SPHINXBUILD) -M html "$(SOURCEDIR)" "$(BUILDDIR)"
	@echo "HTML documentation built at $(BUILDDIR)/html."

# Generate PDF using LaTeX
latexpdf:
	@echo "Building PDF documentation..."
	$(SPHINXBUILD) -M latexpdf "$(SOURCEDIR)" "$(BUILDDIR)"
	@echo "PDF documentation built at $(BUILDDIR)/latexpdf."

# Generate ePub
epub:
	@echo "Building ePub documentation..."
	$(SPHINXBUILD) -M epub "$(SOURCEDIR)" "$(BUILDDIR)"
	@echo "ePub documentation built at $(BUILDDIR)/epub."

# Catch-all rule for other targets
%:
	@$(SPHINXBUILD) -M $@ "$(SOURCEDIR)" "$(BUILDDIR)"
