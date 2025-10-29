# Path to latexrun.
LATEXRUN_PATH := $(HOME)/rntz/e/r/rntztex/latexrun/latexrun
LATEXRUN := $(LATEXRUN_PATH)

# By default, try to build every tex file in this directory.
# Customize $(TEXS) to change this.
TEXS  := $(wildcard *.tex)
PDFS  := $(addsuffix .pdf,$(basename $(TEXS)))

.PHONY: all clean watch watch\:% FORCE
all: $(PDFS)

# `make watch` will automatically recompile your pdfs "live".
# You can also specify a target to recompile, eg. `make watch:foo.pdf`.
# It's a bit overenthusiastic, though; it reruns when ANYTHING changes.
watch: watch\:all

# MAC VERSION using fswatch
watch\:%:
	make --no-print-directory -j $*
	fswatch -Ee '/latex\.out/' -e '/\.#' -o . | xargs -n1 -I{} make --no-print-directory $*

# # LINUX VERSION using inotify-tools
# watch\:%: %
# 	@while inotifywait -e modify,move,delete -r . >/dev/null 2>&1; do \
# 		echo; \
# 		make --no-print-directory -j $*; \
# 	done

%.pdf: %.tex FORCE
	$(LATEXRUN) $<

clean:
	$(LATEXRUN) --clean-all
	rm -r latex.out
	rm *.o *.hi

# debugging: `make print-FOO` will print the value of $(FOO)
.PHONY: print-%
print-%:
	@echo $*=$($*)

# GHC
%: %.hs
	ghc -Wincomplete-patterns -O2 $<

%: %.rs
	rustc -O -g $<

.PHONY: run\:% sample\:%
run\:%: %
	./$<

sample\:%: %
	~/.cargo/bin/samply record ./$<
