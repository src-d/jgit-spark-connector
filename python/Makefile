ENGINE_UBER_JAR = engine-uber.jar
ENGINE_UBER_JAR_LOCATION = ../target/$(ENGINE_UBER_JAR)
JARS_DIR = jars


$(JARS_DIR):
	mkdir -p $(JARS_DIR)

.PHONY: test clean
test: clean $(JARS_DIR)
	cp $(ENGINE_UBER_JAR_LOCATION) $(JARS_DIR) && \
	python -m unittest discover -v -s ./test -t .

clean:
	if [ -d $(JARS_DIR) ] ; \
	then \
		rm -r $(JARS_DIR) ; \
	fi
