CC = g++
CPPFLAGS = -m64 -I/usr/include/hadoop/

stripes: Stripes.cpp
	$(CC) $(CPPFLAGS) $< -lhadooppipes -lhadooputils -lpthread -lssl -g -O2 -o $@

pairs: Pairs.cpp
	$(CC) $(CPPFLAGS) $< -lhadooppipes -lhadooputils -lpthread -lssl -g -O2 -o $@

clean-stripes:
	rm -f stripes

clean-pairs:
	rm -f pairs
