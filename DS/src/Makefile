JAVA = javac

all:
	 $(JAVA) -cp .:./protobuf-java-2.5.0.jar -d ../bin/ *.java

clean:
	 killall rmiregistry
	 rm -rf  ../bin/*.class
