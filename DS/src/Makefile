JAVA = javac

all:
	 rmiregistry 1099 &
	 $(JAVA) -cp .:./protobuf-java-2.5.0.jar *.java

clean:
	 killall rmiregistry
	 rm -rf  *.class
