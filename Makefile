#servidor mock up debe estar activo en otra consola
run:
	JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
	PATH="$$JAVA_HOME/bin:$$PATH" \
	SBT_OPTS="--add-exports=java.base/sun.nio.ch=ALL-UNNAMED" \
	sbt "run --subscription-file data/local_subscriptions.json"