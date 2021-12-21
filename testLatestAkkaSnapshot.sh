VERSION=$(curl https://oss.sonatype.org/content/repositories/snapshots/com/typesafe/akka/akka-actor_2.13/ | grep -o '2.6\.[0-9]*+[0-9]*-[0-9a-z]*' | sort -V | tail -n 1)

echo "Running with version Akka $VERSION"

sbt -Doverride.akka.version=$VERSION +2.13.6 test
