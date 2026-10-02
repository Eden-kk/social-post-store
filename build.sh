#!/bin/sh
set -eu
cd "$(dirname "$0")"
mkdir -p build/classes
find src/main/java src/test/java -name '*.java' -print > build/sources.txt
javac --release 17 -d build/classes @build/sources.txt
