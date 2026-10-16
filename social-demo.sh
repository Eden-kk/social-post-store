#!/bin/sh
set -eu
cd "$(dirname "$0")"
sh build.sh
java -cp build/classes lab.SocialStateTest
java -cp build/classes lab.SocialClusterTest
