#!/bin/sh
set -eu
cd "$(dirname "$0")"
sh build.sh
java -cp build/classes lab.SqlEngineTest
java -cp build/classes lab.DurableStateTest
java -cp build/classes lab.IdentityTest
java -cp build/classes lab.SqlClientTest
java -cp build/classes lab.CounterTest
java -cp build/classes lab.CounterDeltaTest
java -cp build/classes lab.CounterClusterTest
java -cp build/classes lab.SqlCounterTest
java -cp build/classes lab.SqlCounterClusterTest
java -cp build/classes lab.SocialStateTest
java -cp build/classes lab.SocialClusterTest
for round in 1 2 3; do
    echo "CLUSTER_ROUND=$round"
    java -cp build/classes lab.ClusterTest
done
echo ALL_TESTS_PASSED
