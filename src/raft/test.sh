#!/bin/bash
for i in {1..10}
do
   echo "================Iter $i================"
   go test;
   echo "================Done $i================"
done