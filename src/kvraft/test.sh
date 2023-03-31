#!/bin/bash
for i in {1..10}
do
   echo "================Iter $i================"
   if go test;
   then
      echo "================Done $i================"
   else
      break
   fi
done