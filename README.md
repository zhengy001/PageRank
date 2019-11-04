# PageRank

## Overview
Pagerank algorithm implementation using Hadoop MapReduce and Java language.

## Step
* According to the transition matrix input(transition.txt) build a relationship model
* Calculate the weight or transiton fact between pages
** PageRank1 = Transition X PageRank0
* Sum up each unit weight to get new rank model
* Converge above steps N times

## How to run
$ hadoop jar pagerank.Driver -trans /transition -rank /pagerank -unit /output -times 5

usage: pagerank.Driver
* -rank <arg>    input rank file dir
* -times <arg>   times of convergence
* -trans <arg>   input transition file dir
* -unit <arg>    unit output dir
