README
Group 37 - Phase 3

Running the code-

1)We have sent the whole source code as a Maven Project. If you want to verify its execution, extract the source code folder (titled group37_phase3). Extract it in eclipse and then build it.
2) Run it on JDK 1.7.
3)The file NYHotSpots.java contains our primary logic of calculating G* value for New York Hot Spots.
4)There is another file ValueComparator.java which has the code to sort our map of 50 hotspots according to the G* value.
5)There is a main method in NYHotSpots.java. So, to run on Eclipse, just run the NYHotSpots.java file and provide two arguments, with first being input location and the second being output location and the output csv with lat, long, time and corresponding G* value will be generated for the top 50 hotspots.
6)To run it on spark, then use the following command
/usr/local/spark/bin/spark-submit --class NYHotSpots group37_phase3.jar [input path] [path to output]
where spark is in /usr/local/spark and you are in the directory where the jar is present.
7) The output file is in local system and not hdfs.

About the code-

1)The code does not have the logic of removing the header from the input file as was specified on the Discussion Board.
