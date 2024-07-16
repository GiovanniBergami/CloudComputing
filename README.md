# CloudComputing Project

This repository contains the Cloud Computing Project for the accademic year 2023-2024, realized by Giovanni Bergami, Marco Bologna, Gabriele Frassi.
It is a Map-Reduce implementation of a letter frequency counter, through Hadoop, suitable for large text files.

The project can be executed using the .jar file, with the following command from the target folder
> hadoop jar file.Jar it.unipi.hadoop.Start datasetName output nReducers InMapperCombining

where
• nReducers specifies the number of reducers of the second job. It’s important to notice that the number of reducers for the first job is always 1, since there is only one key to be reduced.
• InMapperCombining can be 0 (combiner, default value) or 1 (In-Mapper combiner).

The output can be seen with the command:
> hadoop fs -cat output/part-r*

Outputs and informations on result are available in the folder "Output"