## Description
This is a collection of some useful Hive UDFs and UDAFs.


## Provided Functions
### UDAFs
- Mode (`de.frosner.hive.udaf.Mode`) - computes the statistical mode of a group column

## Building from Source [![Build Status](https://travis-ci.org/FRosner/mustached-hive-udfs.svg)](https://travis-ci.org/FRosner/mustached-hive-udfs)
- `git clone https://github.com/FRosner/mustached-hive-udfs.git`
- `cd mustached-hive-udfs`
- `mvn package`
- Find the packaged jar file under target

## Usage
- Add mustached-hive-udfs-*.jar to your Hive classpath
- Import desired UDF
