# dfs-datastores [![Build Status](https://secure.travis-ci.org/nathanmarz/dfs-datastores.png?branch=master)](http://travis-ci.org/nathanmarz/dfs-datastores)

A dramatically simpler and more powerful way to store records on a distributed filesystem.

to include dfs-datastores in your project, add the following to project.clj:

```clojure
[com.backtype/dfs-datastores "2.0.0"]
```

To run the tests:

```clojure
lein do sub install, sub junit
```

### How to import the project into Eclipse

After checkout run the commands above followed by:

```clojure
lein eclipse
lein do sub eclipse
```

This will create the neccesary eclipse .project and .classpath files. Now go into Eclipse and import the project:

File -> Import... -> General -> Existing Projects into Workspace

In the Import Projects dialog deselect the bundle / parent project. The parent project only serves as a container and is not needed in Eclipse. Once the projects dfs-datastores and dfs-datastores-cascading are imported the build path needs to be adjusted. For this add the following directories to the build path (use context menu in the Package Explorer -> Build Path -> Use as Source Folder:

```
dfs-datastores -> src -> main -> java
dfs-datastores -> src -> test -> java

dfs-datastores-cascading -> src -> main -> java
dfs-datastores-cascading -> src -> test -> java
```
That's it. Now the projects can be edited.

### Changelog

2.0.0
 - add Spark support (consolidate, coerce, balanced distcp can use Spark as engine)
 - improve file handling on AWS S3 file systems