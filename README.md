# Cloud Surf

Cloud Storage Application.
Supports functions uploading, checking out, deleting, and editing files.
Implements distributed system for fault tolerance through replicated lgos and 2-phase commit.
Logs of each change are backed up to multiple servers on each significant change.



## To Compile

$ mvn protobuf:compile protobuf:compile-custom

## To build the code:

$ mvn package

## To run the services:

$ target/surfstore/bin/runBlockServer
$ target/surfstore/bin/runMetadataStore

## To run the client

$ target/surfstore/bin/runClient

## To delete all programs and object files

$ mvn clean
