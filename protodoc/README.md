# Produce API documentation from proto files

This uses the following plugin for the protoc compiler:

  https://github.com/pseudomuto/protoc-gen-doc

To use it, you have to install the executable from one of the releases
somewhere in the PATH. The top level makefile has a target "apidocs"
and this executes a `protoc` command using the plugin and a template file
in this directory to produce HTML documentation of the API.
