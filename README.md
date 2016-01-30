NAME
====
go-csv - a set of golang tools and libraries for manipulating CSV representations

DESCRIPTION
===========
go-csv is a set of tools for manipulating streams of CSV data.

As a rule, most tools in this set assume CSV files that include a header record that describes the contents of each field.

TOOLS
=====
* csv-select - selects the specified fields from the header-prefixed, CSV input stream
* uniquify - augments a partial key so that each record in the output stream has a unique natural key
* surrogate-keys - augments the input stream so that each record in the output stream has a surrogate key derived from the MD5 sum of the natural key
* csv-to-json - converts a CSV stream into a JSON stream.
* json-to-csv - converts a JSON stream into a CSV stream.
* csv-sort - sorts a CSV stream according to the specified columns.
* csv-join - joins two sorted CSV streams after matching on specified columns.

DOCUMENTATION
=============
For more information, refer to https://godoc.org/github.com/wildducktheories/go-csv .

LICENSE
=======
Refer to LICENSE file in same directory.

COPYRIGHT
=========
(c) 2014 - Wild Duck Theories Australia Pty Limited