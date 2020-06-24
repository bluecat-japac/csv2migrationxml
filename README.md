# CSV to BlueCat migration XML

the script needs to build the DNS zone structure, and then write out the sub-zones and resource records in the correct zones in XML.

## Requirement

This module requires the following:

* python3.7
* Input: csv file

## Usage

* To run by script command

```bash
    csv2xml.py <SOURCE>.csv
```

* To run by python command
```bash
    python csv2xml.py <SOURCE>.csv
```

* To see the full list of options:
```bash
    csv2xml.py --help
```

