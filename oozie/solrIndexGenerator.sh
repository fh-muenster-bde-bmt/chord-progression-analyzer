#!/bin/bash

curl http://localhost:8983/solr/bde/update/csv?commit=true --data-binary @solr.csv -H 'Content-type:text/plain; charset=utf-8'