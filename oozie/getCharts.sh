#!/bin/bash

curl "http://ws.audioscrobbler.com/2.0/?method=geo.getTopTracks&api_key=0f7f9f27dfcb1edfab7c5e07615ab9a3&country=germany&limit=100&page=2" -o charts.xml
hdfs dfs -moveFromLocal -f charts.xml /user/hue/incoming/charts.xml
