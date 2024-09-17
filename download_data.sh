#!/bin/bash

mkdir -p input
mkdir -p raw_input
mkdir -p compressed_raw_input

year=2024 # Set the year
for month in {1..01}; do    # Set the month range
  for day in {1..01}; do    # Set the day range
    for hour in {0..23}; do # Set the hour range

      # Format the month, day, and hour to ensure two digits
      formatted_month=$(printf "%02d" $month)
      formatted_day=$(printf "%02d" $day)

      url="https://data.gharchive.org/${year}-${formatted_month}-${formatted_day}-${hour}.json.gz"
      gz_file_path="compressed_raw_input/${year}-${formatted_month}-${formatted_day}-${hour}.json.gz"
      extracted_file_path="${year}-${formatted_month}-${formatted_day}-${hour}.json"

      wget -P compressed_raw_input "$url"

      gunzip -c "$gz_file_path" >raw_input/"$extracted_file_path"
    done
  done
done
