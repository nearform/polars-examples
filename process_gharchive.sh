#!/bin/bash

mkdir -p compressed_raw_input
mkdir -p raw_input
mkdir -p clean_input
mkdir -p node_input
# rm -f compressed_raw_input/*
# rm -f raw_input/*
# rm -f clean_input/*

year=2024                   # Set the year
for month in {1..1}; do     # Set the month range
  for day in {1..10}; do    # Set the day range
    for hour in {0..23}; do # Set the hour range

      # Format the month, day, and hour to ensure two digits
      formatted_month=$(printf "%02d" $month)
      formatted_day=$(printf "%02d" $day)

      gz_file_path="compressed_raw_input/${year}-${formatted_month}-${formatted_day}-${hour}.json.gz"
      extracted_file_path="raw_input/${year}-${formatted_month}-${formatted_day}-${hour}.json"
      clean_file_path="clean_input/${year}-${formatted_month}-${formatted_day}-${hour}.json"
      node_file_path="node_input/${year}-${formatted_month}-${formatted_day}-${hour}.json"

      if [ ! -e "$gz_file_path" ]; then
        url="https://data.gharchive.org/${year}-${formatted_month}-${formatted_day}-${hour}.json.gz"
        wget -P compressed_raw_input "$url"
      fi

      if [ ! -e "$extracted_file_path" ]; then
        gunzip -c "$gz_file_path" >"$extracted_file_path"
      fi

    done

  done
done

poetry run python clean_raw_input.py
poetry run python node_clean_input.py
# poetry run python process_gharchive.py
