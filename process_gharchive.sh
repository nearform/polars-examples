#!/bin/bash

mkdir -p compressed_raw_input
mkdir -p raw_input
mkdir -p clean_input
mkdir -p node_input
# rm -f compressed_raw_input/*
# rm -f raw_input/*
# rm -f clean_input/*

year=2023                   # Set the year
for month in {1..12}; do    # Set the month range
  for day in {1..31}; do    # Set the day range
    for hour in {0..23}; do # Set the hour range

      # Format the month, day, and hour to ensure two digits
      formatted_month=$(printf "%02d" $month)
      formatted_day=$(printf "%02d" $day)

      gz_file_path="compressed_raw_input/${year}-${formatted_month}-${formatted_day}-${hour}.json.gz"
      extracted_file_path="raw_input/${year}-${formatted_month}-${formatted_day}-${hour}.json"
      clean_file_path="clean_input/${year}-${formatted_month}-${formatted_day}-${hour}.json"
      node_file_path="node_input/${year}-${formatted_month}-${formatted_day}-${hour}.json"

      if [ ! -e "$clean_file_path" ]; then
        if [ ! -e "$gz_file_path" ]; then
          echo "Downloading $gz_file_path"
          url="https://data.gharchive.org/${year}-${formatted_month}-${formatted_day}-${hour}.json.gz"
          wget -P compressed_raw_input "$url"
        else
          echo "Skipping downloading $gz_file_path"
        fi

        if [ ! -e "$extracted_file_path" ]; then
          echo "Extracting $gz_file_path"
          gunzip -c "$gz_file_path" >"$extracted_file_path"
        else
          echo "Skipping extracting $gz_file_path"
        fi
      fi
    done
    python clean_raw_input.py
    python node_clean_input.py
    rm -f compressed_raw_input/*
  done
done
