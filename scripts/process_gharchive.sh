#!/bin/bash
BASE_DIR="$(dirname "$(dirname "$(realpath "${BASH_SOURCE[0]}")")")"
mkdir -p ${BASE_DIR}/data/compressed
mkdir -p ${BASE_DIR}/data/raw
mkdir -p ${BASE_DIR}/data/clean
mkdir -p ${BASE_DIR}/data/final

year=2023                  # Set the year
for month in {1..12}; do   # Set the month range
  for day in {1..31}; do   # Set the day range
    for hour in {0..5}; do # Set the hour range

      # Format the month, day, and hour to ensure two digits
      formatted_month=$(printf "%02d" $month)
      formatted_day=$(printf "%02d" $day)

      gz_file_path="${BASE_DIR}/data/compressed/${year}-${formatted_month}-${formatted_day}-${hour}.json.gz"
      extracted_file_path="${BASE_DIR}/data/raw/${year}-${formatted_month}-${formatted_day}-${hour}.json"
      clean_file_path="${BASE_DIR}/data/clean/${year}-${formatted_month}-${formatted_day}-${hour}.json"
      node_file_path="${BASE_DIR}/data/final/${year}-${formatted_month}-${formatted_day}-${hour}.json"

      if [ ! -e "$clean_file_path" ]; then
        if [ ! -e "$gz_file_path" ]; then
          echo "Downloading $gz_file_path"
          url="https://data.gharchive.org/${year}-${formatted_month}-${formatted_day}-${hour}.json.gz"
          wget -P ${BASE_DIR}/data/compressed "$url"
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
    poetry run python ${BASE_DIR}/scripts/process_files.py
    rm -f ${BASE_DIR}/data/compressed/*
  done
done
