import glob
import json
import os
from datetime import datetime
from pathlib import Path

import polars as pl
from polars.datatypes import (
    Boolean,
    Int64,
    List,
    String,
    Struct,
)
from polars.schema import Schema

BASEDIR = Path(__file__).resolve().parent.parent


def clean_ndjson_file(input_file, output_file):
    """
    Clean json lines

    Some lines downloaded from hgarchive have some invalid or unescaped caracter which breaks when loading on Polars,
    so this will just load as json and dump again to properly escape all caracters and be able to load on polars.
    """
    with open(input_file, "r") as infile, open(output_file, "w") as outfile:
        for line in infile:
            try:
                # Attempt to parse each line as a JSON object
                data = json.loads(line)

                # Write the cleaned line back to the output file
                outfile.write(json.dumps(data) + "\n")
            except json.JSONDecodeError as e:
                print(f"Invalid JSON in line: {e}")
                # Optionally, you can skip the invalid lines or log them
        print("... done!!!")


# 0. Loading the data
schema = Schema(
    [
        ("id", String),
        ("type", String),
        (
            "actor",
            Struct(
                {
                    "id": Int64,
                    "login": String,
                    "display_login": String,
                    "gravatar_id": String,
                    "url": String,
                    "avatar_url": String,
                }
            ),
        ),
        ("repo", Struct({"id": Int64, "name": String, "url": String})),
        (
            "payload",
            Struct(
                {
                    "repository_id": Int64,
                    "push_id": Int64,
                    "size": Int64,
                    "distinct_size": Int64,
                    "ref": String,
                    "head": String,
                    "before": String,
                    "commits": List(
                        Struct(
                            {
                                "sha": String,
                                "author": Struct({"email": String, "name": String}),
                                "message": String,
                                "distinct": Boolean,
                                "url": String,
                            }
                        )
                    ),
                    "action": String,
                    "number": Int64,
                    "pull_request": Struct(
                        {
                            "state": String,
                            "user": Struct(
                                {
                                    "login": String,
                                    "id": Int64,
                                    "node_id": String,
                                    "avatar_url": String,
                                    "gravatar_id": String,
                                    "url": String,
                                    "html_url": String,
                                    "followers_url": String,
                                    "following_url": String,
                                    "gists_url": String,
                                    "starred_url": String,
                                    "subscriptions_url": String,
                                    "organizations_url": String,
                                    "repos_url": String,
                                    "events_url": String,
                                    "received_events_url": String,
                                    "type": String,
                                    "site_admin": Boolean,
                                }
                            ),
                            "merged": Boolean,
                        }
                    ),
                }
            ),
        ),
        ("public", String),
        ("created_at", String),
    ]
)


def filter_node(batch, scan=False, streaming=False):
    print(f"processing {BASEDIR}/data/clean batch: {batch}")
    start = datetime.now()
    inital = start
    with open(f"{BASEDIR}/data/final/node.json", "a") as outfile:
        last = datetime.now()
        print(f"{last-start} - Opened output file")
        start = last

        if scan:
            df = pl.scan_ndjson(batch, schema=schema)
            last = datetime.now()
            print(f"{last-start} - Scan Completed")
            start = last

            filtered_df_node = df.filter(
                pl.col("repo").struct.field("name") == "nodejs/node"
            )
            last = datetime.now()
            print(f"{last-start} - Filter Lazy Completed")
            start = last

            data_collected = filtered_df_node.collect(streaming=streaming)
            last = datetime.now()
            print(f"{last-start} - Collect streaming={streaming} Completed")
            start = last

            data = data_collected.write_ndjson()
            last = datetime.now()
            print(f"{last-start} - write_ndjson Completed")
            start = last

            outfile.write(data)
            last = datetime.now()
            print(f"{last-start} - Write Completed")
            start = last

        else:
            df = pl.read_ndjson(batch, schema=schema)
            last = datetime.now()
            print(f"{last-start} - Read Completed")
            start = last

            filtered_df_node = df.filter(
                pl.col("repo").struct.field("name") == "nodejs/node"
            )
            last = datetime.now()
            print(f"{last-start} - Filter Completed")
            start = last

            data = filtered_df_node.write_ndjson()
            last = datetime.now()
            print(f"{last-start} - write_ndjson Completed")
            start = last

            outfile.write(data)
            last = datetime.now()
            print(f"{last-start} - Write Completed")
            start = last

    print(f"{last-inital} - Total time\n\n")


if __name__ == "__main__":
    raw_files = glob.glob(f"{BASEDIR}/data/raw/*.json")
    for i, input_file in enumerate(raw_files, 1):
        if not Path(input_file).exists():
            continue

        filename = input_file.split("/")[-1]
        output_file = f"{BASEDIR}/data/clean/{filename}"

        if Path(output_file).exists():
            print("... file already clean!!!")
            os.remove(input_file)
            continue

        print(f"processing file {i}: {input_file}", end="", flush=True)
        clean_ndjson_file(input_file, output_file)
        os.remove(input_file)

    filter_node(f"{BASEDIR}/data/clean/*.json", scan=True, streaming=True)
    # Use glob to find all files matching the pattern
    clean_files = glob.glob(f"{BASEDIR}/data/clean/*.json")

    # Iterate and remove each file
    for file in clean_files:
        os.remove(file)
    print("Done!!!")
