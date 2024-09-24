import glob
import json
from datetime import datetime
import polars as pl
from polars.datatypes import (
    Array,
    Binary,
    Boolean,
    Categorical,
    DataType,
    Date,
    Datetime,
    Decimal,
    Duration,
    Enum,
    Field,
    Float32,
    Float64,
    Int8,
    Int16,
    Int32,
    Int64,
    List,
    Null,
    Object,
    String,
    Struct,
    Time,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Unknown,
    Utf8,
)
from polars.schema import Schema


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


def filter_node(input_folder, scan=False, streaming=False):
    start = datetime.now()
    inital = start
    with open("node_input/node.json", "w") as outfile:
        last = datetime.now()
        print(f"{last-start} - Opened output file")
        start = last

        if scan:
            df = pl.scan_ndjson(
                f"{input_folder}/*.json",
                schema=schema,
            )
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
            df = pl.read_ndjson(
                f"{input_folder}/*.json",
                schema=schema,
            )
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
    print(f"processing clean_input folder\n\n")
    filter_node("clean_input", scan=False)
    filter_node("clean_input", scan=True, streaming=False)
    filter_node("clean_input", scan=True, streaming=True)
    print("Done!!!")
