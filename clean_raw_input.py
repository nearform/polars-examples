import json
import glob
from pathlib import Path


def clean_ndjson_file(input_file, output_file):
    if Path(output_file).exists():
        print("... skipping!!!")
        return

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


files = glob.glob("raw_input/*.json")

if __name__ == "__main__":
    for i, input_file in enumerate(files, 1):

        filename = input_file.split("/")[-1]
        output_file = f"clean_input/{filename}"
        print(f"processing file {i}: {input_file} -> {output_file}", end="")
        clean_ndjson_file(input_file, output_file)
    print("Done!!!")
