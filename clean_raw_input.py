import json
import glob


def clean_ndjson_file(input_file, output_file):
    with open(input_file, "r") as infile, open(output_file, "w") as outfile:
        for line in infile:
            try:
                # Attempt to parse each line as a JSON object
                data = json.loads(line)

                # Example cleanup: If the object is invalid, replace with None or clean accordingly
                if not isinstance(
                    data, (dict, list, str, int, float, bool, type(None))
                ):
                    data = None  # Handle as per your requirement

                # Write the cleaned line back to the output file
                outfile.write(json.dumps(data) + "\n")
            except json.JSONDecodeError as e:
                print(f"Invalid JSON in line: {e}")
                # Optionally, you can skip the invalid lines or log them


files = glob.glob("raw_input/*.json")

if __name__ == "__main__":
    for i, input_file in enumerate(files, 1):

        filename = input_file.split("/")[-1]
        output_file = f"clean_input/{filename}"
        print(f"processing file {i}: {input_file} -> {output_file}")
        clean_ndjson_file(input_file, output_file)
    print("Done!!!")
