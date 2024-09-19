import json
import glob


def filter_node(ndjson_file, output_file):
    with open(ndjson_file, "r") as infile, open(output_file, "w") as outfile:
        for line_num, line in enumerate(infile, start=1):
            try:
                data = json.loads(line)
                if data["repo"]["name"] != "nodejs/node":
                    continue

                outfile.write(json.dumps(data) + "\n")
            except json.JSONDecodeError as e:
                print(f"Error parsing line {line_num}: {e}")


files = glob.glob("coerce_input/*.json")

if __name__ == "__main__":
    for i, input_file in enumerate(files, 1):
        filename = input_file.split("/")[-1]
        output_file = f"node_input/{filename}"
        print(f"processing file {i}: {input_file} -> {output_file}")
        filter_node(input_file, output_file)
    print("Done!!!")
