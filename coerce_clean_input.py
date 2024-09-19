import json
import glob


def coerce_mixed_types(ndjson_file, output_file):
    with open(ndjson_file, "r") as infile, open(output_file, "w") as outfile:
        for line_num, line in enumerate(infile, start=1):
            try:
                data = json.loads(line)

                # Example: Ensure arrays contain uniform types and set nulls
                for key, value in data.items():
                    if isinstance(value, list):
                        # Coerce all elements in the list to strings, set empty list to None
                        data[key] = [str(v) for v in value] if value else None
                    elif isinstance(value, (int, float)):
                        # Convert numeric values to strings, or set to None if invalid
                        data[key] = str(value) if value else None
                    else:
                        # Ensure other invalid types are set to None
                        data[key] = value if value is not None else None

                outfile.write(json.dumps(data) + "\n")
            except json.JSONDecodeError as e:
                print(f"Error parsing line {line_num}: {e}")


files = glob.glob("clean_input/*.json")

if __name__ == "__main__":
    for i, input_file in enumerate(files, 1):
        filename = input_file.split("/")[-1]
        output_file = f"coerce_input/{filename}"
        print(f"processing file {i}: {input_file} -> {output_file}")
        coerce_mixed_types(input_file, output_file)
    print("Done!!!")
