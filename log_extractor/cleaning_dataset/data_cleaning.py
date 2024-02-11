import csv
import re
import pandas as pd

def remove_info_lines(input_csv, output_csv):
    keywords_to_keep = ["error", "exception", "fail", "warning", "critical", "fatal", "stacktrace", 
                         "traceback", "issue", "crash", "hang", "freeze",
                         "timeout", "deadlock", "corrupt", "invalid", "illegal", "unhandled", "uncaught", 
                         "unexpected", "unimplemented", "unsupported", "missing", "invalid", "illegal", 
                         "unauthorized", "denied", "forbidden", "blocked", "rejected", "panic", "abort"]
    
    with open(input_csv, 'r', newline='') as f_in, open(output_csv, 'w', newline='') as f_out:
        reader = csv.reader(f_in)
        writer = csv.writer(f_out)
        for row in reader:
            contains_keyword = any(keyword in cell.lower() for cell in row for keyword in keywords_to_keep)
            if any(re.search(r'\binfo\b', cell.lower()) for cell in row) and contains_keyword:
                writer.writerow(row)
            elif any(re.search(r'\binfo\b', cell.lower()) for cell in row) and not contains_keyword:
                continue
            elif not any(re.search(r'\binfo\b', cell.lower()) for cell in row) and contains_keyword:
                writer.writerow(row)
            elif not any(re.search(r'\binfo\b', cell.lower()) for cell in row) and not contains_keyword:
                writer.writerow(row)

def remove_spaces(input_csv, output_csv):
    with open(input_csv, 'r') as f_in, open(output_csv, 'w', newline='') as f_out:
        reader = csv.reader(f_in)
        writer = csv.writer(f_out)
        for row in reader:
            # Remove leading and trailing spaces from each cell in the row
            cleaned_row = [cell.strip() for cell in row]
            writer.writerow(cleaned_row)

def remove_empty_lines(input_csv, output_csv):
    with open(input_csv, 'r') as f_in, open(output_csv, 'w', newline='') as f_out:
        reader = csv.reader(f_in)
        writer = csv.writer(f_out)
        for row in reader:
            if any(field.strip() for field in row):  # Check if any field in the row contains non-whitespace characters
                writer.writerow(row)

def csv_to_parquet(input_csv, output_parquet):
    df = pd.read_csv(input_csv)
    df.to_parquet(output_parquet)

# Example usage:
if __name__ == "__main__":
    input_csv = "example.csv" 
    intermediate_csv = "example1.csv" 
    output_csv = "example_without_spaces.csv"  
    output_csv_final = "example_without_spaces_and_newlines.csv"
    output_parquet = "output_data.parquet"  

    remove_info_lines(input_csv, intermediate_csv)
    remove_spaces(intermediate_csv, output_csv)
    remove_empty_lines(output_csv, output_csv_final)
    csv_to_parquet(input_csv, output_parquet)
