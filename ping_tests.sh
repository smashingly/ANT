#!/bin/bash
#set -x

if [ -z "$1" ]; then
    echo "Error: No input CSV file specified."
    echo "Usage: $(basename $0) input_csv_file [output_directory]"
    printf "\n"
    echo "If no output_directory is provided, the logfile and results CSV will be created in the current folder."
    exit 1
fi

# if user provided an output directory then we'll prepend that to the filename later
if [ -n "$2" ]; then
    output_dir="$2"
else
    output_dir="."
fi

# Get the input file name from the command line argument
input_csv="$1"

# remove the .csv extension
base_name=$(basename "${input_csv%.csv}")
#printf "base_name is $base_name\n"

# Replace 'tests' with 'results'
log_name="${base_name/tests/results}"
#printf "log_name is $log_name\n"

# Define the logfile for logging data about failed tests
log_file="${output_dir}/${log_name}.log"
#printf "log_file is $log_file\n"

# Output CSV file for results. Prepend the output_dir.
output_csv="${output_dir}/${log_name}.csv"
#printf "Output CSV incl. path will be: $output_csv\n\n"
#exit 1

# Add datetime entry to log file and also print to stdout
printf "\n---------- Test initiated at $(date) ----------\n" >> "$log_file"
printf "\n---------- Test initiated at $(date) ----------\n"

# Check if the output CSV file exists, if not, create it with headers 
if [ ! -e "$output_csv" ]; then
    echo "id_number,min,avg,max,stddev,command" > "$output_csv"
fi
printf "\n,,,,,---------- Test initiated at $(date) ----------\n" >> "$output_csv"

# Loop through each line in the input CSV file 
while IFS=, read -r -a line; do
    id_number="${line[0]}"
    ssh_command="${line[1]}"
    
    result=$(eval "$ssh_command" 2>&1)
    if [ $? -eq 0 ]; then
        rtt_results=$(echo "$result" | grep 'min/avg/max')
        echo "Test $id_number: Success. Result: $rtt_results"
        echo "Test $id_number: Success. Result: $rtt_results" >> $log_file
        ping_result=$(echo "$result" | grep -oP 'min/avg/max/(m|std)dev = \K[0-9.\/]+')

        # Extract round-trip min, avg, max, and stddev values
        IFS='/' read -r min avg max stddev <<< "$ping_result"
       
        #Log the results to the output CSV file
        echo "$id_number,$min,$avg,$max,$stddev,$ssh_command" >> "$output_csv"    
    else
        # Error-handling - notify user, log failure indication to output CSV, and full logging to failure logfile
        echo "Test $id_number: Failure. Command: ' $ssh_command '"
        echo "Test $id_number: Failure. Command: ' $ssh_command '" >> $log_file
        echo "$id_number,fail,fail,fail,fail,$ssh_command" >> $output_csv
        printf "\n" >> $log_file
        echo "*****************************************************************************" >> $log_file
        datetime=$(date)
        echo "$datetime  Test #$id_number failure. Full output of test:" >> $log_file
        echo "$result" >> $log_file
        echo "*****************************************************************************" >> $log_file
    fi

done < "$input_csv"
# Add datetime entry to log file
printf "\n---------- Test ended at $(date) ----------\n" >> "$log_file"
# Print end message to stdout
printf "\n---------- Test ended at $(date) ----------\n"

