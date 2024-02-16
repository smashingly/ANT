import subprocess
import csv
import os
import datetime


# TODO: we may change this function to take source, dest, and other parameters, as these need to be known for outputting
#  results that influxdb/Grafana can use later. The question is how we can construct the SSH command from these
#  parameters, as currently it's quite handily included in the input CSV file. An easy hack for now would be to
#  replace id_number with a description field, eg. "ping from edats003 to az-syd-vm". That would give influxdb a
#  unique (ish) identifier for each row.
def run_test(id_number, ssh_command, log_file, output_csv):
    try:
        # Execute the command and get the result - this currently is customised for a ping test
        result = subprocess.check_output(ssh_command, shell=True, stderr=subprocess.STDOUT).decode()

        # Look for the line in the ping stats and assign it to rtt_results
        rtt_results = [line for line in result.split('\n') if 'min/avg/max' in line]

        # Log output to the screen and to logfile - we'll convert this later to use the logging module
        # TODO: Convert to use Python's logging module
        print(f"Test {id_number}: Success. Result: {rtt_results}")
        with open(log_file, 'a') as log:
            log.write(f"Test {id_number}: Success. Result: {rtt_results}\n")

        # Parse out the actual ping statistics from the relevant line in the output
        # Example ping output line: 'round-trip min/avg/max/stddev = 0.053/0.154/0.243/0.063 ms'
        ping_result = rtt_results[0].replace(" ms", "").split('=')[1].strip()

        # ping_result now looks something like this: '0.053/0.154/0.243/0.063' - so we split it by '/'
        min_rtt, avg_rtt, max_rtt, stddev_rtt = ping_result.split('/')

        # Write a row into the output CSV file. We may later change this to output JSON (or something else that
        # influxdb likes.)
        with open(output_csv, 'a') as output:
            writer = csv.writer(output)
            writer.writerow([id_number, min_rtt, avg_rtt, max_rtt, stddev_rtt, ssh_command])
    except subprocess.CalledProcessError as e:
        print(f"Test {id_number}: Failure. Command: '{ssh_command}'")
        with open(log_file, 'a') as log:
            log.write(f"Test {id_number}: Failure. Command: '{ssh_command}'\n")
        with open(output_csv, 'a') as output:
            writer = csv.writer(output)
            # TODO: find out from Buddika if 'fail' is usable by Grafana or if we need to provide some other value
            #  that indicates failure, eg. zero, or null.
            writer.writerow([id_number, 'fail', 'fail', 'fail', 'fail', ssh_command])
        with open(log_file, 'a') as log:
            log.write(f"*****************************************************************************\n")
            log.write(f"{datetime.datetime.now()}  Test #{id_number} failure. Full output of test:\n")
            log.write(e.output.decode())
            log.write(f"*****************************************************************************\n")


def main(input_csv, output_dir='.'):
    # Remove the path from the input filename. We use this base name as the basis of results & log file names
    base_name = os.path.basename(input_csv).replace('.csv', '')

    ## One old idea for file nameing...
    # out_basename = base_name.replace('tests', 'results')

    ## Another old approach for file naming... it used the name of this script's file
    # out_basename = os.path.basename(__file__).replace('.py', '')

    # Create the base name for output files by adding yyyymmddhhmmss to the base name
    out_basename = f"{base_name}_{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}"
    # out_basename = base_name + "_output"
    log_file = os.path.join(output_dir, f"{out_basename}.log")
    output_csv = os.path.join(output_dir, f"{out_basename}.csv")

    # TODO: Replace with Python logging module later
    with open(log_file, 'a') as log:
        log.write(f"\n---------- Test initiated at {datetime.datetime.now()} ----------\n")
    print(f"\n---------- Test initiated at {datetime.datetime.now()} ----------\n")

    if not os.path.exists(output_csv):
        with open(output_csv, 'w') as output:
            writer = csv.writer(output)
            writer.writerow(["id_number", "min", "avg", "max", "stddev", "command"])
    with open(output_csv, 'a') as output:
        writer = csv.writer(output)
        writer.writerow(["", "", "", "", "", f"---------- Test initiated at {datetime.datetime.now()} ----------"])

    with open(input_csv, 'r') as input_file:
        reader = csv.reader(input_file)
        for line in reader:
            id_number, ssh_command = line
            run_test(id_number, ssh_command, log_file, output_csv)

    with open(log_file, 'a') as log:
        log.write(f"\n---------- Test ended at {datetime.datetime.now()} ----------\n")
    print(f"\n---------- Test ended at {datetime.datetime.now()} ----------\n")


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("\nError: No input CSV file specified.")
        print("Usage:  python3 script.py input_csv_file [output_directory]\n")
        sys.exit(1)
    elif len(sys.argv) == 3:
        main(sys.argv[1], sys.argv[2])
    else:
        main(sys.argv[1])
