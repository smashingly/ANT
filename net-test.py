import subprocess
import csv
import os
import datetime
import logging
import sys


def run_test(id_number, ssh_command):
    # TODO: we may change this function to take source, dest, and other parameters, as these need to be known
    #  for outputting results that influxdb/Grafana can use later. The question is how we can construct the SSH
    #  command from these parameters, as currently it's quite handily included in the input CSV file. An easy hack
    #  for now would be to replace id_number with a description field, eg. "ping from edats003 to az-syd-vm". That
    #  would give influxdb a unique (ish) identifier for each row.
    try:
        # Execute the command and get the result - this currently is customised for a ping test
        result = subprocess.check_output(ssh_command, shell=True, stderr=subprocess.STDOUT).decode()

        # Look for the line in the ping stats and assign it to rtt_results
        rtt_results = [line for line in result.split('\n') if 'min/avg/max' in line]

        # Log output to the screen and to logfile - we'll convert this later to use the logging module
        # TODO: we're leaving the separate print() statement here for now, because the logger will only display
        #  messages at ERROR level or above, so we won't see INFO level messages on the console (screen). We can
        #  change this later, but for now, we'll leave the print() statement.
        msg = f"Test {id_number}: Success. Result: {rtt_results}"
        print(msg)
        logger.info(msg)

        # Parse out the actual ping statistics from the relevant line in the output. Split at "="
        # Example ping output line: 'round-trip min/avg/max/stddev = 0.053/0.154/0.243/0.063 ms'
        ping_result = rtt_results[0].replace(" ms", "").split('=')[1].strip()

        # ping_result now looks something like this: '0.053/0.154/0.243/0.063' - so we will now split it by the '/'
        min_rtt, avg_rtt, max_rtt, stddev_rtt = ping_result.split('/')
        return [id_number, min_rtt, avg_rtt, max_rtt, stddev_rtt, ssh_command]

    except subprocess.CalledProcessError as e:
        t_stamp = datetime.datetime.now()
        logger.error(f"***************************************************************************************")
        logger.error(f"{t_stamp}  Test #{id_number} (command '{ssh_command}') failed. Full output of test:")
        logger.error(e.output.decode())
        logger.error(f"***************************************************************************************")

        # if something failed in the command, we'll set the RTT values to None
        min_rtt, avg_rtt, max_rtt, stddev_rtt = None, None, None, None
        return [id_number, min_rtt, avg_rtt, max_rtt, stddev_rtt, ssh_command]


def read_input_csv(input_csv):
    # Read the input CSV file and return a list of dicts, each line being mapped to a dictionary, based on the
    # header row of the CSV file. The first character of the header row is "#" and this should be ignored when
    # constructing the first column's name.  Current header row = #test_type, destination, count, size

    with open(input_csv, 'r') as input_file:
        reader = csv.reader(input_file)
        header = next(reader)
        header = [h.lstrip('#') for h in header]
        data = [dict(zip(header, row)) for row in reader]
    return data


if len(sys.argv) < 2:
    print("\nError: No input CSV file specified.")
    print(f"Usage:  python3 {sys.argv[0]} input_csv_file [output_directory]\n")
    sys.exit(1)
elif len(sys.argv) == 3:
    input_csv = sys.argv[1]
    output_dir = sys.argv[2]
else:
    input_csv = sys.argv[1]
    output_dir = '.'

# Remove the path from the input filename. We use this base name as the basis of results & log file names
base_name = os.path.basename(input_csv).replace('.csv', '')

# Create the base name for output files by adding yyyymmddhhmmss to the base name.
out_basename = f"{base_name}_{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}"

# TODO: This approach creates new files upon each program run, to avoid overwriting previous results. But over time
#  it will result in a lot of files, so we may want to add log file rotation, to manage log files better. E.g.,
#  keep the last 28 days worth of logs, and delete any old ones.
log_file = os.path.join(output_dir, f"{out_basename}.log")
output_csv = os.path.join(output_dir, f"{out_basename}.csv")
# TODO: for output files, we may want to implement a clean-up that runs on any output files that are older than
#  a certain age, to avoid filling up the disk with old files.

"""######################### Start of logger setup and configuration #########################"""
logger = logging.getLogger("ant")  # Create a custom logger - "ant" is an arbitrary name
logger.setLevel(logging.INFO)

# Create handlers. Naming convention: c = console, f = file
c_handler = logging.StreamHandler()
f_handler = logging.FileHandler(log_file)
c_handler.setLevel(logging.ERROR)  # determines the error-level (or above) that will be sent to console
f_handler.setLevel(logging.DEBUG)  # determines the error-level (or above) that will be sent to file

# Create formatters and add it to handlers
c_format = logging.Formatter("%(levelname)s: %(message)s")
f_format = logging.Formatter(fmt="%(asctime)s.%(msecs)03d - %(levelname)08s: %(funcName)s: %(message)s",
                             datefmt="%Y-%m-%d %H:%M:%S")
c_handler.setFormatter(c_format)
f_handler.setFormatter(f_format)

# Add handlers to the logger
logger.addHandler(c_handler)
logger.addHandler(f_handler)
"""######################### End of logger setup and configuration #########################"""

logger.info("*" * 20 + f" Initial startup at: {datetime.datetime.now()} " + "*" * 20)
logger.info(f"Input CSV file: {input_csv}")

# If the output file doesn't exist (extremely likely), create it and write the header row
if not os.path.exists(output_csv):
    with open(output_csv, 'w') as output:
        writer = csv.writer(output)
        writer.writerow(["id_number", "min", "avg", "max", "stddev", "command"])
with open(output_csv, 'a') as output:
    writer = csv.writer(output)
    writer.writerow(["", "", "", "", "", f"---------- Test initiated at {datetime.datetime.now()} ----------"])

tests = read_input_csv(input_csv)  # a list of dictionaries, each dict representing a test to be run

for test in tests:
    # print(f"I will run these tests: {id_number}, {ssh_command}")
    results = run_test(id_number=test['id_number'], ssh_command=test['ssh_command'])
    with open(output_csv, 'a') as output:
        writer = csv.writer(output)
        writer.writerow(results)

logger.info(f"Test ended at {datetime.datetime.now()}")
