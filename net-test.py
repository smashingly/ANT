import json
import subprocess
import csv
import os
import datetime
import logging
import sys

TEST_TYPES = ["latency", "throughput", "jitter"]


def parse_ping_results(test_data: dict):
    id_number = test_data['id_number']
    test_command = test_data['test_command']
    raw_output = test_data['raw_output']

    result_rtt_text = [line for line in raw_output.split('\n') if 'min/avg/max' in line]

    # Parse out the actual ping statistics from the relevant line in the output. Split at "="
    # Example ping output line: 'round-trip min/avg/max/stddev = 0.053/0.154/0.243/0.063 ms'
    rtt_data = result_rtt_text[0].replace(" ms", "").split('=')[1].strip()

    # rtt_data now looks something like this: '0.053/0.154/0.243/0.063' - so we will now split it by the '/'
    min_rtt, avg_rtt, max_rtt, stddev_rtt = rtt_data.split('/')

    # Log output to the screen and to logfile. We do this in each specific parse function because we have easy access
    #  to the variables for the specific test type. This allows us to output short-form results in a one-line log entry.
    #  We could do this in run_test() but we'd need a block of if-logic that figures out the test type then extracts
    #  the necessary key/value data from the results dict, then generates the appropriate message.
    # TODO: we're leaving the separate print() statement here for now, because the logger will only display
    #  messages at ERROR level or above, so we won't see INFO level messages on the console (screen). And we do not
    #  want to change the console logging level away from ERROR. We can change this later.
    short_form_results = f"{min_rtt}/{avg_rtt}/{max_rtt}/{stddev_rtt}"
    msg = f"Test {id_number}: Success. Result: {short_form_results} ms (min/avg/max/mdev-or-stddev)"
    print(msg)
    logger.info(msg)

    return {
        "id_number": id_number,
        "timestamp": str(test_data['timestamp']),
        "source": test_data['test_params']['source'],
        "destination": test_data['test_params']['destination'],
        "min_rtt": min_rtt,
        "avg_rtt": avg_rtt,
        "max_rtt": max_rtt,
        "stddev_rtt": stddev_rtt,
        "test_command": test_command
    }


def parse_iperf_results(test_data: dict):
    # iperf3 output is in JSON format, and for a throughput test, the data we need is at
    # (data).end.sum_received.seconds, .bytes, and .bits_per_second.

    id_number = test_data['id_number']
    test_command = test_data['test_command']
    raw_output = test_data['raw_output']
    test_type = test_data['test_params']['test_type']

    # Convert the JSON string to a Python dictionary
    command_result = json.loads(raw_output)

    if test_type == "throughput":
        parsed_results = {
            "id_number": id_number,
            "timestamp": str(test_data['timestamp']),
            "source": test_data['test_params']['source'],
            "destination": test_data['test_params']['destination'],
            "seconds": command_result['end']['sum_sent']['seconds'],
            "bytes": command_result['end']['sum_sent']['bytes'],
            "bits_per_second": command_result['end']['sum_sent']['bits_per_second'],
            "test_command": test_command
        }
        short_form_results = f"{parsed_results['seconds']} seconds; {parsed_results['bytes']} bytes; " \
                             f"{parsed_results['bits_per_second']} bits/sec"
    elif test_type == "jitter":
        parsed_results = {
            "id_number": id_number,
            "timestamp": str(test_data['timestamp']),
            "source": test_data['test_params']['source'],
            "destination": test_data['test_params']['destination'],
            "jitter_ms": command_result['end']['sum']['jitter_ms'],
            "packets": command_result['end']['sum']['packets'],
            "lost_packets": command_result['end']['sum']['lost_packets'],
            "test_command": test_command
        }
        short_form_results = f"{parsed_results['jitter_ms']} ms jitter; {parsed_results['packets']} packets; " \
                                f"{parsed_results['lost_packets']} lost"
    else:
        raise ValueError(f"Invalid test type '{test_type}' passed for test {id_number}.")

    # Log output to the screen and to logfile. We do this in each specific parse function so that we have access to
    #  the variables for that specific test type. This allows us to output short-form results in a one-line log entry.
    # TODO: we're leaving the separate print() statement here for now, because the logger will only display
    #  messages at ERROR level or above, so we won't see INFO level messages on the console (screen). And we do not
    #  want to change the console logging level away from ERROR. We can change this later.
    msg = f"Test {id_number}: Success. Results: {short_form_results}"
    print(msg)
    logger.info(msg)

    return parsed_results


def parse_results(id_number, timestamp, test_params, test_command, raw_output):
    # This is a wrapper function to make the code inside run_test() tidier. It just calls the relevant parse function.
    #  This abstraction also makes it easier add more test types in future.

    test_data = {
        "id_number": id_number,
        "timestamp": str(timestamp),
        "test_params": test_params,
        "test_command": test_command,
        "raw_output": raw_output
    }

    if test_params['test_type'] == "latency":
        return parse_ping_results(test_data)
    elif test_params['test_type'] in ["throughput", "jitter"]:
        return parse_iperf_results(test_data)


def run_test(test_params: dict):
    """
    Run a ping test based on the parameters in the input dictionary. The dictionary should contain the following keys:
    - id_number: a unique identifier for the test. Mandatory.
    - source: the source IP or hostname for the test. Used for constructing the test command (ie. local or SSH).
    Default is 'localhost'
    - destination: the destination IP or hostname for the test. Mandatory.
    - count: the number of pings to send (optional; default 10)
    - size: the size of the ping packet (optional; default 56 bytes)
    :param test_params: a dictionary containing the parameters for the test
    :return: a list containing the results of the test
    """
    id_number = test_params['id_number']  # this is a required field, so we can assume it's present
    source = test_params.get('source', 'localhost')  # if value was missing from CSV, assume 'localhost'
    destination = test_params['destination']  # required field
    # TODO: come up with a better way to handle the username. Probably need a separate config file with hosts/accounts
    username = "ash"  # temporarily hard-code this for now

    if test_params['test_type'] == "latency":
        size = test_params.get('size', 56)  # optional field; go for 56 byte packet size if not specified
        count = test_params.get('count', 10)  # optional field; set default of 10 pings if not specified
        # TODO: do something better for the interval later. Config file, or separate CSV field?
        interval = 0.2  # temporarily hard-code this for now
        test_command = f"ping -c {count} -i {interval} -s {size} {destination}"

    elif test_params['test_type'] == "throughput":
        size = test_params.get('size', None)  # mandatory for throughput tests - throw exception if missing
        if size is None:
            logger.error(f"Size parameter missing for test {id_number}. This field is required for throughput tests.")
            raise ValueError(
                f"Size parameter missing for test {id_number}. This field is required for throughput tests.")
        else:
            test_command = f"iperf3 -c {destination} -n {size} -4 --json"
    elif test_params['test_type'] == "jitter":
        test_command = f"iperf3 -c {destination} -u -4 --json"
    else:
        logger.error(f"Unknown test type '{test_params['test_type']}' for test {id_number}. Skipping test.")
        raise ValueError(f"Unknown test type '{test_params['test_type']}' for test {id_number}. Skipping test.")

    # If it's a test to execute remotely then we need to construct an SSH command to run the test on the remote host
    if source not in ["localhost", "127.0.0.1"]:
        test_command = f"ssh -n -o ConnectTimeout=2 {username}@{source} '{test_command}'"

    timestamp = datetime.datetime.now()
    logger.info(f"{timestamp}  Test #{id_number} initiated. Running command: {test_command}")

    try:
        # Execute the command and get the result.
        raw_output = subprocess.check_output(test_command, shell=True, stderr=subprocess.STDOUT).decode()

    except subprocess.CalledProcessError as e:
        t_stamp = datetime.datetime.now()
        logger.error(f"***************************************************************************************")
        logger.error(f"{t_stamp}  Test #{id_number} (command '{test_command}') failed. Full output of test:")
        logger.error(e.output.decode())
        logger.error(f"***************************************************************************************")

        # TODO: Fix this, this is the old code for dealing with ping failures. Need to genericise it so that it can
        #  handle any test type gracefully.
        # If something failed in the command, we'll set the RTT values to None
        min_rtt, avg_rtt, max_rtt, stddev_rtt = None, None, None, None
        return [id_number, min_rtt, avg_rtt, max_rtt, stddev_rtt, test_command]

    else:  # if the command didn't trigger a CalledProcessError, assume success and return the parsed results
        print(f"Trying to parse results for test {id_number} now...")
        p_results = parse_results(id_number=id_number, timestamp=timestamp, test_params=test_params,
                                  test_command=test_command, raw_output=raw_output)
        print(f"Parsed results: {p_results}")
        return p_results


def read_input_file(filename):
    # Read the input CSV file and return a list of dicts, each line being mapped to a dictionary, based on the
    # header row of the CSV file. The first character of the header row is "#" and this should be ignored when
    # constructing the first column's name.  Current header row = #id_number,test_type,destination,count,size

    with open(filename, 'r') as input_file:
        reader = csv.reader(input_file)
        header = next(reader)
        header = [h.lstrip('#') for h in header]
        data = []

        # Construct a dictionary from the row's data, converting empty CSV values to None.
        for row in reader:
            row_dict = {header[i]: value if value != "" else None for i, value in enumerate(row)}
            # Iterate over the dict and remove any key-value pairs where the value is None. This makes it easier to
            #  assign default values to missing test command parameters in the test-running function(s).
            row_dict = {k: v for k, v in row_dict.items() if v is not None}
            data.append(row_dict)

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
log_file = os.path.join(output_dir, f"{base_name}.log")  # FIXME: temporarily changed it to basename while debugging
output_file = os.path.join(output_dir, f"{out_basename}.json")
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

all_tests = read_input_file(input_csv)  # a list of dictionaries, each dict representing a test to be run

all_results = {
    "latency_tests": [],
    "throughput_tests": [],
    "jitter_tests": []
}

# TODO: Add feature which iterates over all_tests and makes a list of unique source hosts; then iterate through those
#  hosts, checking with shutil.which to ensure that iperf3 is installed. It would need to have the same code that checks
#  "is this host = me? (ie. localhost)" to know if it's excuting the shutil.which locally, or remotely via SSH.
#  Instead of hard-coding "iperf3" as what we're confirming, we would have a constant defined at the top of the code
#  "REQUIRED_TOOLS" or something, and we would iterate over that. We only need iperf3 at present but using a constant
#  makes it easier to add other test tools in future.

for test in all_tests:
    test_type = test['test_type']
    if test_type not in TEST_TYPES:  # see constant that is defined at top of code just after imports
        logger.error(f"Unknown test type '{test_type}' for test {test['id_number']}. Skipping test.")
        continue
    else:
        logger.debug(f"Test type: {test_type} will be run.")
        results = run_test(test)

        # Append the results to the appropriate list in all_results
        key_name = test_type + "_tests"
        all_results[key_name].append(results)

# Write the results to a JSON file
with open(output_file, 'w') as json_file:
    json.dump(all_results, json_file, indent=4)

logger.info(f"Tests ended at {datetime.datetime.now()}")
