import json
import subprocess
import csv
import os
import datetime
import logging
import sys
import socket

TEST_TYPES = ["latency", "throughput", "jitter"]


def parse_ping_results(test_data: dict):
    id_number = test_data['id_number']
    test_command = test_data['test_command']
    raw_output = test_data['raw_output']

    # Isolate out the line in the ping output that contains the summary results
    rtt_line = [line for line in raw_output.split('\n') if 'min/avg/max' in line]

    # Keep the text between (but excluding) " = " and " ms"
    # Example ping output line: 'round-trip min/avg/max/stddev = 0.053/0.154/0.243/0.063 ms'
    rtt_data = rtt_line[0].replace(" ms", "").split('=')[1].strip()

    # rtt_data now looks something like this: '0.053/0.154/0.243/0.063' - so we split it by the '/'
    min_rtt, avg_rtt, max_rtt, stddev_rtt = rtt_data.split('/')

    # Isolate out the line that contains the packet loss data. Note: there's only one line that matches this, but
    # because we're using a list comprehension (in square brackets) we will get a list with one item in it.
    loss_line = [line for line in raw_output.split('\n') if 'packet loss' in line]

    # Grab the transmitted packets, received packets, and % packet loss from loss_line. This will work for
    # MacOS and most Linux, but won't work on Windows as it has vastly different wording.
    # Example line 1 (MacOS, Linux): "10 packets transmitted, 10 packets received, 0.0% packet loss"
    # Example line 2 (some Linux): "10 packets transmitted, 10 received, 0.0% packet loss"

    split_line = loss_line[0].split(', ')    # ['10 packets transmitted', '10 packets received', etc...]
    packets_transmitted = int(split_line[0].split(' ')[0])      # ['10', 'packets', 'transmitted'] -> '10' -> 10
    packets_received = int(split_line[1].split(' ')[0])         # ['10', 'packets', 'received'] -> '10' -> 10
    # it's too hard to parse this out of the string, so let's calculate the loss percentage ourselves:
    packet_loss_percent = round(((packets_transmitted - packets_received) / packets_transmitted) * 100, 4)

    # Log output to the screen and to logfile. We do this inside the parse functions because we have easy access to
    #  the variables for the specific test type. This allows us to output short-form results in a one-line log entry.
    #  We could do this in run_test() but we'd need a block of if-logic that works out the test type then extracts
    #  the necessary key/value data from the results dict, then generates the appropriate message.
    # Note: we're using a separate print() statement because the logger will only display console messages if they're
    #  at ERROR or above severity level, and it's inappropriate to log success using an ERROR level. And we do not
    #  want to change the console logging level to INFO as this would flood the user with a lot of unnecessary info.
    short_results = f""
    success_msg = (f"Test ID {id_number}: Success. Result: "
                   f"{min_rtt} / {avg_rtt} / {max_rtt} / {stddev_rtt} ms  (min/avg/max/*dev), "
                   f"{packets_transmitted} / {packets_received} / {packet_loss_percent}%  (#tx/#rx/%loss)")
    print(success_msg)
    logger.info(success_msg)

    return {
        "id_number": id_number,
        "timestamp": str(test_data['timestamp']),
        "source": test_data['test_params']['source'],
        "destination": test_data['test_params']['destination'],
        "min_rtt": min_rtt,
        "avg_rtt": avg_rtt,
        "max_rtt": max_rtt,
        "stddev_rtt": stddev_rtt,
        "packets_transmitted": packets_transmitted,
        "packets_received": packets_received,
        "packet_loss_percent": packet_loss_percent,
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
    #  NOTE: We use a separate print() statement for the console output, because the logger will only display console
    #  messages at WARNING level or above, so we can't use one logger.info() call to convey success to the console.
    msg = f"Test ID {id_number}: Success. Result: {short_form_results}"
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
    Run a test based on the parameters in the input dictionary. The dictionary should contain the following keys:
    - id_number: a unique identifier for the test. Mandatory.
    - source: the source IP or hostname for the test. Used for constructing the test command (ie. local or SSH).
    Default is 'localhost' if not supplied.
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

    # Get the current machine's hostname, FQDN and name-lookup the IP from the hostname. The user should not ever be
    #  putting an IP address into the 'source' field, but in case they do, we'll try to handle it gracefully. Also
    #  note that on some systems (particularly home networks) gethostname() will include ".local" or ".gateway".
    if source in [my_hostname, my_fqdn, my_ip_addr, "localhost", "127.0.0.1"]:
        # Run the test locally - do nothing, just log the answer
        logger.info(f"Test ID {id_number} source '{source}' matches local machine details. Test will be run locally.")
    else:
        logger.info(f"Test ID {id_number} source '{source}' does not match local machine. Constructing SSH remote command.")
        test_command = f"ssh -n -o ConnectTimeout=2 {username}@{source} '{test_command}'"

    # this timestamp records the test start time, so we grab it here just before the test is executed
    timestamp = datetime.datetime.now()
    logger.info(f"Test ID {id_number} initiated. Running command: {test_command}")

    try:
        # Execute the command and get the result.
        raw_output = subprocess.check_output(test_command, shell=True, stderr=subprocess.STDOUT).decode()

    except subprocess.CalledProcessError as e:
        logger.error(f"Test failure for test ID {id_number} (command '{test_command}'). "
                     f"Full output of test: {e.output.decode()}")

        # TODO: Fix this, this is the old code for dealing with ping failures. Need to genericise it so that it can
        #  handle any test type gracefully.
        return None
        # If something failed in the command, we'll set the RTT values to None
        # min_rtt, avg_rtt, max_rtt, stddev_rtt = None, None, None, None
        # return [id_number, min_rtt, avg_rtt, max_rtt, stddev_rtt, test_command]

    else:  # if the command didn't trigger a CalledProcessError, assume success and return the parsed results
        p_results = parse_results(id_number=id_number, timestamp=timestamp, test_params=test_params,
                                  test_command=test_command, raw_output=raw_output)
        logger.debug(f"Test ID {id_number} parsed results: {p_results}")
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
            print(row)
            print(type(row))
            if row[0].startswith("#"):
                # Skip rows starting with a hash character, but log this to the screen and to the logfile.
                logger.warning(f"Skipping row in input file starting with a '#' character: {row}")
                continue
            else:
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
logger.setLevel(logging.INFO)  # set to logging.DEBUG for additional output during development

# Create handlers. Naming convention: c = console, f = file
c_handler = logging.StreamHandler()
f_handler = logging.FileHandler(log_file)
c_handler.setLevel(logging.WARNING)  # determines the error-level (or above) that will be sent to console
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

logger.info(f"{'*' * 20} Initial startup {'*' * 20}")
logger.info(f"Input CSV file: {input_csv}. Output file: {output_file}")

# Get the local machine's hostname, FQDN and IP address. This is used in the test-run loop to determine if the test
#  should be run locally or via SSH. We also use this to log the local machine's details at the start of the script.
logger.debug("Getting local machine's hostname, FQDN and IP address.")
my_hostname = socket.gethostname().lower().split('.')[0]  # Extract the part before the first dot
my_fqdn = socket.getfqdn().lower()
my_ip_addr = socket.gethostbyname(my_hostname)

# The wording of this log entry is carefully chosen, to make it clear that the my_ip_addr is not pulled from
#  the NIC or OS, it's derived by performing a lookup on my_hostname, which will use OS DNS settings or /etc/hosts.
logger.info(f"My hostname: {my_hostname}. My FQDN: {my_fqdn}. DNS resolves {my_hostname} to {my_ip_addr}.")

logger.debug("Reading input file and constructing test list.")
all_tests = read_input_file(input_csv)  # a list of dictionaries, each dict representing a test to be run
logger.debug(f"Read {len(all_tests)} rows in input file {input_csv}.")

# initialise the all_results dictionary with its high-level keys - these are lists of results of a given test type
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

# Do the actual work - iterate over all tests and run them
for test in all_tests:
    id_number = test['id_number']
    test_type = test['test_type']

    # Check test_type's validity (see constant, declared just after the import statements)
    if test_type not in TEST_TYPES:
        logger.warning(f"Unknown test type '{test_type}' for test {id_number}. Skipping test.")
        continue
    else:
        logger.debug(f"Test ID {id_number} of type {test_type} will be run.")
        results = run_test(test)

        # Append the results to the appropriate list in all_results
        key_name = test_type + "_tests"
        all_results[key_name].append(results)

# Write the results to a JSON file
with open(output_file, 'w') as json_file:
    json.dump(all_results, json_file, indent=4)

logger.info(f"{'*' * 20} End of script execution {'*' * 20}")
