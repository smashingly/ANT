
import json
import subprocess
import csv
import os
import datetime
import logging
import sys
import socket
import argparse
import configparser

TEST_TYPES = ["latency", "throughput", "jitter"]        # used in main code body loop


def parse_ping_results(test_data: dict):
    # This function parses the results of a ping test. It takes a dictionary as input, containing the following
    # keys: id_number, timestamp, test_params, test_command, and raw_output. It returns a dictionary containing the
    # parsed results, or None if the parsing fails.
    id_number = test_data['id_number']
    test_command = test_data['test_command']
    raw_output = test_data['raw_output']
    source = test_data['test_params']['source']
    dest = test_data['test_params']['destination']

    # We set these values to None so that any of the If statements that check for invalid ping result text can just
    #  do nothing (other than log an error message). This saves repeating the same 2 lines of code in the If statements
    #  that detect: A) no min/avg/max line; B) >1 min/avg/max line; and C) running on Windows OS.
    min_rtt, avg_rtt, max_rtt, stddev_rtt = None, None, None, None
    packets_txd, packets_rxd, packet_loss_percent = None, None, None

    # DEBUG CODE FOR TEST-BREAKING THE PING PARSER
    # raw_output = "min/avg/max\nmin/avg/max\nMultiple occurrences of min/avg/max/mdev"
    # raw_output = "Hello there\nThis is a test\nThere is no occurrence of min, avg, max etc"

    # Isolate out the line in the ping output that contains the summary results
    rtt_line = [line for line in raw_output.split('\n') if 'min/avg/max' in line]

    # Ensure that if raw_output has 0, or >1 lines containing 'min/avg/max', then we handle it gracefully. We log
    #  something, then do nothing else, because we've already set all the test result variables to None.
    if len(rtt_line) == 0:
        logger.error(f"Test ID {id_number}: No line found in ping output containing 'min/avg/max'. Skipping test. "
                     f"Full output of test:\n{raw_output}")
    elif len(rtt_line) > 1:     # This situation is extremely unlikely
        logger.error(f"Test ID {id_number}: Multiple lines found in ping output containing 'min/avg/max'. Skipping "
                     f"test. Full output of test:\n{raw_output}")
    else:
        # Keep the text between (but excluding) " = " and " ms"
        # Example ping output line: 'round-trip min/avg/max/stddev = 0.053/0.154/0.243/0.063 ms'
        rtt_data = rtt_line[0].replace(" ms", "").split('=')[1].strip()

        # rtt_data now looks something like this: '0.053/0.154/0.243/0.063' - so we split it by the '/'
        min_rtt, avg_rtt, max_rtt, stddev_rtt = rtt_data.split('/')


        # TODO: this isn't really effective; we need to check if the OS of the machine RUNNING THE TEST is Windows.
        #  Either ditch this 'if' clause, or implement parsing of Windows-style pings (better approach).
        if os.name == 'nt':         # Skip detection if this script is running on a Windows machine.
            logger.info(f"Test ID {id_number}: Running on Windows; packet count & loss not analysed.")
            success_msg_suffix = f"packet count & loss not analysed (running on Windows)."
        else:
            # Create a list of any lines in the output that have 'packet loss' in them. There should only be one.
            loss_lines = [line for line in raw_output.split('\n') if 'packet loss' in line]
            if len(loss_lines) == 0:
                logger.error(f"Test ID {id_number}: No line found in ping output containing 'packet loss'. Will record "
                             f"RTT results but not tx/rx/lost packets. Full output of test:\n{raw_output}")
                success_msg_suffix = f"packet count data not found in ping output."
            else:
                # Grab the transmitted packets, received packets, and % packet loss from loss_line. This will work
                # for MacOS and most Linux, but won't work on Windows as it uses different wording and symbols.
                # Example line 1 (MacOS, Linux): "10 packets transmitted, 10 packets received, 0.0% packet loss"
                # Example line 2 (some Linux): "10 packets transmitted, 10 received, 0.0% packet loss"
                loss_line = loss_lines[0]
                split_line = loss_line.split(', ')       # ['10 packets transmitted', '10 packets received', etc...]
                packets_txd = int(split_line[0].split(' ')[0])      # ['10', 'packets', 'transmitted'] -> '10' -> 10
                packets_rxd = int(split_line[1].split(' ')[0])         # ['10', 'packets', 'received'] -> '10' -> 10

                # It's hard to reliably parse loss% out of the string because some Linux OSes use slightly different
                # wording, or insert "+1 duplicates" in the middle of the string. So we calculate the loss ourselves.
                packet_loss_percent = round(((packets_txd - packets_rxd) / packets_txd) * 100, 4)
                success_msg_suffix = f"{packets_txd} / {packets_rxd} / {packet_loss_percent}%  (#tx/#rx/loss)"

        # Log output to the screen and to logfile. We do this inside the parse functions because we have easy access to
        #  the variables for the specific test type. This allows us to output short-form results in a one-line log entry.
        #  We could do this in run_test() but we'd need a block of if-logic that works out the test type then extracts
        #  the necessary key/value data from the results dict, then generates the appropriate message.
        # Note: we're using a separate print() statement because the logger will only display console messages if they're
        #  at WARNING or above severity, and it's inappropriate to log success using a WARNING/ERROR severity.
        success_msg = (f"Test ID {id_number} (src: '{source}', dst: '{dest}'): Success. Result: "
                       f"{min_rtt} / {avg_rtt} / {max_rtt} / {stddev_rtt} ms  (min/avg/max/*dev), " + success_msg_suffix)
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
        "packets_txd": packets_txd,
        "packets_rxd": packets_rxd,
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

    # TODO: ponder what could go wrong here, and think about error-handling / exception-handling. What if the
    #  JSON is malformed? What if the JSON is missing the expected keys?

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
    username = host_config.get(source, 'username')  # get this host's username from the host_config file

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
        return None

    else:  # if the command didn't trigger a CalledProcessError, assume success and return the parsed results
        p_results = parse_results(id_number=id_number, timestamp=timestamp, test_params=test_params,
                                  test_command=test_command, raw_output=raw_output)
        logger.debug(f"Test ID {id_number} parsed results: {p_results}")
        return p_results


def read_input_file(filename):
    # Read the input CSV file and return a list of dicts, each line being mapped to a dictionary, based on the
    # header row of the CSV file. The first character of the header row is "#" and this should be ignored when
    # constructing the first column's name.  Current header row = #id_number,test_type,destination,count,size

    # CSV file MUST have header row. Need to check this, and if it's not present, throw an error. We will do this
    # by checking whether the first character of header is = "#".  If it isn't, then we will log an error and halt.

    with open(filename, 'r') as input_file:
        reader = csv.reader(input_file)
        header = next(reader)                       # grab the first row of file (the header row)
        if not header[0].startswith("#"):
            logger.critical(f"Input file {filename} has no header row, or header row doesn't start with '#'. Halting execution.")
            exit(1)
        header = [h.lstrip('#') for h in header]    # remove any leading '#' characters found in header fields
        data = []

        # Construct a dictionary from the remaining reader rows, converting empty CSV values to None.
        for row in reader:
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


# Parse command-line arguments, derive output and log-file naming, and set up the logger

parser = argparse.ArgumentParser(description='Run network tests based on input CSV file.')
# Positional arguments
parser.add_argument('input_csv', help='Input CSV file')
parser.add_argument('output_directory', nargs='?', default='.',
                    help='Output directory (optional, default is current directory)')
# Optional arguments
parser.add_argument('-c', '--hostconfig', default='host_config.ini',
                    help='Override the default hosts config file (optional, default is host_config.ini)')

args = parser.parse_args()
input_csv = args.input_csv
output_dir = args.output_directory
host_config_file = args.hostconfig

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

"""########################### Start of logger setup and configuration ###########################
   *****  ABSOLUTELY MINIMISE THE AMOUNT OF CODE THAT COMES BEFORE THIS SECTION, AS LOGGING  *****
   *****  IS NOT RUNNING UNTIL AFTER THIS SECTION                                            *****"""
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

logger.info(f"Reading host configuration file {host_config_file}.")
host_config = configparser.ConfigParser()
host_config.read(host_config_file)

logger.debug("Reading input file and constructing test list.")
all_tests = read_input_file(input_csv)  # a list of dictionaries, each dict representing a test to be run
logger.debug(f"Read {len(all_tests)} rows in input file {input_csv}.")

# Extract all unique hostnames from all_tests
unique_hostnames = set()       # Using a set automatically prevents duplicates
for test in all_tests:
    unique_hostnames.add(test['source'])

# Make a list of all hostnames in the host_config file
all_test_hosts = [host_config[section]['hostname'] for section in host_config.sections()]

# Check if each unique hostname in all_tests is in the host_config file. If not, log an error and halt execution.
missing_hostnames = [hostname for hostname in unique_hostnames if hostname not in all_test_hosts]
if missing_hostnames:
    logger.critical(f"One or more source hostnames in {input_csv} are missing from {host_config_file}: {missing_hostnames}")
    exit(1)  # Halt execution with error code (non-zero)
else:
    logger.info(f"All source hostnames in {input_csv} are present in {host_config_file}.")

# initialise the all_results dictionary with its high-level keys
all_results = {
    "latency_tests": [],
    "throughput_tests": [],
    "jitter_tests": []
}

# Do the actual work - iterate over all_tests and run each
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

        # if run_test failed (eg. SSH failure, test command failure, etc) then results will be None
        if results is not None or results is None:
            # Append the results to the appropriate list in all_results
            key_name = test_type + "_tests"
            all_results[key_name].append(results)

# Write the results to a JSON file
logger.info(f"All tests have been iterated over. Writing results to {output_file}.")
with open(output_file, 'w') as json_file:
    json.dump(all_results, json_file, indent=4)

logger.info(f"{'*' * 20} End of script execution {'*' * 20}")
