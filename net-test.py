
import json
import subprocess
import csv
import os
import datetime
import logging.handlers
import logging
import socket
import argparse
import configparser

# This is the version number of the script. We are using SemVer (Semantic Versioning) system. The version number
# consists of three numbers separated by dots. The first number is the major version, the second number is the minor
# version, and the third number is the patch version. The major version is incremented when there are breaking changes.
# The minor version is incremented when new features are added in a backwards-compatible manner. The patch version is
# incremented when backwards-compatible bug fixes are made. The version number is stored as a string, and is used in
# the --version argument of the argparse.ArgumentParser() object. See https://semver.org/ for more details.
VERSION = "2.0.0"

# Default directory locations. These defaults are assigned to variables during argpase setup in get_cmdline_args().
DEFAULT_LOG_DIR = "./"
DEFAULT_RESULTS_DIR = "./"
DEFAULT_INPUT_CSV = "./net-test.csv"
DEFAULT_HOST_CONFIG = "./host_config.ini"
# TODO: think about adding default locations for the remaining in/out files, eg. results, etc

# Other constants that are unlikely to need changing:
LOGGING_LEVEL = logging.INFO        # can be overridden using the -V/--verbose argument
BASE_NAME = "net-test"

# Constants that users/devs may need to play with and change:
TEST_TYPES = ["latency", "throughput", "jitter"]        # used in main code body loop
PING_INTERVAL = 0.2  # seconds between pings, used across all latency tests. Used in run_tests().


def get_cmdline_args() -> argparse.Namespace:
    """
    Parse command-line arguments using argparse. This function is called at the start of the script, and returns the
    parsed arguments. The argparse.ArgumentParser object is created here, and the arguments are defined. The function
    then returns the parsed arguments to the main code body, where they can be used to set up the logger, and to
    determine input/output file paths.  The reason for wrapping this into a function is simply for main body code
    readability - it makes the main code body easier to read, as the argparse setup is quite verbose.
    :return: parsed arguments (type: argparse.Namespace)
    """

    # TODO: play with the 'epilog' arg to argparse.ArgumentParser() to make the help text more useful. Consider adding
    #   an epilog that explains the CSV format, and/or how to use the host_config file, and the permissions required
    #   for the log and results directories.
    # Parse command-line arguments, derive output and log-file naming, and set up the logger
    parser = argparse.ArgumentParser(
        description=f"Net-test (ANT) version {VERSION}.\n"
                    f"Runs network tests on local & remote hosts based on input CSV file.")

    # Configure the command-line arguments
    parser.add_argument("-i", "--input", default=DEFAULT_INPUT_CSV, metavar="<input file>",
                        help=f"Input CSV file containing test parameters. (default is '{DEFAULT_INPUT_CSV}'). "
                             f"The user account executing this script must have read permissions to this file.")

    parser.add_argument("-o", "--output", default=DEFAULT_RESULTS_DIR, metavar="<output dir>",
                        help=f"Results output directory (default is '{DEFAULT_LOG_DIR}'). "
                             "The user account executing this script must have write permissions to this folder.")

    parser.add_argument("-c", "--host-config", default=DEFAULT_HOST_CONFIG, metavar="<host config>",
                        help=f"Override the default hosts config file (optional, default is {DEFAULT_HOST_CONFIG})")

    parser.add_argument("-l", "--log-dir", default=DEFAULT_LOG_DIR, metavar="<log dir>",
                        help=f"Log file output directory (default is '{DEFAULT_LOG_DIR}'). "
                             f"The user account executing this script must have read + write permissions to this folder.")

    parser.add_argument("-v", "--version", action="version", version=f"%(prog)s version {VERSION}",
                        help="Display the version number and exit.")
    # TODO: evaluate the level of most INFO logging messages, and decide whether --verbose could also set the console log to
    #  DEBUG or INFO level. This would be useful for troubleshooting, but it would also make the console output very noisy.
    #  But that might also point to some INFO level messages needing to be downgraded to DEBUG. Or we could have separate
    #  flags for console and file logging levels.  Or levels of verbosity, eg. -v for file DEBUG, -vv for file DEBUG plus
    #  console INFO, -vvv for file DEBUG plus console DEBUG. Don't overthink it though.
    parser.add_argument('-V', '--verbose', action='store_true',
                        help='Enable debug logging (applies to log file only)')
    # TODO: could add a parameter "--help-csv" which explains the CSV format. It would call a separate function where the
    #  help text is defined, to avoid cluttering the main code body.

    return parser.parse_args()


def setup_logging(name, log_level, file_path):
    """
    Set up the logging for the script. This function is called at the start of the script, and returns a logger object
    which can be used to log messages. The logger object is returned so that it can be used in the main code body,
    otherwise it would only be accessible from within this function.
    :param name: unique name for the logger (fairly arbitrary but must be used consistently throughout the program)
    :param log_level: the logging level for the logger. Normally set to logging.INFO for production use, and set to
    logging.DEBUG for additional output during development & troubleshooting.
    :param file_path: path & filename of the log file to create.
    :return: logger object
    """
    logger = logging.getLogger(name)    # Create a custom logger that we can use throughout the program
    logger.setLevel(log_level)          # set to logging.DEBUG if you want additional output during development
    # Create handlers. Naming convention: c = console, f = file
    c_handler = logging.StreamHandler()
    f_handler = logging.handlers.TimedRotatingFileHandler(
        filename=file_path, when="D", interval=1, backupCount=60)  # rotate daily, keep 60 days worth of logs
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

    print(f"Logging to {file_path}")
    return logger


def check_dir_and_permissions(dir_path, description ="Directory", mode = os.W_OK, no_logger=False):
    """
    Check if a directory exists and that it has the specified permissions for the user under which this program is
    executed. If it doesn't exist, or if it's not writable, log an error and halt
    :param dir_path: directory to be checked
    :param description: allows the error message to be more meaningful, eg. "Log directory <...> does not exist"
    :param no_logger: set to True if no logger is set up when this function is called (ie. when checking log_dir).
    If set to True, output will be sent to console instead, using print() instead of logger.error()
    :param mode: access mode (eg. os.W_OK, os.R_OK, os.X_OK, etc), see os.access() for more details
    """
    logging_enabled = not no_logger      # setting separate boolean purely to make the code more readable

    if logging_enabled:   # if logging is enabled
        logger = logging.getLogger(BASE_NAME)
        logger.debug(f"Checking for existence and permissions of {description.lower()} {results_dir}.")

    # We OR both of these tests, because either of these tests will fail if the file doesn't exist or the user doesn't
    #  have the required permissions. This is because you can't check for a file's existence if you don't have Read
    #  permissions for that file, and you can't check a file's permissions if it doesn't exist. If you have separate
    #  if/elif statements for existence and permission checks, whichever test is the first, will fail regardless of
    #  whether it's an existence issue or a permissions issue.
    #  The easy fix is to combine the tests with an OR, and have a consolidated but less-precise error message.
    if not os.access(dir_path, mode) or not os.path.exists(dir_path):
        message = (f"{description} {dir_path} does not exist, or doesn't have the required permissions for this user.")
        if logging_enabled:
            logger.critical(message)
        else:
            print(f"\nFATAL ERROR: {message}\n" + " " * 13 +
                  f"Run {BASE_NAME}.py with the '--help' option for information on usage & permissions.\n")
        exit(1)

    if logging_enabled:
        logger.debug(f"{description} {dir_path} exists and has the correct permissions.")


def parse_ping_results(test_data: dict):
    # This function parses the results of a ping test. It takes a dictionary as input, containing the following
    # keys: id_number, timestamp, test_params, test_command, and raw_output. It returns a dictionary containing the
    # parsed results, or None if the parsing fails.
    id_number = test_data['id_number']
    test_command = test_data['test_command']
    raw_output = test_data['raw_output']
    source = test_data['test_params']['source']
    dest = test_data['test_params']['destination']

    # We set these values to None here, to avoid repeating the same 2 lines of code in the If statements
    #  that detect: A) no min/avg/max line; B) >1 min/avg/max line; and C) running on Windows OS.
    min_rtt, avg_rtt, max_rtt, stddev_rtt = None, None, None, None
    packets_txd, packets_rxd, packet_loss_percent = None, None, None

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
        # Keep the text between " = " and " ms"
        # Example ping output line: 'round-trip min/avg/max/stddev = 0.053/0.154/0.243/0.063 ms'
        rtt_data = rtt_line[0].replace(" ms", "").split('=')[1].strip()

        # rtt_data now looks something like this: '0.053/0.154/0.243/0.063' - so we split it by the '/'
        min_rtt, avg_rtt, max_rtt, stddev_rtt = rtt_data.split('/')

        # Create a list of any lines in the output that have 'packet loss' in them. There should only be one.
        loss_lines = [line for line in raw_output.split('\n') if 'packet loss' in line]
        if len(loss_lines) == 0:
            logger.error(f"Test ID {id_number}: No line found in ping output containing 'packet loss'. Will record "
                         f"RTT results but not tx/rx/lost packets. Full output of test:\n{raw_output}")
            success_msg_suffix = f"packet count data not found in ping output."
        else:
            # Grab the transmitted packets, received packets, and % packet loss from loss_line. This will work
            # for MacOS and most Linux OSes.
            # Example line 1 (MacOS, Linux): "10 packets transmitted, 10 packets received, 0.0% packet loss"
            # Example line 2 (some Linux): "10 packets transmitted, 10 received, 0.0% packet loss"
            loss_line = loss_lines[0]
            split_line = loss_line.split(', ')       # ['10 packets transmitted', '10 packets received', etc...]
            packets_txd = int(split_line[0].split(' ')[0])      # ['10', 'packets', 'transmitted'] -> '10' -> 10
            packets_rxd = int(split_line[1].split(' ')[0])         # ['10', 'packets', 'received'] -> '10' -> 10

            # It's hard to reliably parse loss% out of the string because some Linux OSes use slightly different
            # wording, or insert "+1 duplicates" in the middle of the string. So we calculate the loss ourselves.
            packet_loss_percent = round(((packets_txd - packets_rxd) / packets_txd) * 100, 4)
            success_msg_suffix = f"{packets_txd}/{packets_rxd}/{packet_loss_percent}% (#tx/#rx/loss)"

        # Log output to the screen and to logfile. We do this inside the parse functions because we have easy access to
        #  the variables for the specific test type. This allows us to output short-form results in a one-line log entry.
        #  We could do this in run_test() but we'd need a block of if-logic that works out the test type then extracts
        #  the necessary key/value data from the results dict, then generates the appropriate message.
        # Note: we're using a separate print() statement because the logger will only display console messages if they're
        #  at WARNING or above severity, and it's inappropriate to log success using a WARNING/ERROR severity.
        success_msg = (f"Test ID {id_number} (src: '{source}', dst: '{dest}', ping): Success. Result: "
                       f"{min_rtt}/{avg_rtt}/{max_rtt}/{stddev_rtt} ms (min/avg/max/*dev), " + success_msg_suffix)
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
    source = test_data['test_params']['source']
    dest = test_data['test_params']['destination']

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
    msg = f"Test ID {id_number} (src: '{source}', dst: '{dest}', {test_type}): Success. Result: {short_form_results}"
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
        interval = PING_INTERVAL
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
    # Read the input CSV file and return a list of dicts, each line being mapped to a dictionary, based on hard-coded
    # column names. The first row of the file MUST be a comment line starting with #. This can be used to explain to
    # end users what the column names are, but note that the code doesn't use the header row to map column names to
    # the data. It uses hard-coded column names. This is to prevent the end user from making mistakes in the header
    # row, and to prevent the end user from changing the column names without changing the code. Any other rows that
    # start with a # character will be ignored.  This makes it easy to comment out specific tests in the input file.

    column_headers = ['id_number', 'test_type', 'source', 'destination', 'count', 'size']

    with open(filename, 'r') as input_file:
        reader = csv.reader(input_file)
        file_header = next(reader)                       # grab the first row of file (the header row)
        if not file_header[0].startswith("#"):
            logger.critical(f"Input file {filename} must have a first row that starts with '#'. Halting execution.")
            exit(1)

        data = []

        # Construct a dictionary from the remaining reader rows, converting empty CSV values to None.
        for row in reader:
            if row[0].startswith("#"):
                # Skip rows starting with a hash character, but log this to the screen and to the logfile.
                logger.warning(f"Skipping row in input file starting with a '#' character: {row}")
                continue
            else:
                row_dict = {column_headers[i]: value if value != "" else None for i, value in enumerate(row)}
                # Iterate over the dict and remove any key-value pairs where the value is None. This makes it easier to
                #  assign default values to missing test command parameters in the test-running function(s).
                row_dict = {k: v for k, v in row_dict.items() if v is not None}
                data.append(row_dict)

    return data


# This script will not work under Windows, for a couple of reasons. Firstly, the output of the ping command is vastly
# different under Windows. Secondly, the command-line options for the Windows ping command are completely different.
if os.name == 'nt':
    print(f"FATAL: This script will not run on Windows systems. It is designed for execution on Unix-based "
          f"operating systems. Halting execution.")
    exit(1)

# Record the start-time of program execution so we can output the duration at the end of the script
execution_start_time = datetime.datetime.now()

# Call my custom function that wraps all the argparse stuff, to keep the main code body tidy.
args = get_cmdline_args()

# Set the log level based on the --verbose flag
if args.verbose:
    LOGGING_LEVEL = logging.DEBUG
input_csv = args.input
results_dir = args.output        # Where the JSON file will be output to
host_config_file = args.host_config
log_dir = args.log_dir

# This must be checked *before* logging is enabled; Other directories/files (eg. results_dir) are checked later.
check_dir_and_permissions(dir_path=log_dir, description="Log directory", mode=os.W_OK | os.R_OK, no_logger=True)

# Append yyyymmddhhmmss timestamping to the output filename, eg. net-test_2024-03-19_125400.json
output_filename = f"{BASE_NAME}_{datetime.datetime.now().strftime('%Y-%m-%d_%H%M%S')}.json"
output_filepath = os.path.join(results_dir, f"{output_filename}")
# TODO: for output files, we may want to implement a clean-up that runs on any output files that are older than
#  a certain age, to avoid filling up the disk with JSON files.

"""
########################### Start of logger setup and configuration ###########################
*****  MINIMISE THE AMOUNT OF CODE THAT COMES BEFORE LOGGER SETUP, AS LOGGING WILL NOT BE *****
*****  RUNNING UNTIL AFTER THIS SECTION!                                                  *****"""
log_file = os.path.join(log_dir, f"{BASE_NAME}.log")
logger_name = BASE_NAME
logger = setup_logging(name=logger_name, log_level=LOGGING_LEVEL, file_path=log_file)
"""######################### End of logger setup and configuration #########################"""

logger.info(f"{'*' * 20} Initial startup {'*' * 20}")
logger.info(f"Input CSV file: {input_csv}. Output file: {output_filepath}")

# Check that our input and output directories exist and have the correct permissions
check_dir_and_permissions(dir_path=results_dir, description="Results directory", mode=os.W_OK)
check_dir_and_permissions(dir_path=input_csv, description="Input file", mode=os.R_OK)

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
# TODO: add validation of the config file, making sure that the relevant fields are defined and not empty etc

logger.debug("Reading input file and constructing test list.")
all_tests = read_input_file(input_csv)  # a list of dictionaries, each dict representing a test to be run
# TODO: consider doing an initial validation of all the imported test data to ensure that it's valid. Eg. check that
#  the test_type is valid, that the source and destination are valid, sources exist in the host config file, etc.
logger.debug(f"Read {len(all_tests)} rows in input file {input_csv}.")

# Extract all unique hostnames from all_tests
unique_hostnames = set()       # Using a set automatically prevents duplicates, as sets don't allow them
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
logger.info(f"All tests have been iterated over. Writing results to {output_filepath}.")
with open(output_filepath, 'w') as json_file:
    json.dump(all_results, json_file, indent=4)

execution_duration = datetime.datetime.now() - execution_start_time
# Create a string that expresses the duration in a human-readable format, hh:mm:ss
execution_duration_str = str(execution_duration).split('.')[0]  # remove the microseconds from the string

logger.info(f"{'*' * 20} End of script execution. Run-time was {execution_duration_str} {'*' * 20}")
