import json
import subprocess
import csv
import os
from datetime import datetime, timedelta
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
VERSION = "2.6.4"

# Default directory locations. These defaults are assigned to variables during argpase setup in get_cmdline_args().
DEFAULT_LOG_DIR = "./"
DEFAULT_RESULTS_DIR = "./"
DEFAULT_INPUT_CSV = "./net-test.csv"
DEFAULT_HOST_CONFIG = "./host_config.ini"
CSV_COLUMNS = ['id_number', 'test_type', 'source', 'destination', 'count', 'size']

# Other constants that are unlikely to need changing:
BASE_NAME = "net-test"
LOGGER_NAME = "net-test"
LOGGING_LEVEL = logging.INFO  # can be overridden using the -V/--verbose argument

# Constants that users/devs may need to play with and change:
TEST_TYPES = ["latency", "throughput", "jitter"]  # used in main code body loop
DEFAULT_PING_INTERVAL = 0.2  # seconds between pings, used across all latency tests. Used in run_tests().


def get_cmdline_args() -> argparse.Namespace:
    """
    Parse command-line arguments using argparse. This function is called at the start of the script, and returns the
    parsed arguments. The argparse.ArgumentParser object is created here, and the arguments are defined. The function
    then returns the parsed arguments to the main code body, where they can be used to set up the logger, and to
    determine input/output file paths.  The reason for wrapping this into a function is simply for main body code
    readability - it makes the main code body easier to read, as the argparse setup is quite verbose.
    :return: parsed arguments (type: argparse.Namespace)
    """

    # Parse command-line arguments, derive output and log-file naming, and set up the logger
    parser = argparse.ArgumentParser(
        description=f"Net-test (ANT) version {VERSION}.\n"
                    f"Runs network tests on local & remote hosts based on input CSV file.")

    # Configure the command-line arguments
    parser.add_argument("-i", "--input", metavar="<input file>", required=True,
                        help=f"Input CSV file containing test parameters. Mandatory argument. "
                             f"Executing user account must have read permissions.")

    parser.add_argument("-o", "--output", default=DEFAULT_RESULTS_DIR, metavar="<output dir>",
                        help=f"Results output directory. Optional argument (defaults to '{DEFAULT_RESULTS_DIR}'). "
                             "Executing user account must have read + write permissions for this directory.")

    parser.add_argument("-c", "--host-config", default=DEFAULT_HOST_CONFIG, metavar="<host config>",
                        help=f"Override the default hosts config file. Optional argument "
                             f"(defaults to {DEFAULT_HOST_CONFIG}). Executing user account must have read permissions.")

    parser.add_argument("-l", "--log-dir", default=DEFAULT_LOG_DIR, metavar="<log dir>",
                        help=f"Log file output directory. Optional argument (defaults to '{DEFAULT_LOG_DIR}'). "
                             f"Executing user account must have read + write permissions to this directory.")

    parser.add_argument("--max-age", nargs='?', const=60, type=int, default=None, metavar="<days>",
                        help="Optional. Delete any JSON results files with an age greater than <days> (calculated "
                             "from file modification date). If --max-age is used without <days> then a default of "
                             "60 days applies.")

    parser.add_argument("-t", "--ping-interval", default=DEFAULT_PING_INTERVAL, metavar="<interval>",
                        help=f"Interval between pings in seconds. Optional argument (defaults to "
                             f"{DEFAULT_PING_INTERVAL}). Used in ping/latency tests ONLY.")

    parser.add_argument("-v", "--version", action="version", version=f"%(prog)s version {VERSION}",
                        help="Display the version number and exit.")
    parser.add_argument('-V', '--verbose', action='store_true',
                        help='Enable debug logging (applies to log file only)')

    return parser.parse_args()


def setup_logging(name, log_level, file_path) -> logging.Logger:
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
    logger = logging.getLogger(name)  # Create a custom logger that we can use throughout the program
    logger.setLevel(log_level)  # set to logging.DEBUG if you want additional output during development
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

    return logger


def check_dir_and_permissions(dir_path, description="Directory", mode=os.W_OK):
    """
    Check if a directory exists and that it has the specified permissions for the user under which this program is
    executed. If it doesn't exist, or if it's not writable, log an error and halt. The function automatically checks
    if logging has been set up yet, and if it hasn't, then it outputs any error messages using print() instead of
    logger.error(). This is because the first call to this function is to check the log_file directory, and at that
    stage logging hasn't yet been set up.
    :param dir_path: directory to be checked
    :param description: allows the error message to be more meaningful, eg. "Log directory <...> does not exist"
    :param mode: access mode (eg. os.W_OK, os.R_OK, os.X_OK, etc), see os.access() for more details
    """
    logging_enabled = True if LOGGER_NAME in logging.Logger.manager.loggerDict else False

    if logging_enabled:  # if logging is enabled
        # logger = logging.getLogger(LOGGER_NAME)
        logger.debug(f"Checking for existence and permissions of {description.lower()} '{dir_path}'.")

    # We OR both of these tests, because either of these tests will fail if the file doesn't exist or the user doesn't
    #  have the required permissions. This is because you can't check for a file's existence if you don't have Read
    #  permissions for that file, and you can't check a file's permissions if it doesn't exist. If you have separate
    #  if/elif statements for existence and permission checks, whichever test is the first, will fail regardless of
    #  whether it's an existence issue or a permissions issue.
    #  The easy fix is to combine the tests with an OR, and have a consolidated but less-precise error message.
    if not os.access(dir_path, mode) or not os.path.exists(dir_path):
        message = f"{description} {dir_path} does not exist, or doesn't have the required permissions for this user."
        if logging_enabled:
            logger.critical(message)
        else:
            print(f"\nFATAL ERROR: {message}\n" + " " * 13 +
                  f"Run {BASE_NAME}.py with the '--help' option for information on usage & permissions.\n")
        exit(1)

    if logging_enabled:
        logger.debug(f"{description} {dir_path} exists and has the correct permissions.")


def read_input_file(filename) -> list:
    """
    Read the input CSV file and return a list of dicts, each line being mapped to a dict representing 1 test. The
    first row of the file MUST be a comment line starting with #. This can be used to convey to end users what
    the column names are, but note that it is NOT used by the code to map column names to the data. We use
    hard-coded column names, to protecdt against end-users from breaking the code by misspelling column names.
    Any other rows that start with a # character will be ignored. This makes it possible to add more comments to the
    CSV file, or to comment out specific tests.
    :param filename: the name of the input CSV file
    :return: a list of dictionaries, each representing a test to be run
    """

    logger.debug(f"Reading input file {filename}.")
    csv_line_num = 1
    column_headers = CSV_COLUMNS

    with open(filename, 'r') as input_file:
        reader = csv.reader(input_file)
        file_header = next(reader)  # grab the first row of file (the header row)
        if not file_header[0].startswith("#"):
            logger.critical(f"Input file {filename} must have a first row that starts with '#'. Halting execution.")
            exit(1)

        data = []

        # Construct a dictionary from the remaining reader rows, converting empty CSV values to None.
        for row in reader:
            csv_line_num += 1
            if row[0].startswith("#"):
                # Skip rows starting with a hash character, but log this to the screen and to the logfile.
                logger.warning(f"Skipping line {csv_line_num} in input file because it starts with a '#' character.")
                logger.debug(f"\tContent of skipped line {csv_line_num}: {row}")
                continue
            else:
                row_dict: dict[str, any]    # suppresses IDE int -> str warning when csv_line_num is added to the dict
                # We use .strip() on 'value' in case users include a space after the comma in the CSV file.
                row_dict = {column_headers[i]: value.strip() if value != "" else None for i, value in enumerate(row)}
                # Iterate over the dict and remove any key-value pairs where the value is None. This makes it easier to
                #  assign default values to missing test command parameters in the test-running function(s).
                row_dict = {k: v for k, v in row_dict.items() if v is not None}
                row_dict['csv_line_num'] = csv_line_num  # add a key to the dict to store the CSV line number
                data.append(row_dict)

    logger.debug(f"Read {csv_line_num} lines from input file {filename}.")
    return data


def host_config_validated_ok(tests: list) -> bool:
    """
    Checks that each unique source hostname in the input CSV file has a corresponding entry in the host_config file.
    :param tests: pass alL_tests from the main body as this argument
    :return: True if all source hostnames in the input CSV file are present in the host_config file, False otherwise.
    """
    unique_hostnames = set()  # Using a set automatically prevents duplicates, as sets don't allow them
    for t in tests:
        unique_hostnames.add(t['source'])
    logger.debug(f"Found {len(unique_hostnames)} unique source hostnames in input CSV file.")
    # Make a list of all hostnames in the host_config file
    all_test_hosts = [host_config[section]['hostname'] for section in host_config.sections()]
    logger.debug(f"Found {len(all_test_hosts)} source hostnames in host config file.")
    # Check if each unique test source host in the CSV has an entry in the host_config file. If not, then quit.
    missing_hostnames = [hostname for hostname in unique_hostnames if hostname not in all_test_hosts]
    if missing_hostnames:
        logger.error(
            f"One or more source hostnames in input CSV are missing from host config file: {missing_hostnames}")
        return False
    else:
        logger.info(f"All source hostnames in {input_csv} are present in {host_config_file}.")
        return True


def test_data_validated_ok(test_data: list) -> bool:
    """
    Validate the test data read from the input CSV file. This function will run through the test data and validate
    things like test_type, source, destination, etc. It will log any specific errors found, along with the offending
    line and field, and return False if any errors are found. If no errors are found, it will return True. NOTE: we
    already test for the header-row (starting with '#') in read_input_file(), so we don't need to check for it here.
    This is not deep validation, i.e. we aren't checking for valid hostnames or IP addresses.
    :param test_data: a list of dictionaries, each representing a test to be run (i.e. all_tests in main code body)
    :return: True if all tests are valid, False if any errors are found
    """
    logger.debug("Validating test data in input file.")
    for item in test_data:
        csv_line_num = item.get('csv_line_num', None)
        id_num = item.get('id_number', None)
        t_type = item.get('test_type', None)
        source = item.get('source', None)
        destination = item.get('destination', None)
        size = item.get('size', None)

        # Check that all mandatory fields are present and non-empty
        if not all([id_num, t_type, source, destination]):
            logger.error(f"CSV line {csv_line_num}: One or more mandatory fields "
                         f"(id_number, test_type, source, destination) are missing or empty.")
            return False

        # Check that the test type is valid
        if t_type not in TEST_TYPES:
            logger.error(f"CSV line {csv_line_num}: Invalid test type '{t_type}'. It must be one of: {TEST_TYPES}.")
            return False

        # If test type is 'throughput', check that the 'size' field is non-empty
        if t_type == "throughput" and not size:
            logger.error(f"CSV line {csv_line_num}: 'size' field is missing or empty for throughput test. "
                         f"Value must be in the form '100M', '1G', etc.")
            return False

    # Check for duplicate id_number values
    id_numbers = [t['id_number'] for t in test_data]
    duplicates = [item for item in set(id_numbers) if id_numbers.count(item) > 1]
    if duplicates:
        logger.error(f"Duplicate id_number values found in input file: {duplicates}")
        return False

    logger.debug("All test data in input file has been validated successfully.")
    return True


def delete_old_result_files(directory: str, max_days: int) -> None:
    """
    Delete old result files from the results directory. This function will delete any files in the results directory
    that match the naming convention of the script's output files, and that are older than the specified number of days.
    :param directory: the directory to search for old result files
    :param max_days: the maximum age of files to keep (in days)
    :return: None
    """
    match_prefix = results_prefix   # the naming prefix of the script's output files
    match_suffix = ".json"  # the suffix of the script's output files
    logger.info(f"Will delete files older than {max_days} days in directory '{directory}' "
                f"with pattern '{match_prefix}*{match_suffix}'")
    current_time = datetime.now()
    for filename in os.listdir(directory):
        if filename.startswith(match_prefix) and filename.endswith(match_suffix):
            file_path = os.path.join(directory, filename)
            file_stat = os.stat(file_path)
            file_modified_time = datetime.fromtimestamp(file_stat.st_mtime)
            file_age = current_time - file_modified_time
            if file_age > timedelta(days=max_days):
                os.remove(file_path)
                logger.debug(f"Deleted old file: {filename} (age: {file_age.days} days)")


def parse_ping_results(test_data: dict, raw_output: str) -> dict:
    """
    Parse the results of a ping test. This function takes a dictionary as input, containing the following
    keys: id_number, test_params, and raw_output. It returns a dictionary containing the parsed results.
    :param test_data: dictionary containing the test ID and parameters (source, dest, etc) of the test that was run.
    :param raw_output: string containing the raw output from subprocess.check_output() for the ping test.
    :return: a dictionary containing the parsed results of the ping test.
    """
    id_num = test_data['id_number']
    source = test_data['source']
    dest = test_data['destination']

    # We set these values to None here, to avoid repeating the same 2 lines of code in the If statements
    #  that detect: A) no min/avg/max line; B) >1 min/avg/max line; and C) running on Windows OS.
    min_rtt, avg_rtt, max_rtt, stddev_rtt = None, None, None, None
    packets_txd, packets_rxd, packet_loss_percent = None, None, None

    # Isolate out the line in the ping output that contains the summary results
    rtt_line = [line for line in raw_output.split('\n') if 'min/avg/max' in line]

    # Ensure that if raw_output has 0, or >1 lines containing 'min/avg/max', then we handle it gracefully. We log
    #  something, then do nothing else, because we've already set all the test result variables to None.
    if len(rtt_line) == 0:
        logger.error(f"Test ID {id_num}: No line found in ping output containing 'min/avg/max'. Skipping test. "
                     f"Full output of test:\n{raw_output}")
    elif len(rtt_line) > 1:  # This situation is extremely unlikely
        logger.error(f"Test ID {id_num}: Multiple lines found in ping output containing 'min/avg/max'. Skipping "
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
            logger.error(f"Test ID {id_num}: No line found in ping output containing 'packet loss'. Will record "
                         f"RTT results but not tx/rx/lost packets. Full output of test:\n{raw_output}")
            success_msg_suffix = f"packet count data not found in ping output."
        else:
            # Grab the transmitted packets, received packets, and % packet loss from loss_line. This will work
            # for MacOS and most Linux OSes.
            # Example line 1 (MacOS, Linux): "10 packets transmitted, 10 packets received, 0.0% packet loss"
            # Example line 2 (some Linux): "10 packets transmitted, 10 received, 0.0% packet loss"
            loss_line = loss_lines[0]
            split_line = loss_line.split(', ')  # ['10 packets transmitted', '10 packets received', etc...]
            packets_txd = int(split_line[0].split(' ')[0])  # ['10', 'packets', 'transmitted'] -> '10' -> 10
            packets_rxd = int(split_line[1].split(' ')[0])  # ['10', 'packets', 'received'] -> '10' -> 10

            # It's hard to reliably parse loss% out of the string because some Linux OSes use slightly different
            # wording, or insert "+1 duplicates" in the middle of the string. So we calculate the loss ourselves.
            packet_loss_percent = round(((packets_txd - packets_rxd) / packets_txd) * 100, 4)
            success_msg_suffix = f"{packets_txd}/{packets_rxd}/{packet_loss_percent}% (#tx/#rx/loss)"

        # Log output to the screen and to logfile. We do this inside the parse functions because we have easy access to
        #  the variables for the specific test type. This enables output of short-form results in a one-line log entry.
        #  We could do this in run_test() but we'd need a block of if-logic that works out the test type then extracts
        #  the necessary key/value data from the results dict, then generates the appropriate message.
        # Note: we're using a separate print() here because the logger will only display console messages if they're
        #  at WARNING or above severity, and it's inappropriate to log success using a WARNING/ERROR severity.
        success_msg = (f"Test ID {id_num} (src: '{source}', dst: '{dest}', ping): Success. Result: "
                       f"{min_rtt}/{avg_rtt}/{max_rtt}/{stddev_rtt} ms (min/avg/max/*dev), " + success_msg_suffix)
        print(success_msg)
        logger.info(success_msg)

    return {
        "min_rtt": min_rtt,
        "avg_rtt": avg_rtt,
        "max_rtt": max_rtt,
        "stddev_rtt": stddev_rtt,
        "packets_txd": packets_txd,
        "packets_rxd": packets_rxd,
        "packet_loss_percent": packet_loss_percent,
    }


def parse_iperf_results(test_data: dict, raw_output: str) -> dict:
    """
    Parse the results of an iPerf3 test. This function takes a dictionary as input, containing the following
    keys: id_number, test_params, and raw_output. It returns a dictionary containing the parsed results.
    :param test_data: a dictionary containing the test ID, test parameters, and the raw output of the test command.
    :param raw_output: the raw output of the iperf3 test command that was run.
    :return: a dictionary containing the parsed results of the iPerf3 test.
    """

    id_num = test_data['id_number']
    t_type = test_data['test_type']
    source = test_data['source']
    dest = test_data['destination']

    # Convert iPerf's JSON output to a Python dictionary
    command_result = json.loads(raw_output)

    # There is a situation in which iPerf3 will run, but cannot connect to the server (connection refused, etc). and
    #  weirdly iPerf3/subprocess.check_output() will return exit code 0. Luckily iPerf3 returns JSON with an 'error'
    #  key whose value is a string explaining the error.
    if "error" in command_result:
        logger.error(f"Test ID {id_num} (src: '{source}', dst: '{dest}', {t_type}): Failure. iPerf3 encountered "
                     f"an error: '{command_result['error']}'")
        logger.debug(f"Full JSON returned by iPerf3: {command_result}")
        return {
            "error": command_result['error']
        }

    # Dig through specific fields in the JSON for the test measurements we are interested in
    if t_type == "throughput":
        parsed_results = {
            "seconds": command_result['end']['sum_sent']['seconds'],
            "bytes": command_result['end']['sum_sent']['bytes'],
            "bits_per_second": command_result['end']['sum_sent']['bits_per_second'],
        }
        short_form_results = f"{parsed_results['seconds']} seconds; {parsed_results['bytes']} bytes; " \
                             f"{parsed_results['bits_per_second']} bits/sec"
    elif t_type == "jitter":
        parsed_results = {
            "jitter_ms": command_result['end']['sum']['jitter_ms'],
            "packets": command_result['end']['sum']['packets'],
            "lost_packets": command_result['end']['sum']['lost_packets'],
        }
        short_form_results = f"{parsed_results['jitter_ms']} ms jitter; {parsed_results['packets']} packets; " \
                             f"{parsed_results['lost_packets']} lost"
    else:
        raise ValueError(f"Invalid test type '{t_type}' passed for test {id_num}.")

    # Log output to the screen and to logfile. We do this in each specific parse function so that we have access to
    #  the variables for that specific test type. This allows us to output short-form results in a one-line log entry.
    #  NOTE: We use a separate print() statement for the console output, because the logger will only display console
    #  messages at WARNING level or above, so we can't use one logger.info() call to convey success to the console.
    msg = f"Test ID {id_num} (src: '{source}', dst: '{dest}', {t_type}): Success. Result: {short_form_results}"
    print(msg)
    logger.info(msg)

    return parsed_results


def parse_results(test_params: dict, raw_output: str) -> dict:
    """
    Wrapper function to make the code inside run_test() tidier. It just calls the relevant parse function for the
    test type that was run. This abstraction layer also makes it easier add more test types in the future.
    :param test_params: a dict containing the parameters of the test that was run.
    :param raw_output: the raw output of the test command that was run.
    :return: whatever the wrapped parser functions return, which is a dict of test results.
    """
    if test_params['test_type'] == "latency":
        return parse_ping_results(test_params, raw_output)
    elif test_params['test_type'] in ["throughput", "jitter"]:
        return parse_iperf_results(test_params, raw_output)


def run_test(test_params: dict) -> dict:
    """
    Run a test based on the parameters in the input dictionary. The dictionary should contain the following keys:
    - id_num: a unique identifier for the test. Mandatory.
    - source: the source IP or hostname for the test. Used for constructing the test command (ie. local or SSH).
    Default is 'localhost' if not supplied.
    - destination: the destination IP or hostname for the test. Mandatory.
    - count: the number of pings to send (optional; default 10)
    - size: the size of the ping packet (optional; default 56 bytes)
    :param test_params: a dictionary containing the parameters for the test
    :return: a dictionary containing the results of the test
    """
    id_num = test_params['id_number']  # this is a required field, so we can assume it's present
    source = test_params.get('source', 'localhost')  # if value was missing from CSV, assume 'localhost'
    destination = test_params['destination']  # required field
    username = host_config.get(source, 'username')  # get this host's username from the host_config file

    if test_params['test_type'] == "latency":
        size = test_params.get('size', 56)  # optional field; go for 56 byte packet size if not specified
        count = test_params.get('count', 10)  # optional field; set default of 10 pings if not specified
        interval = PING_INTERVAL
        test_command = f"ping -c {count} -i {interval} -s {size} {destination}"

    elif test_params['test_type'] == "throughput":
        size = test_params.get('size', None)  # mandatory for throughput tests - throw exception if missing
        if size is None:
            logger.error(f"Size parameter missing for test {id_num}. This field is required for throughput tests.")
            raise ValueError(
                f"Size parameter missing for test {id_num}. This field is required for throughput tests.")
        else:
            test_command = f"iperf3 -c {destination} -n {size} -4 --json"
    elif test_params['test_type'] == "jitter":
        test_command = f"iperf3 -c {destination} -u -4 --json"
    else:
        logger.error(f"Unknown test type '{test_params['test_type']}' for test {id_num}. Skipping test.")
        raise ValueError(f"Unknown test type '{test_params['test_type']}' for test {id_num}. Skipping test.")

    # Get the current machine's hostname, FQDN and name-lookup the IP from the hostname. The user should not ever be
    #  putting an IP address into the 'source' field, but in case they do, we'll try to handle it gracefully. Also
    #  note that on some systems (particularly home networks) gethostname() will include ".local" or ".gateway".
    if source in [my_hostname, my_fqdn, my_ip_addr, "localhost", "127.0.0.1"]:
        # Run the test locally - do nothing here, because test_command already = a local test
        logger.info(f"Test ID {id_num} source '{source}' matches local machine details. Test will be run locally.")
    else:
        # Wrap test_command in an SSH command to run the test on a remote machine
        logger.info(f"Test ID {id_num} source '{source}' is not local machine. Constructing SSH remote command.")
        test_command = f"ssh -n -o ConnectTimeout=2 {username}@{source} '{test_command}'"

    # this timestamp records the test start time, so we grab it here just before the test is executed
    timestamp = datetime.now()
    logger.info(f"Test ID {id_num} initiated. Running command: {test_command}")

    # Data that appears in results_dict regardless of test type, or whether the test succeeds or fails
    results_dict = {
        "id_number": id_num,
        "timestamp": str(timestamp),
        "status": None,
        "source": source,
        "destination": destination,
        "test_command": test_command
    }

    try:
        # Execute the command and get the result.
        raw_output = subprocess.check_output(test_command, shell=True, stderr=subprocess.STDOUT).decode()

    except subprocess.CalledProcessError as e:
        logger.error(f"Test failure for test ID {id_num} (command '{test_command}'). "
                     f"Full output of test command: {e.output.decode()}")
        results_dict["status"] = "Failure"

    else:  # if the command didn't trigger a CalledProcessError, we can parse the results
        p_results = parse_results(test_params=test_params, raw_output=raw_output)
        logger.debug(f"Test ID {id_num} parsed results: {p_results}")
        # This next check is because older iPerf3 versions can return exit code 0 under some failure conditions (eg.
        # connection refused), when --json is used. Luckily in these situations iPerf3 returns a JSON object with an
        # 'error' key. So parse_iperf_results() looks for this and passes on iPerf's 'error' key/value pair here.
        if p_results.get("error", None) is not None:
            results_dict["status"] = "Failure"
        else:
            results_dict["status"] = "Success"

        # Regardless, we'll merge p_results into results_dict so that our output file contains the error value/string.
        results_dict.update(p_results)

    # Regardless of whether the test succeeded or failed, we return the results_dict to the main code body.
    return results_dict


# This script will not work under Windows, for a couple of reasons. Firstly, the output of the ping command is vastly
# different under Windows. Secondly, the command-line options for the Windows ping command are completely different.
if os.name == 'nt':
    print(f"FATAL: This script will not run on Windows systems. It is designed for execution on Unix-based "
          f"operating systems. Halting execution.")
    exit(1)

# Record the start-time of program execution so we can output the duration at the end of the script
execution_start_time = datetime.now()

# Process command-line arguments
args = get_cmdline_args()
if args.verbose:
    LOGGING_LEVEL = logging.DEBUG
log_dir = args.log_dir
input_csv = args.input
results_dir = args.output
host_config_file = args.host_config
PING_INTERVAL = args.ping_interval

# This must be checked *before* logging is enabled. Other directories/files are checked after logging is enabled.
check_dir_and_permissions(dir_path=log_dir, description="Log directory", mode=os.W_OK | os.R_OK)

# Append yyyymmddhhmmss timestamping to the output filename, eg. net-test_2024-03-19_125400.json
results_prefix = f"{BASE_NAME}_results-"
results_filename = f"{results_prefix}{datetime.now().strftime('%Y-%m-%d_%H%M%S')}.json"
results_filepath = os.path.join(results_dir, f"{results_filename}")

""""###############################################################################
#   BEGIN LOGGER SETUP AS EARLY AS POSSIBLE TO ENSURE ALL OPERATIONS ARE LOGGED.  #
#   AVOID ADDING CODE ABOVE THIS POINT TO PREVENT UNLOGGED OPERATIONS.            #
################################################################################"""
log_file = os.path.join(log_dir, f"{BASE_NAME}.log")
logger = setup_logging(name=LOGGER_NAME, log_level=LOGGING_LEVEL, file_path=log_file)

logger.info(f"{'*' * 20} Initial startup {'*' * 20}")
logger.info(f"Input CSV file is: {input_csv}. Output file will be: {results_filepath}. "
            f"Logging level: {logging.getLevelName(LOGGING_LEVEL)}.")
print(f"Input CSV file is: {input_csv}. Output file will be: {results_filepath}")
print(f"Logging to {log_file}. Logging level: {logging.getLevelName(LOGGING_LEVEL)}.")

# Check that our input and output directories exist and have the correct permissions
check_dir_and_permissions(dir_path=results_dir, description="Results directory", mode=os.W_OK)
check_dir_and_permissions(dir_path=input_csv, description="Input file", mode=os.R_OK)

if args.max_age is not None:
    max_age_days = args.max_age
    logger.info(f"Deleting results files older than {max_age_days} days.")
    print(f"Deleting results files older than {max_age_days} days...")
    delete_old_result_files(directory=results_dir, max_days=max_age_days)

# Get the local hostname, FQDN and IP address. This is used to decide if a given test will be run locally, or via SSH.
logger.debug("Getting local machine's hostname, FQDN and IP address.")
my_hostname = socket.gethostname().lower().split('.')[0]  # Extract the part before the first dot
my_fqdn = socket.getfqdn().lower()
my_ip_addr = socket.gethostbyname(my_hostname)

# The wording of this log entry is carefully chosen, to make it clear that 'my_ip_addr' is not pulled from the NIC
#  the OS, it's derived by performing a lookup on my_hostname, which will use OS DNS settings or /etc/hosts.
logger.info(f"My hostname: {my_hostname}. My FQDN: {my_fqdn}. DNS resolves {my_hostname} to {my_ip_addr}.")

all_tests = read_input_file(input_csv)  # a list of dictionaries, each dict representing a test to be run

if not test_data_validated_ok(all_tests):
    logger.critical(f"Input file '{input_csv}' contains invalid data. Halting execution.")
    exit(1)

logger.info(f"Reading host configuration file {host_config_file}.")
host_config = configparser.ConfigParser()
host_config.read(host_config_file)

# Check that the host_config file has corresponding entries for each unique test source hostname.
if not host_config_validated_ok(all_tests):
    logger.critical(f"Host configuration file '{host_config_file}' is missing entries for source hostnames. "
                    f"Halting execution.")
    exit(1)

# initialise the all_results dictionary with its high-level keys
all_results = {
    "latency_tests": [],
    "throughput_tests": [],
    "jitter_tests": []
}

# Do the actual work - iterate over all_tests and run each test, appending the results to all_results
for test in all_tests:
    id_number = test['id_number']
    test_type = test['test_type']

    logger.debug(f"Test ID {id_number} of type '{test_type}' will be run.")
    test_results = run_test(test)

    # Append the results to the appropriate list in all_results
    key_name = test_type + "_tests"
    all_results[key_name].append(test_results)

# Write the results to the output file in JSON format
logger.info(f"All tests have been iterated over. Writing results to {results_filepath}.")
with open(results_filepath, 'w') as json_file:
    json.dump(all_results, json_file, indent=4)

execution_duration = datetime.now() - execution_start_time
# Create a string that expresses the duration in a human-readable format, hh:mm:ss
execution_duration_str = str(execution_duration).split('.')[0]  # remove the microseconds from the string

logger.info(f"{'*' * 20} End of script execution. Run-time was {execution_duration_str} {'*' * 20}")
