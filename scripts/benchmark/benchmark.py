#!/env/python3

import click
import yaml
from subprocess import call, list2cmdline
import time
import shutil
from prettytable import PrettyTable
import numpy
from natural import date
import os
import traceback
import logging
import pathlib


from logging import handlers

# create logger with 'spam_application'
logger = logging.getLogger('benchmark')
logFormatter = logging.Formatter("%(asctime)s [%(levelname)-5.5s]  %(message)s")

consoleHandler = logging.StreamHandler()
consoleHandler.setFormatter(logFormatter)
logger.addHandler(consoleHandler)


syslogHandler = logging.handlers.SysLogHandler(address = '/dev/log')
#syslogHandler.setFormater(logFormatter)
logger.addHandler(syslogHandler)

logger.setLevel(logging.DEBUG)

def get_folder_size(start_path = '.'):
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(start_path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            total_size += os.path.getsize(fp)
    return total_size

def get_tiledb_file_sizes(start_path = '.'):
    sizes = {}
    for dirpath, dirnames, filenames in os.walk(start_path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            if not f in sizes:
                sizes[f] = 0
            sizes[f] += os.path.getsize(fp)
    return sizes

def flush_caches():
    """Flush linux caches"""
    logger.info("Syncing and flushing linux caches")
    call("sync")
    call(["echo", "3", "|", "sudo", "tee", "/proc/sys/vm/drop_caches"])

@click.command()
@click.option('--config', required=True, help='Yaml config file for benchmark')
def run_benchmark(config):
    """ Benchmark script for tiledb-vcf"""

    # Open yaml config file
    with open(config, 'r') as stream:
        try:
            benchmarking_start = time.time()
            results = []
            config = yaml.load(stream)
            base_cmd = config['base_command']
            iterations = config['iterations']
            ingestion_files = config['ingestion_files']
            attribute_results = {}
            suite_index = 0
            suite_names = []

            errors = {}

            # Get the size of the files being ingested
            ingestion_size = 0
            for ingestion_file in ingestion_files:
                ingestion_size += os.path.getsize(ingestion_file) / (1024 * 1024)

            # Loop through each test suite
            for suite_name, test_set in config['suites'].items():
                suite_names.append(suite_name)
                test_results = {}
                # Run each suite the given number of iterations
                iteration_count = 0
                for i in range(iterations):
                    iteration_count += 1

                    array_uri = test_set['array_uri']
                    group_uri = test_set['group_uri']
                    dir_to_rm = None
                    if 'group_uri' in test_set:
                        dir_to_rm = group_uri
                    else:
                        dir_to_rm = array_uri

                    if not dir_to_rm is None and os.path.isdir(dir_to_rm):
                        shutil.rmtree(dir_to_rm)

                    if not os.path.isdir(group_uri):
                        pathlib.Path(group_uri).mkdir(parents=True, exist_ok=True)

                    # Run each test in the suite
                    for test in test_set['tests']:

                        # Flush caches
                        flush_caches()

                        test_name = test["name"]
                        logger.info("Starting test %s - %s iteration %d", suite_name, test_name, i)

                        # Add specified arguments
                        cmd = [base_cmd] + test['args']
                        # Add group uri argument
                        cmd.extend(["-a", array_uri])

                        # If store or register add ingestion files
                        if test_name == "store" or test_name == "register":
                            cmd.append("-f")
                            cmd.extend(ingestion_files)

                        if test_name == "export":
                            export_path = os.path.join(group_uri, "export")
                            if not os.path.isdir(export_path):
                                os.mkdir(export_path)
                            #cmd.extend(["-p",  export_path + os.path.sep])

                        logger.info("Running: %s", list2cmdline(cmd))

                        # Time and run test command
                        t0 = time.time()
                        t1 = None
                        try:
                            ret = call(cmd)
                            t1 = time.time()
                        except Exception as e:
                            if not suite_name in errors:
                                errors[suite_name] = {
                                    "test_name": []
                                }
                            if not test_name in errors[suite_name]:
                                errors[suite_name][test_name] = []
                            errors[suite_name][test_name].append({
                                "iteration": i,
                                "ret_code": ret
                            })
                            logging.error(traceback.format_exc())
                            continue

                        array_size = 0
                        tiledb_file_sizes = None
                        if 'check_array_size' in test and test['check_array_size']:
                            array_size = get_folder_size(array_uri)
                            tiledb_file_sizes = get_tiledb_file_sizes(array_uri)

                        # Save results
                        if not test_name in test_results:
                            test_results[test_name] = {"time": [], "size": [], "file_sizes": {}}
                        test_results[test_name]["time"].append(t1-t0)
                        test_results[test_name]["size"].append(array_size)
                        if tiledb_file_sizes != None:
                            for file_name, size in tiledb_file_sizes.items():
                                if not file_name in test_results[test_name]["file_sizes"]:
                                    test_results[test_name]["file_sizes"][file_name] = []
                                test_results[test_name]["file_sizes"][file_name].append(size)

                # If there was a store test we should save results for printing table at the end
                if 'store' in test_results:
                    ingestion_times = test_results["store"]["time"]
                    ingestion_time_avg = numpy.average(ingestion_times)
                    size_avg = numpy.average(test_results["store"]["size"]) / (1024 * 1024)
                    ingestion_time_std = numpy.std(ingestion_times)
                    export_time_avg = 'N/A'
                    export_time_std = 'N/A'

                    if 'export' in test_results:
                        export_times = test_results["export"]["time"]
                        export_time_avg = numpy.average(export_times)
                        export_time_std = numpy.std(export_times)

                    results.append([suite_name, iteration_count, ingestion_time_avg, ingestion_time_std,
                                    size_avg, ingestion_size, export_time_avg, export_time_std])

                    for file_name, file_sizes in test_results['store']["file_sizes"].items():
                        if not file_name in attribute_results:
                            attribute_results[file_name] = [None] * len(config['suites']) #{suite_name: 'N/A'}

                        file_size_avg = numpy.average(file_sizes) / (1024 * 1024)
                        attribute_results[file_name][suite_index] = file_size_avg

                suite_index += 1

                # Remove directory to save space again
                dir_to_rm = None
                if 'group_uri' in test_set:
                    dir_to_rm = group_uri
                else:
                    dir_to_rm = array_uri

                if not dir_to_rm is None and os.path.isdir(dir_to_rm):
                    shutil.rmtree(dir_to_rm)

            header = ['Test', 'Iterations', 'Ingestion Time (seconds)',
                      'Ingestion Time (seconds) STDDEV', 'Array Size (MB)', 'Ingestion Size (MB)',
                      'Export Time (seconds)', 'Export Time STDDEV (seconds)']
            t = PrettyTable(header)
            for result in results:
                t.add_row(result)

            data = ",".join(header) + "\n"
            for result in results:
                data += ",".join(map(str, result)) + "\n"
            logger.info(data)

            print("")
            print(t)

            t = PrettyTable()

            t.add_column("Test", suite_names)

            for file_name, sizes in attribute_results.items():
                t.add_column(file_name, sizes)

            #for result in attribute_results:
            #    print(result)
            #    t.add_row(result)
            #for index in range(len(suite_names)):
            #    results = [] #[None] * len(attribute_results)
            #    for file_name, result in attribute_results.items():
            #        results.append(result[index])
            #    t.add_column(suite_names[index], results)

            # Set file_name column
            #file_name_results = [] #[None] * len(attribute_results)
            #for file_name, result in attribute_results.items():
            #    file_name_results.append(file_name)
            #t.add_column("file_name", file_name_results)

            print("")
            print(t)

            data = ",".join(t.field_names) + "\n"
            for row in t._get_rows(t._get_options({})):
                data += ",".join(map(str, row)) + "\n"
            logger.info(data)

            logger.info("Total time taken to run benchmark was: %s", date.compress(time.time() - benchmarking_start))

            if errors:
                logger.error("Errors detected in run, dumping details:")
                logger.error(errors)

        except yaml.YAMLError as exc:
            print(exc)

if __name__ == '__main__':
    run_benchmark()