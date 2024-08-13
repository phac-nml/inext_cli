import sys
import argparse
import traceback
import logging
import os
from inext_cli.classes.config import config, analysisDir
from inext_cli.utils import is_url_reachable
from . import push, pull, download


tasks = {
    'push': (push, 'Retrieve data from IRIDA Next'),
    'pull': (pull, 'Submit data to IRIDA Next'),
    'download': (download,'Download files from IRIDA Next')
}

def main(argv=None):
    module_idx = 0
    help_msg = 1
    parser = argparse.ArgumentParser(prog="inext_cli")
    sub_parsers = parser.add_subparsers(dest="command")
    for k, v in tasks.items():
        format_parser = sub_parsers.add_parser(k, description=v[help_msg], help=v[help_msg])
        v[module_idx].add_args(format_parser)
        
    args = parser.parse_args(argv)
    if args.command is None:
        parser.print_help()
        sys.exit()
    

    error_file = "errors.txt"
    try:
        logging.info("Running {}".format(args.command))
        tasks[args.command][module_idx].run(args)
        logging.info("Finished: {}".format(args.command))
    except Exception as e:
        with open(error_file, "w") as f:
            f.write(traceback.format_exc())
        error_number = e.errno if hasattr(e, "errno") else -1
        logging.critical("Program exited with errors, please review logs. For the full traceback please see file: {}".format(error_file))
        SystemExit(error_number)
    else:
        logging.info("Program finished without errors.")

# call main function
if __name__ == '__main__':
    main()

