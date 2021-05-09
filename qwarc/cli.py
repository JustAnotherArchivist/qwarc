import argparse
import asyncio
import importlib.util
import logging
import os.path
import qwarc
import qwarc.utils
import qwarc.version
import sys


def setup_logging(logFilename):
    rootLogger = logging.getLogger()
    rootLogger.handlers = []
    rootLogger.setLevel(logging.INFO)

    formatter = qwarc.utils.LogFormatter()

    fileHandler = logging.FileHandler(logFilename)
    fileHandler.setFormatter(formatter)
    rootLogger.addHandler(fileHandler)

    stderrHandler = logging.StreamHandler()
    stderrHandler.setFormatter(formatter)
    rootLogger.addHandler(stderrHandler)


def check_files(specFilename, logFilename):
    success = True
    if not os.path.isfile(specFilename):
        print(f'Error: "{specFilename}" does not exist or is not a regular file', file=sys.stderr)
        success = False
    if os.path.exists(logFilename):
        print(f'Error: "{logFilename}" already exists', file=sys.stderr)
        success = False
    if os.path.exists('STOP'):
        print('Error: "STOP" exists', file=sys.stderr)
        success = False
    return success


def main():
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--version', action='version', version=f'qwarc {qwarc.version.__version__}')
    parser.add_argument('--log', metavar='LOGFILE', default='./qwarc.log')
    parser.add_argument('--database', metavar='DBFILE', default='./qwarc.db')
    parser.add_argument('--warc', metavar='PREFIX', help='prefix for the WARC filenames', default='./qwarc')
    parser.add_argument('--concurrency', type=int, default=1)
    parser.add_argument('--memorylimit', metavar='LIMIT', type=int,
                        help='pause when less than LIMIT bytes memory is free; disable if 0', default=0)
    parser.add_argument('--disklimit', metavar='LIMIT', type=int,
                        help='pause when less than LIMIT bytes disk space is free; disable if 0', default=0)
    parser.add_argument('--warcsplit', metavar='SIZE', type=int,
                        help='split WARCs into files of SIZE bytes; disable if 0', default=0)
    parser.add_argument('--warcdedupe', action='store_true', help='enable deduplication of WARC records')
    parser.add_argument('specfile')
    parser.add_argument('--userAgent', type=str)

    args = parser.parse_args()

    if not check_files(args.specfile, args.log):
        sys.exit(1)

    setup_logging(args.log)

    spec = importlib.util.spec_from_file_location('spec', args.specfile)
    specMod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(specMod)
    specDependencies = specMod.__dict__.get('specDependencies', qwarc.utils.SpecDependencies())

    a = qwarc.QWARC(
        itemClasses=set(qwarc.Item.get_subclasses()),
        warcBasePath=args.warc,
        dbPath=args.database,
        command=sys.argv,
        specFile=args.specfile,
        specDependencies=specDependencies,
        logFilename=args.log,
        userAgent=args.userAgent,
        concurrency=args.concurrency,
        memoryLimit=args.memorylimit,
        minFreeDisk=args.disklimit,
        warcSizeLimit=args.warcsplit,
        warcDedupe=args.warcdedupe,
    )
    if not os.path.exists(args.database):
        a.create_db()

    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(a.run(loop))
    except (Exception, KeyboardInterrupt) as e:
        logging.exception('Unhandled error')
    loop.close()
