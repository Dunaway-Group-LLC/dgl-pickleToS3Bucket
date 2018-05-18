"""__main__ to test S3pickleBucket classes

"""

import sys
import getopt
import logging
from dglPickleToS3BucketClasses import S3pickleBucket, getPickleBucket
import pickle


class Usage(Exception):
    """Class to throw Usage Exception

    """

    def __init__(self, msg):
        self.msg = msg


def main(argv=None):
    logging.basicConfig(filename='testPickleBucket.log', level=logging.DEBUG)

    if argv is None:
        argv = sys.argv

#        logging.debug("main Argv: ", argv)
    try:
        try:
            opts, args = getopt.getopt(sys.argv[1:], "h", ["help"])
            logging.debug(args)
        except (getopt.error) as msg:
            raise Usage(msg)
        # Get bucket name from args
        pbName = args[1]
        logging.debug("Args: %s pbname %s" % (args, pbName))
        # try pickleBucket gaicClasses
        pb = getPickleBucket(pbName)     # bucket exists?
        logging.debug("pb %s" % (type(pb)))
        if type(pb) != S3pickleBucket:  # a pickle bucket or and error code
            quit()
        aDict = {}  # An empty dict to try pickle
        try:
            pkld = pickle.dumps(aDict)
            logging.debug("pkld: %s %s" % (type(pkld), str(pkld)))
            pb.storeObject(pkld, "emptyDict")
        except Exception as e:
            logging.error("Pickle exception %s " % (e.msg))
    except (Usage) as err:
        print >>sys.stderr, err.msg
        print >>sys.stderr, "for help use --help"
        return 2


if __name__ == "__main__":
        sys.exit(main())
