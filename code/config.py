# imports
import os

# defining global variables
OUTPUT_DIR = os.path.dirname(os.path.realpath(__file__))
OUTPUT_DIR = "{}/output".format(OUTPUT_DIR[:OUTPUT_DIR.find("/code")])
OUTPUT_MASTER_DIR = "{}/masters".format(OUTPUT_DIR)