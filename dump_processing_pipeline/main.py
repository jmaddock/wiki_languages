import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import subprocess
import utils
import config

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
JOB_SCRIPT = 'job_script'
XML_DUMP_TO_RAW_EDITS_TASK_LIST = 'xml_dump_to_raw_edits_task_list'
PROCESS_RAW_EDITS_TASK_LIST = 'process_raw_edits_task_list'

MAX_JOBS = 6

# create a temp directory for task lists if it doesn't already exist
def create_script_dir():
    utils.log('checking for dir: {0}'.format(config.TEMP_SCRIPT_DIR))
    if not os.path.exists(config.TEMP_SCRIPT_DIR):
        utils.log('creating dir: {0}'.format(config.TEMP_SCRIPT_DIR))
        os.makedirs(config.TEMP_SCRIPT_DIR)

# get the number of lines in a file and divide by 16
# limit number of nodes using global var MAX_NODES
def get_num_nodes(fname):
    with open(fname) as f:
        for i, l in enumerate(f):
            pass
    num_nodes = int((i + 1)/16)
    if num_nodes > MAX_JOBS:
        num_nodes = MAX_JOBS
    return num_nodes

def load_and_run(prev_job_id):
    return

# STEP 1: convert xml dumps to csvs
def xml_dump_to_raw_edits():
    # define the path and file name for creating a task list
    task_list_path = os.path.join(config.TEMP_SCRIPT_DIR,XML_DUMP_TO_RAW_EDITS_TASK_LIST)
    utils.log('creating xml_dump_to_raw_edits task list in {0}'.format(task_list_path))
    # define the path to the python script
    script_file = os.path.join(SCRIPT_DIR,'xml_dump_to_raw_edits.py')
    # create the task list
    subprocess.run(['python3',script_file,'-j',task_list_path])
    # load the task list into parallel sql
    subprocess.run(['cat',task_list_path,'psu','--load'])
    # calculate the number of nodes required for the job (num_tasks / 16)
    num_nodes = '0-{0}'.format(get_num_nodes(task_list_path))
    # call qsub to run the job and capture the job number through stdout
    process = subprocess.run(['qsub','job_script','-t',num_nodes],stdout=subprocess.PIPE)
    # remove the server information from the job number
    job_num = process.stdout.split('[]')[0] # of form 4529568[].mgmt2.hyak.local
    return job_num

def process_raw_edits(prev_job_id):
    task_list_path = os.path.join(config.TEMP_SCRIPT_DIR,PROCESS_RAW_EDITS_TASK_LIST)
    dependency_str = 'depend=afterokarray:{0}[]'.format(prev_job_id)
    process = subprocess.run(['qsub','job_script','-W',dependency_str])
    job_num = process.stdout.split('[]')[0] # of form 4529568[].mgmt2.hyak.local
    return job_num

def combine_all_edits():
    return

def main():
    create_script_dir()
    xml_dump_to_raw_edits()
    sys.exit(0)

if __name__ == "__main__":
    main()
