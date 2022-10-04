'''
Takes all the configs from a configs dir and puts them into a bash file to be run sequentially.
'''
import os
import sys
from utils.constants import CONFIGS_PATH, PIPELINE_PATH

# Directory to source config files from
filepath = os.path.join(PIPELINE_PATH, 'config_runner.sh')

if __name__ == '__main__':
    s = "#!/bin/bash\n\n"
    type = sys.argv[1]
    psqrole = ''

    if len(sys.argv) > 2:
        psql_role = sys.argv[2]

    i = 0
    for f in os.listdir(CONFIGS_PATH):
        # Filter configs based on argument
        if type in f:
            path = os.path.join(CONFIGS_PATH, f)
            s += 'python run.py ' + path + ' ' + psql_role

            # The first config should recreate cohort, labels, and features
            if i == 0:
                s += ' -recreate_sources'
                i += 1

            s += '\n'

    with open(filepath, 'w') as f:
        f.write(s)
