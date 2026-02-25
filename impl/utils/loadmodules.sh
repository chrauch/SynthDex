#!/bin/bash

# source this file to load the modules for mogon

export https_proxy=http://webproxy.zdv.uni-mainz.de:8888

source /etc/profile.d/modules.sh

module load compiler/GCCcore/13.3.0
module load lang/Python/3.12.3-GCCcore-13.3.0

# srun --pty -p bigmem -A m2_jgu-ijoin -N 1 -n 6 -c 6 -t 900 -C broadwell --mem 393216 bash -i