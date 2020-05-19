# from SLURM_tools import submit, job_wait
# JID = submit("echo hello world", job_name='test', modules=['python', 'java', 'g++'])
# job_wait(JID, period = 15, verbose = False)

import subprocess as sp
import time, sys, tempfile

def add_option(options, option, value):
    if value:
        return '{} {}={}'.format(options, option, value)
    else:
        return options

def submit(job,
           conda = 'osprey',
           partition = 'savio2',
           account='fc_bioml',
           gpus=0,
           qos = 'savio_normal',
           job_name = 'slurmjob',
           time = '48:0:0',
           error = '', 
           output = '', 
           nodes = 1,
           tasks_per_node = 1,
           cpus_per_task = 1,
           mem_per_cpu = 3000, 
           mail_type = None,#'FAIL'
           mail_user = 'hunter_nisonoff@berkeley.edu',
           shell = 'bash',
           modules = ['python'],
           verbose = True):

    if not error:
        error = job_name + '_%j.err'
    if not output:
        output = job_name + '_%j.out'
    if gpus > 0:
        cpus_per_task = max(cpus_per_task, 2*gpus)
    options = {'--partition':partition, '--qos':qos, '--job-name':job_name,
               '--time':time, '--error':error, '--output':output,
               '--nodes':nodes, '--tasks-per-node':tasks_per_node,
               '--cpus-per-task':cpus_per_task, '--mem-per-cpu':mem_per_cpu,
               '--mail-type':mail_type, '--mail-user':mail_user, '--account':account}
    options = {k:v for k, v in list(options.items()) if v} # remove options set to None
    if gpus > 0:
        options["--gres"] = f"gpu:{gpus}"
    
    opt_string = ''
    for option, value in list(options.items()):
        opt_string = add_option(opt_string, option, value)
    command = 'sbatch '+opt_string

    mod_string = ''
    for mod in modules:
        mod_string += 'module load '+mod+';\n'
    mod_string += f'source activate {conda};\n'
    mod_string += f'which python;\n'

    submission = sp.Popen(command, shell=True,
                          stdout=sp.PIPE, stderr=sp.PIPE, stdin=sp.PIPE)
    job = '#!/bin/{}\n{}{}'.format(shell, mod_string, job)
    out, err = submission.communicate(input = job.encode())
    out = out.decode()
    err = err.decode()
    if err:
        sys.exit(err)
    else:
        jobID = out.strip().split()[-1]
        if verbose:
            print('Submitted: {}'.format(jobID))
        return jobID
    
def submit_script(job):
    submission = sp.Popen('sbatch ' + job, shell=True,
                          stdout=sp.PIPE, stderr=sp.PIPE)
    out, err = submission.communicate()
    if err:
        sys.exit(err)
    else:
        jobID = out.strip().split()[-1]
        return jobID.decode()

def check_job(jobID):
    check = sp.Popen('sacct --format End -j {}'.format(jobID), shell=True,
                     stdout=sp.PIPE, stderr=sp.PIPE)
    out, err = check.communicate()
    if err:
        sys.exit(err)
    out = out.decode()
    endtime = out.split('\n')[2].strip()
    if endtime:
        finished = endtime != 'Unknown'
    else:
        finished = False
    return finished

def job_wait(jobID, period=60, verbose=True):
    unfinished = True
    # takes a second for the slurm scheduler to respond to sacct
    time.sleep(1)
    while unfinished:
        unfinished = not check_job(jobID)
        time.sleep(period)
    if verbose:
        print('Completed: {}'.format(jobID))
