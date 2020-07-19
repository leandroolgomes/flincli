import click
import yaml
from tabulate import tabulate
import requests
from os import environ
from urllib.parse import urlencode
from datetime import datetime, timedelta
import time
import json

FLINK_BASE_URL=environ.get('FLINK_BASE_URL', None)

DEFAUL_HEADER = {'Content-type': 'application/json', 'Accept': 'application/json'}


@click.group()
@click.version_option()
def flink():
    """Cli for flink jobs"""

"""
Validate if Flink URL exists
"""
def validateFlinkUrl():
    if not FLINK_BASE_URL:
        print('Export FLINK_BASE_URL env var')
        raise Exception('Flink URL not defined')

"""
list all jobs on flink
"""
def list_jobs():
    r = requests.get('%s/jobs/overview'%FLINK_BASE_URL)
    return r.json()['jobs']

def filter_running(jobs):
    result = []
    for job in jobs:
        if job['state'] == 'RUNNING':
            result.append(job)
    return result

"""
format seconds in a pretty format
"""
def pretty_time_delta(seconds):
    sign_string = '-' if seconds < 0 else ''
    seconds = abs(int(seconds))
    days, seconds = divmod(seconds, 86400)
    hours, seconds = divmod(seconds, 3600)
    minutes, seconds = divmod(seconds, 60)
    if days > 0:
        return '%s%dd%dh%dm%ds' % (sign_string, days, hours, minutes, seconds)
    elif hours > 0:
        return '%s%dh%dm%ds' % (sign_string, hours, minutes, seconds)
    elif minutes > 0:
        return '%s%dm%ds' % (sign_string, minutes, seconds)
    else:
        return '%s%ds' % (sign_string, seconds)

"""
list existings jobs on Flink
"""
@flink.command(help='list all jobs')
@click.option('--all', '-a', is_flag=True, required=False, help='Show all jobs')
def list(all):
    validateFlinkUrl()
    all_jobs = list_jobs()
    output = all_jobs if all else filter_running(all_jobs)
    if output:
        click.echo(tabulate({'ID': [item['jid'] for item in output],
                             'Name': [item['name'] for item in output],
                             'Status': [item['state'] for item in output],
                             'Started': [datetime.fromtimestamp(item['start-time']/1000.0) for item in output],
                             'Ended': ['-' if item['end-time'] < 0 else datetime.fromtimestamp(item['end-time']/1000.0) for item in output],
                             'Running time': [pretty_time_delta(item['duration']/1000.0) for item in output],
                             },
                            headers='keys', tablefmt='grid'))
    else:
        click.echo("No jobs found")


"""
Upload a jar file to Flink
"""
def upload_jar(file_path):
    files = {'jarfile': open(file_path, 'rb') }
    r = requests.post('%s/jars/upload'%FLINK_BASE_URL, files=files)
    data = r.json()
    if data['status'] != 'success':
        raise Exception('Fail to upload jar file %s'%file_path)

    # jar id
    return data['filename'].split('/')[-1]

"""
Return only running jobs on Flink
"""
def find_running_job(job_base_name):
    runningJobs = list_jobs()
    result = []
    for job in runningJobs:
        if job['state'] == 'RUNNING' and job['name'] == job_base_name:
            result.append(job)
    return result

"""
convert job run params to Flink format
"""
def generateParams(params):
    paramsList = []
    for p in params:
        paramsList.append("--%s"%" ".join(p.split('=')))

    return " ".join(paramsList)

"""
Cancel a Flink job by job id
"""
def cancel_job(job_id):
    r = requests.patch('%s/jobs/%s'%(FLINK_BASE_URL, job_id))
    if not r.ok:
        print(r.text)
        raise Exception("Error canceling job")

"""
Run a job wih uploaded jar id
"""
def run_jar(jar_id, deployParams):
    queryString = urlencode(deployParams)
    r = requests.post('%s/jars/%s/run?%s'%(FLINK_BASE_URL, jar_id, queryString))
    if not r.ok:
        print(r.text)
        raise Exception("Error running jar")
    data = r.json()
    return data['jobid']

"""
Deploy a jar to Flink
- verify if there is a job running
- if exists, create a savepoint, cancel job e create a new job using the previous created savepoint
- if not exists, just run the new job
"""
@flink.command(help='Deploy a job jar')
@click.option('--name', '-n', metavar='<job_base_name>', help='Job base name')
@click.option('--jarfile', '-f', metavar='<file_path>', help='Path of file to upload to Flink')
@click.option('--params', '-p', metavar='<params>', multiple=True, required=False, help='Parameters to run job')
@click.option('--parallelism', '-pr', metavar='<parallelism>', required=False, help='Job parallelism')
@click.option('--savepoint', '-s', metavar='<savepointPath>', required=False, help='Savepoint path for job')
@click.option('--entryclass', '-e', metavar='<entryclass>', required=False, help='Entry class for job')
def deploy(name, jarfile, params, parallelism, savepoint, entryclass):
    validateFlinkUrl()

    deployParams = {}
    if(params):    
        deployParams['program-args'] = generateParams(params)
    if(savepoint):
        deployParams['savepointPath'] = savepoint
    if(entryclass):
        deployParams['entry-class'] = entryclass
    if(parallelism):
        deployParams['parallelism'] = parallelism

    currentJob = None
    runningJobs = find_running_job(name)
    if len(runningJobs) == 0:
        click.echo('No instance running for job %s. Starting a new deployment.'%name)
    elif len(runningJobs) == 1:
        currentJob = runningJobs[0]
        job_id = currentJob['jid']
        click.echo("Creating savepoint for job %s"%job_id)
        
        if not savepoint:
            createdSavepoint = create_savepoint(job_id, savepoint)
            if not createdSavepoint:
                click.echo('Error on savepoint creation. Aborting deployment.')
                return
            deployParams['savepointPath'] = createdSavepoint

        cancel_job(job_id)
        click.echo('Job %s cancelled'%job_id)
    
    click.echo('Uploading jar to Flink...')
    jarID = upload_jar(jarfile)
    click.echo('Jar file upload! JarId: %s'%jarID)
    click.echo('Deploying job with params: %s'%deployParams)
    new_job_id = run_jar(jarID, deployParams)
    click.echo("Deploy completed with jar id: %s!"%jarID)
    click.echo(tabulate({'Job': [name], 'JobId': [new_job_id]},headers='keys', tablefmt='grid'))


"""
Create a savepoint from a running Job
"""
@flink.command(help='Trigger a savepoint for job')
@click.option('--job-base-name', '-n', metavar='<job_base_name>', help='Job base name')
@click.option('--savepoint-path', '-s', metavar='<savepoint_path>', help='Alternative savepoint path')
def savepoint(job_base_name, savepoint_path):
    validateFlinkUrl()
    runningJobs = find_running_job(job_base_name)
    if len(runningJobs) == 1:
        job_id = runningJobs[0]['jid']
        created = create_savepoint(job_id, savepoint_path)
        if not created:
            print('Error on savepoint creation')
        else:
            print('Savepoint created: %s'%created)
    else:
        print('No running jobs for basename %s'%job_base_name)
    
"""
Create a savepoint from a running Job
"""
def create_savepoint(job_id, savepoint_path):
    payload = { 'cancel-job': False }
    if savepoint_path:
        payload['target-directory'] = savepoint_path

    r = requests.post('%s/jobs/%s/savepoints'%(FLINK_BASE_URL, job_id), json=payload, headers=DEFAUL_HEADER)
    if not r.ok:
        print(r.text)
        raise Exception("Error creating savepoint") 

    data = r.json()
    request_id = data['request-id']

    # monitor
    stop = datetime.now() + timedelta(seconds=60)
    finish = False
    while(stop > datetime.now()):
        r = requests.get('%s/jobs/%s/savepoints/%s'%(FLINK_BASE_URL, job_id, request_id))
        data = r.json()
        status = data['status']['id']
        if status == 'IN_PROGRESS':
            print('Savepoint in progress...')
            time.sleep(0.5)
            continue

        if status == 'COMPLETED':
            print('Savepoint completed!')
            finish = True
            return data['operation']['location']

        break

    if not finish:
        print('Timeout on savepoint creation...')

    return finish