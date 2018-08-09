#!/usr/bin/python

from __future__ import print_function

import argparse
import sys
import subprocess
import string
import time

__VERSION__ = '1.0.0'

EXECUTORS = {
    'local': 'LocalExecutor',
    'sequential': 'SequentialExecutor',
    'celery': 'CeleryExecutor'
}

CONTAINERS = {
    'webserver': 'webserver_1',
    'scheduler': 'scheduler_1',
    'fmeengine': 'fmeengine_1'
}

# Garbage Collect
def garbage_collect():
    # Clean up as part of this gig
    #subprocess.call('docker rm -v $(docker ps -a -q -f status=exited)', shell=True)
    #subprocess.call('docker rmi $(docker images -f "dangling=true" -q)', shell=True)
    #subprocess.call('docker volume rm $(docker volume ls -qf dangling=true)', shell=True)
    subprocess.call('./docker-cleanup.sh', shell=True)

def get_executor(args):
    if args.executor:
        return args.executor
    else:
        executor = raw_input('Enter Executor (' + ', '.join(EXECUTORS.keys()) + '): ')
        if not executor in EXECUTORS:
            raise Exception('Specified executor is not valid.')
        return executor

def get_container(args):
    if args.container:
        return args.container
    else:
        container = raw_input('Enter Executor (' + ', '.join(CONTAINERS.keys()) + '): ')
        if not container in CONTAINERS:
            raise Exception('Specified container is not valid.')
        return container

def kill_container(container):
    subprocess.call('docker kill {0}'.format(container), shell=True)

def command_start(args):
    executor = get_executor(args)


    print("Executing start on {0} executor.".format(executor))

    subprocess_command = "docker-compose -f docker-compose-{0}.yml start -d"
    subprocess.call(subprocess_command.format(EXECUTORS[executor]), shell=True)

def command_up(args):
    executor = get_executor(args)

    print("Executing up on {0} executor.".format(executor))

    subprocess_command = "docker-compose -f docker-compose-{0}.yml up -d"
    subprocess.call(subprocess_command.format(EXECUTORS[executor]), shell=True)

def command_stop(args):
    executor = get_executor(args)

    print("Executing stop on {0} executor.".format(executor))

    subprocess_command = "docker-compose -f docker-compose-{0}.yml stop"
    subprocess.call(subprocess_command.format(EXECUTORS[executor]), shell=True)

def command_down(args):
    executor = get_executor(args)

    print("Executing down on {0} executor.".format(executor))

    subprocess_command = "docker-compose -f docker-compose-{0}.yml down"
    subprocess.call(subprocess_command.format(EXECUTORS[executor]), shell=True)

def command_jupyter(args):
    port = "8888"
    url = "http://127.0.0.1:{}".format(port)
    c_call = "docker exec -itd docker-airflow_{0}".format(CONTAINERS['webserver'])
    jupyter_pkill = "".join([
        c_call,
        " pkill -f jupyter"
    ])
    jupyter_exec = "".join([
        c_call,
        " jupyter notebook --no-browser ",
        "--port {} ".format(port),
        "--ip=0.0.0.0 ",
        "--NotebookApp.token=''"
    ])

    print('Starting Jupyter NB within the webserver environment on ' + url)

    subprocess.call(jupyter_pkill, shell=True)
    subprocess.call(jupyter_exec, shell=True)

    try:
        import webbrowser
        time.sleep(2)
        webbrowser.open(url, new=2)
    except ImportError:
        pass

def command_garbage_collect(args):
    garbage_collect()
def command_pull_latest_image(args):
    print("Pulling latest from docker hub")
    subprocess.call('docker pull mrmaksimize/airflow:latest', shell=True)

def command_rebuild_image(args):
    if args.image:
        image = args.image
    else:
        image = "mrmaksimize/airflow"

    execute_command = raw_input("DID YOU REMOVE THE IMAGE FIRST??? y/[n]: ")

    if execute_command == 'y' or execute_command == 'Y':
        subprocess.call(
            "docker build --rm --no-cache -t {0} .".format(image), shell=True)
    else:
        print("Delete the image before continuing. Not executing command.")

def command_remove_image(args):
    if args.image:
        subprocess.call('docker rmi -f ' + args.image, shell=True)
    else:
        raise Exception('An image id or image name is needed.')

def command_kill_all_containers(args):
    for container in CONTAINERS.values():
        kill_container(container)

    garbage_collect()

def command_setup(args):

    menv = args.menv or 'mac'
    subprocess.call('rm -rf .env && cp ' + menv + '.env .env')

def command_ssh(args):
    container = get_container(args)

    print("Opening connection to local {0} container".format(CONTAINERS[container]))
    subprocess_command = "docker exec -it dockerairflow_{0} /bin/bash".format(CONTAINERS[container])
    subprocess.call(subprocess_command , shell=True)

def build_argparser():
    usage = '''
    Docker command wrapper for docker-airflow

    %(prog)s [options] <command> [options] [executor]

    options:

        -v              Version
        -h, --help      Display this usage text.

    Commands:
        down
        garbage_collect
        jupyter
        kill_all_containers
        pull_latest_image
        rebuild_image
        remove_image
        setup
        start
        stop
        ssh
        up

    '''

    arg_parser = argparse.ArgumentParser(
        prog='commander',
        usage=usage
    )

    arg_parser.add_argument(
        '-v', '--version',
        action='version',
        version='%(prog)s ' + __VERSION__
    )

    subparsers = arg_parser.add_subparsers(
        title='Commander subcommands',
        description='valid subcommands',
        help='additional help'
    )


    garbage_collet_parser = subparsers.add_parser('garbage_collect',
                                                  help='Clean up the Docker environment')
    garbage_collet_parser.set_defaults(func=command_garbage_collect)

    jupyter_parser = subparsers.add_parser('jupyter', help='execute a Jupyter NB')
    jupyter_parser.set_defaults(func=command_jupyter)

    kill_all_containers_parser = subparsers.add_parser('kill_all_containers',
                                                       help='Kills all airflow containers')
    kill_all_containers_parser.set_defaults(func=command_kill_all_containers)

    pull_latest_image_parser = subparsers.add_parser('pull_latest_image',
                                                     help='Pull the latest Docker image')
    pull_latest_image_parser.set_defaults(func=command_pull_latest_image)

    rebuild_image_parser = subparsers.add_parser('rebuild_image',
                                                 help='Rebuild Docker image')
    rebuild_image_parser.add_argument('image', help="Docker image name to built.")
    rebuild_image_parser.set_defaults(func=command_rebuild_image)

    remove_image_parser = subparsers.add_parser('remove_image',
                                                help='Remove specified Docker image')
    remove_image_parser.add_argument('image', help="Docker image name or id to be removed")
    remove_image_parser.set_defaults(func=command_remove_image)

    setup_parser = subparsers.add_parser('setup', help='Setup environment')
    setup_parser.add_argument('menv', help="Machine environment.")
    setup_parser.set_defaults(func=command_setup)

    ssh_parser = subparsers.add_parser('ssh', help='connect to a specified container')
    ssh_parser.add_argument(
        'container',
        help="".join(
            ["Container to ssh into. Valid values: ".join(CONTAINERS.keys())]))

    ssh_parser.set_defaults(func=command_ssh)

    docker_commands_help = 'Executes docker-compose {0} for a specified executor'
    docker_commands_arg_help = "".join([
        "Specifies which docker-compose file to use. Valid values: ".join(
            EXECUTORS.keys())
    ])

    down_parser = subparsers.add_parser('down', help=docker_commands_help.format('down'))
    down_parser.add_argument('executor', help=docker_commands_arg_help)
    down_parser.set_defaults(func=command_down)

    start_parser = subparsers.add_parser('start', help=docker_commands_help.format('start'))
    start_parser.add_argument('executor', help=docker_commands_arg_help)
    start_parser.set_defaults(func=command_start)

    stop_parser = subparsers.add_parser('stop', help=docker_commands_help.format('stop'))
    stop_parser.add_argument('executor', help=docker_commands_arg_help)
    stop_parser.set_defaults(func=command_stop)

    up_parser = subparsers.add_parser('up', help=docker_commands_help.format('up'))
    up_parser.add_argument('executor', help=docker_commands_arg_help)
    up_parser.set_defaults(func=command_up)

    return arg_parser

if __name__ == '__main__':
    arg_parser = build_argparser()

    try:
        arguments = arg_parser.parse_args(sys.argv[1:])

        arguments.func(arguments)

        print("Done.")
    except Exception as e:
        print('There was an unforseen error: ', e)
        arg_parser.print_help()
