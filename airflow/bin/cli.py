#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from __future__ import print_function
import errno
import hashlib
import importlib
import locale
import logging

import os
import platform
import subprocess
import textwrap
import random
import string
import yaml
from collections import OrderedDict
from importlib import import_module

import getpass

import reprlib
import argparse

import requests
import tenacity
from builtins import input
from tempfile import NamedTemporaryFile

import json
from tabulate import tabulate

import daemon
from daemon.pidfile import TimeoutPIDLockFile
import io
import psutil
import re
import signal
import sys
import threading
import time
import traceback

from typing import Any

import airflow
from airflow import api
from airflow import jobs, settings
from airflow.configuration import conf, get_airflow_home
from airflow.exceptions import AirflowException, AirflowWebServerTimeout
from airflow.executors import get_default_executor
from airflow.models import (
    Connection, DagModel, DagBag, DagPickle, TaskInstance, DagRun, Variable, DAG
)
from airflow.ti_deps.dep_context import (DepContext, SCHEDULER_QUEUED_DEPS)
from airflow.typing_compat import Protocol
from airflow.upgrade.checker import check_upgrade
from airflow.upgrade.formatters import (ConsoleFormatter, JSONFormatter)
from airflow.utils import cli as cli_utils, db
from airflow.utils.dot_renderer import render_dag
from airflow.utils.net import get_hostname
from airflow.utils.timezone import parse as parsedate
from airflow.utils.log.logging_mixin import (LoggingMixin, redirect_stderr,
                                             redirect_stdout)
from airflow.www.app import (cached_app, create_app)
from airflow.www_rbac.app import cached_app as cached_app_rbac
from airflow.www_rbac.app import create_app as create_app_rbac
from airflow.www_rbac.app import cached_appbuilder
from airflow.version import version as airflow_version

import pygments
from pygments.formatters.terminal import TerminalFormatter
from pygments.lexers.configs import IniLexer
from sqlalchemy.orm import exc
import six
from six.moves.urllib_parse import urlunparse, urlsplit, urlunsplit

api.load_auth()
api_module = import_module(conf.get('cli', 'api_client'))  # type: Any
api_client = api_module.Client(api_base_url=conf.get('cli', 'endpoint_url'),
                               auth=api.API_AUTH.api_auth.CLIENT_AUTH)

log = logging.getLogger(__name__)

DAGS_FOLDER = settings.DAGS_FOLDER

if "BUILDING_AIRFLOW_DOCS" in os.environ:
    DAGS_FOLDER = '[AIRFLOW_HOME]/dags'


def sigint_handler(sig, frame):
    sys.exit(0)


def sigquit_handler(sig, frame):
    """Helps debug deadlocks by printing stacktraces when this gets a SIGQUIT
    e.g. kill -s QUIT <PID> or CTRL+\
    """
    print("Dumping stack traces for all threads in PID {}".format(os.getpid()))
    id_to_name = dict([(th.ident, th.name) for th in threading.enumerate()])
    code = []
    for thread_id, stack in sys._current_frames().items():
        code.append("\n# Thread: {}({})"
                    .format(id_to_name.get(thread_id, ""), thread_id))
        for filename, line_number, name, line in traceback.extract_stack(stack):
            code.append('File: "{}", line {}, in {}'
                        .format(filename, line_number, name))
            if line:
                code.append("  {}".format(line.strip()))
    print("\n".join(code))


def setup_logging(filename):
    root = logging.getLogger()
    handler = logging.FileHandler(filename)
    formatter = logging.Formatter(settings.SIMPLE_LOG_FORMAT)
    handler.setFormatter(formatter)
    root.addHandler(handler)
    root.setLevel(settings.LOGGING_LEVEL)

    return handler.stream


def setup_locations(process, pid=None, stdout=None, stderr=None, log=None):
    if not stderr:
        stderr = os.path.join(settings.AIRFLOW_HOME, 'airflow-{}.err'.format(process))
    if not stdout:
        stdout = os.path.join(settings.AIRFLOW_HOME, 'airflow-{}.out'.format(process))
    if not log:
        log = os.path.join(settings.AIRFLOW_HOME, 'airflow-{}.log'.format(process))
    if not pid:
        pid = os.path.join(settings.AIRFLOW_HOME, 'airflow-{}.pid'.format(process))

    return pid, stdout, stderr, log


def process_subdir(subdir):
    if subdir:
        subdir = subdir.replace('DAGS_FOLDER', DAGS_FOLDER)
        subdir = os.path.abspath(os.path.expanduser(subdir))
        return subdir


def get_dag(args):
    dagbag = DagBag(process_subdir(args.subdir))
    if args.dag_id not in dagbag.dags:
        raise AirflowException(
            'dag_id could not be found: {}. Either the dag did not exist or it failed to '
            'parse.'.format(args.dag_id))
    return dagbag.dags[args.dag_id]


def get_dags(args):
    if not args.dag_regex:
        return [get_dag(args)]
    dagbag = DagBag(process_subdir(args.subdir))
    matched_dags = [dag for dag in dagbag.dags.values() if re.search(
        args.dag_id, dag.dag_id)]
    if not matched_dags:
        raise AirflowException(
            'dag_id could not be found with regex: {}. Either the dag did not exist '
            'or it failed to parse.'.format(args.dag_id))
    return matched_dags


@cli_utils.action_logging
def backfill(args, dag=None):
    logging.basicConfig(
        level=settings.LOGGING_LEVEL,
        format=settings.SIMPLE_LOG_FORMAT)

    dag = dag or get_dag(args)

    if not args.start_date and not args.end_date:
        raise AirflowException("Provide a start_date and/or end_date")

    # If only one date is passed, using same as start and end
    args.end_date = args.end_date or args.start_date
    args.start_date = args.start_date or args.end_date

    if args.task_regex:
        dag = dag.sub_dag(
            task_regex=args.task_regex,
            include_upstream=not args.ignore_dependencies)

    run_conf = None
    if args.conf:
        run_conf = json.loads(args.conf)

    if args.dry_run:
        print("Dry run of DAG {0} on {1}".format(args.dag_id,
                                                 args.start_date))
        for task in dag.tasks:
            print("Task {0}".format(task.task_id))
            ti = TaskInstance(task, args.start_date)
            ti.dry_run()
    else:
        if args.reset_dagruns:
            DAG.clear_dags(
                [dag],
                start_date=args.start_date,
                end_date=args.end_date,
                confirm_prompt=not args.yes,
                include_subdags=False,
            )

        dag.run(
            start_date=args.start_date,
            end_date=args.end_date,
            mark_success=args.mark_success,
            local=args.local,
            donot_pickle=(args.donot_pickle or
                          conf.getboolean('core', 'donot_pickle')),
            ignore_first_depends_on_past=args.ignore_first_depends_on_past,
            ignore_task_deps=args.ignore_dependencies,
            pool=args.pool,
            delay_on_limit_secs=args.delay_on_limit,
            verbose=args.verbose,
            conf=run_conf,
            rerun_failed_tasks=args.rerun_failed_tasks,
            run_backwards=args.run_backwards
        )


@cli_utils.action_logging
def trigger_dag(args):
    """
    Creates a dag run for the specified dag

    :param args:
    :return:
    """
    try:
        message = api_client.trigger_dag(dag_id=args.dag_id,
                                         run_id=args.run_id,
                                         conf=args.conf,
                                         execution_date=args.exec_date)
        print(message)
    except IOError as err:
        raise AirflowException(err)


@cli_utils.action_logging
def delete_dag(args):
    """
    Deletes all DB records related to the specified dag

    :param args:
    :return:
    """
    if args.yes or input(
            "This will drop all existing records related to the specified DAG. "
            "Proceed? (y/n)").upper() == "Y":
        try:
            message = api_client.delete_dag(dag_id=args.dag_id)
            print(message)
        except IOError as err:
            raise AirflowException(err)
    else:
        print("Bail.")


@cli_utils.action_logging
def pool(args):
    def _tabulate(pools):
        return "\n%s" % tabulate(pools, ['Pool', 'Slots', 'Description'],
                                 tablefmt="fancy_grid")

    try:
        imp = getattr(args, 'import')
        if args.get is not None:
            pools = [api_client.get_pool(name=args.get)]
        elif args.set:
            pools = [api_client.create_pool(name=args.set[0],
                                            slots=args.set[1],
                                            description=args.set[2])]
        elif args.delete:
            pools = [api_client.delete_pool(name=args.delete)]
        elif imp:
            if os.path.exists(imp):
                pools = pool_import_helper(imp)
            else:
                print("Missing pools file.")
                pools = api_client.get_pools()
        elif args.export:
            pools = pool_export_helper(args.export)
        else:
            pools = api_client.get_pools()
    except (AirflowException, IOError) as err:
        print(err)
    else:
        print(_tabulate(pools=pools))


def pool_import_helper(filepath):
    with open(filepath, 'r') as poolfile:
        pl = poolfile.read()
    try:
        d = json.loads(pl)
    except Exception as e:
        print("Please check the validity of the json file: " + str(e))
    else:
        try:
            pools = []
            n = 0
            for k, v in d.items():
                if isinstance(v, dict) and len(v) == 2:
                    pools.append(api_client.create_pool(name=k,
                                                        slots=v["slots"],
                                                        description=v["description"]))
                    n += 1
                else:
                    pass
        except Exception:
            pass
        finally:
            print("{} of {} pool(s) successfully updated.".format(n, len(d)))
            return pools


def pool_export_helper(filepath):
    pool_dict = {}
    pools = api_client.get_pools()
    for pool in pools:
        pool_dict[pool[0]] = {"slots": pool[1], "description": pool[2]}
    with open(filepath, 'w') as poolfile:
        poolfile.write(json.dumps(pool_dict, sort_keys=True, indent=4))
    print("{} pools successfully exported to {}".format(len(pool_dict), filepath))
    return pools


@cli_utils.action_logging
def variables(args):
    if args.get:
        try:
            var = Variable.get(args.get,
                               deserialize_json=args.json,
                               default_var=args.default)
            print(var)
        except ValueError as e:
            print(e)
    if args.delete:
        Variable.delete(args.delete)
    if args.set:
        Variable.set(args.set[0], args.set[1])
    # Work around 'import' as a reserved keyword
    imp = getattr(args, 'import')
    if imp:
        if os.path.exists(imp):
            import_helper(imp)
        else:
            print("Missing variables file.")
    if args.export:
        export_helper(args.export)
    if not (args.set or args.get or imp or args.export or args.delete):
        # list all variables
        with db.create_session() as session:
            vars = session.query(Variable)
            msg = "\n".join(var.key for var in vars)
            print(msg)


def import_helper(filepath):
    with open(filepath, 'r') as varfile:
        var = varfile.read()

    try:
        d = json.loads(var)
    except Exception:
        print("Invalid variables file.")
    else:
        suc_count = fail_count = 0
        for k, v in d.items():
            try:
                Variable.set(k, v, serialize_json=not isinstance(v, six.string_types))
            except Exception as e:
                print('Variable import failed: {}'.format(repr(e)))
                fail_count += 1
            else:
                suc_count += 1
        print("{} of {} variables successfully updated.".format(suc_count, len(d)))
        if fail_count:
            print("{} variable(s) failed to be updated.".format(fail_count))


def export_helper(filepath):
    var_dict = {}
    with db.create_session() as session:
        qry = session.query(Variable).all()

        d = json.JSONDecoder()
        for var in qry:
            try:
                val = d.decode(var.val)
            except Exception:
                val = var.val
            var_dict[var.key] = val

    with open(filepath, 'w') as varfile:
        varfile.write(json.dumps(var_dict, sort_keys=True, indent=4))
    print("{} variables successfully exported to {}".format(len(var_dict), filepath))


@cli_utils.action_logging
def pause(args):
    set_is_paused(True, args)


@cli_utils.action_logging
def unpause(args):
    set_is_paused(False, args)


def set_is_paused(is_paused, args):
    DagModel.get_dagmodel(args.dag_id).set_is_paused(
        is_paused=is_paused,
    )

    print("Dag: {}, paused: {}".format(args.dag_id, str(is_paused)))


def show_dag(args):
    dag = get_dag(args)
    dot = render_dag(dag)
    if args.save:
        filename, _, fileformat = args.save.rpartition('.')
        dot.render(filename=filename, format=fileformat, cleanup=True)
        print("File {} saved".format(args.save))
    elif args.imgcat:
        data = dot.pipe(format='png')
        try:
            proc = subprocess.Popen("imgcat", stdout=subprocess.PIPE, stdin=subprocess.PIPE)
        except OSError as e:
            if e.errno == errno.ENOENT:
                raise AirflowException(
                    "Failed to execute. Make sure the imgcat executables are on your systems \'PATH\'"
                )
            else:
                raise
        out, err = proc.communicate(data)
        if out:
            print(out.decode('utf-8'))
        if err:
            print(err.decode('utf-8'))
    else:
        print(dot.source)


def _run(args, dag, ti):
    if args.local:
        run_job = jobs.LocalTaskJob(
            task_instance=ti,
            mark_success=args.mark_success,
            pickle_id=args.pickle,
            ignore_all_deps=args.ignore_all_dependencies,
            ignore_depends_on_past=args.ignore_depends_on_past,
            ignore_task_deps=args.ignore_dependencies,
            ignore_ti_state=args.force,
            pool=args.pool)
        run_job.run()
    elif args.raw:
        ti._run_raw_task(
            mark_success=args.mark_success,
            job_id=args.job_id,
            pool=args.pool,
        )
    else:
        pickle_id = None
        if args.ship_dag:
            try:
                # Running remotely, so pickling the DAG
                with db.create_session() as session:
                    pickle = DagPickle(dag)
                    session.add(pickle)
                    pickle_id = pickle.id
                    # TODO: This should be written to a log
                    print('Pickled dag {dag} as pickle_id: {pickle_id}'.format(
                        dag=dag, pickle_id=pickle_id))
            except Exception as e:
                print('Could not pickle the DAG')
                print(e)
                raise e

        executor = get_default_executor()
        executor.start()
        print("Sending to executor.")
        executor.queue_task_instance(
            ti,
            mark_success=args.mark_success,
            pickle_id=pickle_id,
            ignore_all_deps=args.ignore_all_dependencies,
            ignore_depends_on_past=args.ignore_depends_on_past,
            ignore_task_deps=args.ignore_dependencies,
            ignore_ti_state=args.force,
            pool=args.pool)
        executor.heartbeat()
        executor.end()


@cli_utils.action_logging
def run(args, dag=None):
    if dag:
        args.dag_id = dag.dag_id

    # Load custom airflow config
    if args.cfg_path:
        with open(args.cfg_path, 'r') as conf_file:
            conf_dict = json.load(conf_file)

        if os.path.exists(args.cfg_path):
            os.remove(args.cfg_path)

        conf.read_dict(conf_dict, source=args.cfg_path)
        settings.configure_vars()

    # IMPORTANT, have to use the NullPool, otherwise, each "run" command may leave
    # behind multiple open sleeping connections while heartbeating, which could
    # easily exceed the database connection limit when
    # processing hundreds of simultaneous tasks.
    settings.configure_orm(disable_connection_pool=True)

    if not args.pickle and not dag:
        dag = get_dag(args)
    elif not dag:
        with db.create_session() as session:
            print('Loading pickle id %s', args.pickle)
            dag_pickle = session.query(DagPickle).filter(DagPickle.id == args.pickle).first()
            if not dag_pickle:
                raise AirflowException("Who hid the pickle!? [missing pickle]")
            dag = dag_pickle.pickle

    task = dag.get_task(task_id=args.task_id)
    ti = TaskInstance(task, args.execution_date)
    ti.refresh_from_db()

    ti.init_run_context(raw=args.raw)

    hostname = get_hostname()
    print("Running %s on host %s", ti, hostname)

    if args.interactive:
        _run(args, dag, ti)
    else:
        if settings.DONOT_MODIFY_HANDLERS:
            with redirect_stdout(ti.log, logging.INFO), redirect_stderr(ti.log, logging.WARN):
                _run(args, dag, ti)
        else:
            # Get all the Handlers from 'airflow.task' logger
            # Add these handlers to the root logger so that we can get logs from
            # any custom loggers defined in the DAG
            airflow_logger_handlers = logging.getLogger('airflow.task').handlers
            root_logger = logging.getLogger()
            root_logger_handlers = root_logger.handlers

            # Remove all handlers from Root Logger to avoid duplicate logs
            for handler in root_logger_handlers:
                root_logger.removeHandler(handler)

            for handler in airflow_logger_handlers:
                root_logger.addHandler(handler)
            root_logger.setLevel(logging.getLogger('airflow.task').level)

            with redirect_stdout(ti.log, logging.INFO), redirect_stderr(ti.log, logging.WARN):
                _run(args, dag, ti)

            # We need to restore the handlers to the loggers as celery worker process
            # can call this command multiple times,
            # so if we don't reset this then logs from next task would go to the wrong place
            for handler in airflow_logger_handlers:
                root_logger.removeHandler(handler)
            for handler in root_logger_handlers:
                root_logger.addHandler(handler)
    logging.shutdown()


@cli_utils.action_logging
def task_failed_deps(args):
    """
    Returns the unmet dependencies for a task instance from the perspective of the
    scheduler (i.e. why a task instance doesn't get scheduled and then queued by the
    scheduler, and then run by an executor).
    >>> airflow task_failed_deps tutorial sleep 2015-01-01
    Task instance dependencies not met:
    Dagrun Running: Task instance's dagrun did not exist: Unknown reason
    Trigger Rule: Task's trigger rule 'all_success' requires all upstream tasks
    to have succeeded, but found 1 non-success(es).
    """
    dag = get_dag(args)
    task = dag.get_task(task_id=args.task_id)
    ti = TaskInstance(task, args.execution_date)

    dep_context = DepContext(deps=SCHEDULER_QUEUED_DEPS)
    failed_deps = list(ti.get_failed_dep_statuses(dep_context=dep_context))
    # TODO, Do we want to print or log this
    if failed_deps:
        print("Task instance dependencies not met:")
        for dep in failed_deps:
            print("{}: {}".format(dep.dep_name, dep.reason))
    else:
        print("Task instance dependencies are all met.")


@cli_utils.action_logging
def task_state(args):
    """
    Returns the state of a TaskInstance at the command line.
    >>> airflow task_state tutorial sleep 2015-01-01
    success
    """
    dag = get_dag(args)
    task = dag.get_task(task_id=args.task_id)
    ti = TaskInstance(task, args.execution_date)
    print(ti.current_state())


@cli_utils.action_logging
def dag_state(args):
    """
    Returns the state of a DagRun at the command line.
    >>> airflow dag_state tutorial 2015-01-01T00:00:00.000000
    running
    """
    dag = get_dag(args)
    dr = DagRun.find(dag.dag_id, execution_date=args.execution_date)
    print(dr[0].state if len(dr) > 0 else None)


@cli_utils.action_logging
def next_execution(args):
    """
    Returns the next execution datetime of a DAG at the command line.
    >>> airflow next_execution tutorial
    2018-08-31 10:38:00
    """
    dag = get_dag(args)

    if dag.is_paused:
        print("[INFO] Please be reminded this DAG is PAUSED now.")

    if dag.latest_execution_date:
        next_execution_dttm = dag.following_schedule(dag.latest_execution_date)

        if next_execution_dttm is None:
            print("[WARN] No following schedule can be found. " +
                  "This DAG may have schedule interval '@once' or `None`.")

        print(next_execution_dttm)
    else:
        print("[WARN] Only applicable when there is execution record found for the DAG.")
        print(None)


@cli_utils.action_logging
def rotate_fernet_key(args):
    session = settings.Session()
    for conn in session.query(Connection).filter(
            Connection.is_encrypted | Connection.is_extra_encrypted):
        conn.rotate_fernet_key()
    for var in session.query(Variable).filter(Variable.is_encrypted):
        var.rotate_fernet_key()
    session.commit()


@cli_utils.action_logging
def list_dags(args):
    dagbag = DagBag(process_subdir(args.subdir))
    s = textwrap.dedent("""\n
    -------------------------------------------------------------------
    DAGS
    -------------------------------------------------------------------
    {dag_list}
    """)
    dag_list = "\n".join(sorted(dagbag.dags))
    print(s.format(dag_list=dag_list))
    if args.report:
        print(dagbag.dagbag_report())


@cli_utils.action_logging
def list_tasks(args, dag=None):
    dag = dag or get_dag(args)
    if args.tree:
        dag.tree_view()
    else:
        tasks = sorted([t.task_id for t in dag.tasks])
        print("\n".join(sorted(tasks)))


@cli_utils.action_logging
def test(args, dag=None):
    # We want log outout from operators etc to show up here. Normally
    # airflow.task would redirect to a file, but here we want it to propagate
    # up to the normal airflow handler.

    dag = dag or get_dag(args)

    task = dag.get_task(task_id=args.task_id)
    # Add CLI provided task_params to task.params
    if args.task_params:
        passed_in_params = json.loads(args.task_params)
        task.params.update(passed_in_params)
    ti = TaskInstance(task, args.execution_date)

    try:
        logging.getLogger('airflow.task').propagate = True
        if args.dry_run:
            ti.dry_run()
        else:
            ti.run(ignore_task_deps=True, ignore_ti_state=True, test_mode=True)
    except Exception:
        if args.post_mortem:
            try:
                debugger = importlib.import_module("ipdb")
            except ImportError:
                debugger = importlib.import_module("pdb")
            debugger.post_mortem()
        else:
            raise
    finally:
        # Make sure to reset back to normal. When run for CLI this doesn't
        # matter, but it does for test suite
        logging.getLogger('airflow.task').propagate = False


@cli_utils.action_logging
def render(args):
    dag = get_dag(args)
    task = dag.get_task(task_id=args.task_id)
    ti = TaskInstance(task, args.execution_date)
    ti.render_templates()
    for attr in task.__class__.template_fields:
        print(textwrap.dedent("""\
        # ----------------------------------------------------------
        # property: {}
        # ----------------------------------------------------------
        {}
        """.format(attr, getattr(task, attr))))


@cli_utils.action_logging
def clear(args):
    logging.basicConfig(
        level=settings.LOGGING_LEVEL,
        format=settings.SIMPLE_LOG_FORMAT)
    dags = get_dags(args)

    if args.task_regex:
        for idx, dag in enumerate(dags):
            dags[idx] = dag.sub_dag(
                task_regex=args.task_regex,
                include_downstream=args.downstream,
                include_upstream=args.upstream)

    DAG.clear_dags(
        dags,
        start_date=args.start_date,
        end_date=args.end_date,
        only_failed=args.only_failed,
        only_running=args.only_running,
        confirm_prompt=not args.no_confirm,
        include_subdags=not args.exclude_subdags,
        include_parentdag=not args.exclude_parentdag,
    )


class GunicornMonitor(LoggingMixin):
    """
    Runs forever, monitoring the child processes of @gunicorn_master_proc and
    restarting workers occasionally or when files in the plug-in directory
    has been modified.
    Each iteration of the loop traverses one edge of this state transition
    diagram, where each state (node) represents
    [ num_ready_workers_running / num_workers_running ]. We expect most time to
    be spent in [n / n]. `bs` is the setting webserver.worker_refresh_batch_size.
    The horizontal transition at ? happens after the new worker parses all the
    dags (so it could take a while!)
       V ────────────────────────────────────────────────────────────────────────┐
    [n / n] ──TTIN──> [ [n, n+bs) / n + bs ]  ────?───> [n + bs / n + bs] ──TTOU─┘
       ^                          ^───────────────┘
       │
       │      ┌────────────────v
       └──────┴────── [ [0, n) / n ] <─── start
    We change the number of workers by sending TTIN and TTOU to the gunicorn
    master process, which increases and decreases the number of child workers
    respectively. Gunicorn guarantees that on TTOU workers are terminated
    gracefully and that the oldest worker is terminated.

    :param gunicorn_master_pid: pid of the main Gunicorn process
    :param num_workers_expected: Number of workers to run the Gunicorn web server
    :param master_timeout: Number of seconds the webserver waits before killing gunicorn master that
        doesn't respond
    :param worker_refresh_interval: Number of seconds to wait before refreshing a batch of workers.
    :param worker_refresh_batch_size: Number of workers to refresh at a time. When set to 0, worker
        refresh is disabled. When nonzero, airflow periodically refreshes webserver workers by
        bringing up new ones and killing old ones.
    :param reload_on_plugin_change: If set to True, Airflow will track files in plugins_follder directory.
        When it detects changes, then reload the gunicorn.
    """
    def __init__(
        self,
        gunicorn_master_pid,
        num_workers_expected,
        master_timeout,
        worker_refresh_interval,
        worker_refresh_batch_size,
        reload_on_plugin_change
    ):
        super(GunicornMonitor, self).__init__()
        self.gunicorn_master_proc = psutil.Process(gunicorn_master_pid)
        self.num_workers_expected = num_workers_expected
        self.master_timeout = master_timeout
        self.worker_refresh_interval = worker_refresh_interval
        self.worker_refresh_batch_size = worker_refresh_batch_size
        self.reload_on_plugin_change = reload_on_plugin_change

        self._num_workers_running = 0
        self._num_ready_workers_running = 0
        self._last_refresh_time = time.time() if worker_refresh_interval > 0 else None
        self._last_plugin_state = self._generate_plugin_state() if reload_on_plugin_change else None
        self._restart_on_next_plugin_check = False

    def _generate_plugin_state(self):
        """
        Generate dict of filenames and last modification time of all files in settings.PLUGINS_FOLDER
        directory.
        """
        if not settings.PLUGINS_FOLDER:
            return {}

        all_filenames = []
        for (root, _, filenames) in os.walk(settings.PLUGINS_FOLDER):
            all_filenames.extend(os.path.join(root, f) for f in filenames)
        plugin_state = {f: self._get_file_hash(f) for f in sorted(all_filenames)}
        return plugin_state

    @staticmethod
    def _get_file_hash(fname):
        """Calculate MD5 hash for file"""
        hash_md5 = hashlib.md5()
        with open(fname, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

    def _get_num_ready_workers_running(self):
        """Returns number of ready Gunicorn workers by looking for READY_PREFIX in process name"""
        workers = psutil.Process(self.gunicorn_master_proc.pid).children()

        def ready_prefix_on_cmdline(proc):
            try:
                cmdline = proc.cmdline()
                if len(cmdline) > 0:  # pylint: disable=len-as-condition
                    return settings.GUNICORN_WORKER_READY_PREFIX in cmdline[0]
            except psutil.NoSuchProcess:
                pass
            return False

        ready_workers = [proc for proc in workers if ready_prefix_on_cmdline(proc)]
        return len(ready_workers)

    def _get_num_workers_running(self):
        """Returns number of running Gunicorn workers processes"""
        workers = psutil.Process(self.gunicorn_master_proc.pid).children()
        return len(workers)

    def _wait_until_true(self, fn, timeout=0):
        """
        Sleeps until fn is true
        """
        start_time = time.time()
        while not fn():
            if 0 < timeout <= time.time() - start_time:
                raise AirflowWebServerTimeout(
                    "No response from gunicorn master within {0} seconds".format(timeout)
                )
            time.sleep(0.1)

    def _spawn_new_workers(self, count):
        """
        Send signal to kill the worker.
        :param count: The number of workers to spawn
        """
        excess = 0
        for _ in range(count):
            # TTIN: Increment the number of processes by one
            self.gunicorn_master_proc.send_signal(signal.SIGTTIN)
            excess += 1
            self._wait_until_true(
                lambda: self.num_workers_expected + excess == self._get_num_workers_running(),
                timeout=self.master_timeout
            )

    def _kill_old_workers(self, count):
        """
        Send signal to kill the worker.
        :param count: The number of workers to kill
        """
        for _ in range(count):
            count -= 1
            # TTOU: Decrement the number of processes by one
            self.gunicorn_master_proc.send_signal(signal.SIGTTOU)
            self._wait_until_true(
                lambda: self.num_workers_expected + count == self._get_num_workers_running(),
                timeout=self.master_timeout)

    def _reload_gunicorn(self):
        """
        Send signal to reload the gunciron configuration. When gunciorn receive signals, it reload the
        configuration, start the new worker processes with a new configuration and gracefully
        shutdown older workers.
        """
        # HUP: Reload the configuration.
        self.gunicorn_master_proc.send_signal(signal.SIGHUP)
        time.sleep(1)
        self._wait_until_true(
            lambda: self.num_workers_expected == self._get_num_workers_running(),
            timeout=self.master_timeout
        )

    def start(self):
        """
        Starts monitoring the webserver.
        """
        self.log.debug("Start monitoring gunicorn")
        try:  # pylint: disable=too-many-nested-blocks
            self._wait_until_true(
                lambda: self.num_workers_expected == self._get_num_workers_running(),
                timeout=self.master_timeout
            )
            while True:
                if not self.gunicorn_master_proc.is_running():
                    sys.exit(1)
                self._check_workers()
                # Throttle loop
                time.sleep(1)

        except (AirflowWebServerTimeout, OSError) as err:
            self.log.error(err)
            self.log.error("Shutting down webserver")
            try:
                self.gunicorn_master_proc.terminate()
                self.gunicorn_master_proc.wait()
            finally:
                sys.exit(1)

    def _check_workers(self):
        num_workers_running = self._get_num_workers_running()
        num_ready_workers_running = self._get_num_ready_workers_running()

        # Whenever some workers are not ready, wait until all workers are ready
        if num_ready_workers_running < num_workers_running:
            self.log.debug(
                '[%d / %d] Some workers are starting up, waiting...',
                num_ready_workers_running, num_workers_running
            )
            time.sleep(1)
            return

        # If there are too many workers, then kill a worker gracefully by asking gunicorn to reduce
        # number of workers
        if num_workers_running > self.num_workers_expected:
            excess = min(num_workers_running - self.num_workers_expected, self.worker_refresh_batch_size)
            self.log.debug(
                '[%d / %d] Killing %s workers', num_ready_workers_running, num_workers_running, excess
            )
            self._kill_old_workers(excess)
            return

        # If there are too few workers, start a new worker by asking gunicorn
        # to increase number of workers
        if num_workers_running < self.num_workers_expected:
            self.log.error(
                "[%d / %d] Some workers seem to have died and gunicorn did not restart "
                "them as expected",
                num_ready_workers_running, num_workers_running
            )
            time.sleep(10)
            num_workers_running = self._get_num_workers_running()
            if num_workers_running < self.num_workers_expected:
                new_worker_count = min(
                    num_workers_running - self.worker_refresh_batch_size, self.worker_refresh_batch_size
                )
                self.log.debug(
                    '[%d / %d] Spawning %d workers',
                    num_ready_workers_running, num_workers_running, new_worker_count
                )
                self._spawn_new_workers(num_workers_running)
            return

        # Now the number of running and expected worker should be equal

        # If workers should be restarted periodically.
        if self.worker_refresh_interval > 0 and self._last_refresh_time:
            # and we refreshed the workers a long time ago, refresh the workers
            last_refresh_diff = (time.time() - self._last_refresh_time)
            if self.worker_refresh_interval < last_refresh_diff:
                num_new_workers = self.worker_refresh_batch_size
                self.log.debug(
                    '[%d / %d] Starting doing a refresh. Starting %d workers.',
                    num_ready_workers_running, num_workers_running, num_new_workers
                )
                self._spawn_new_workers(num_new_workers)
                self._last_refresh_time = time.time()
                return

        # if we should check the directory with the plugin,
        if self.reload_on_plugin_change:
            # compare the previous and current contents of the directory
            new_state = self._generate_plugin_state()
            # If changed, wait until its content is fully saved.
            if new_state != self._last_plugin_state:
                self.log.debug(
                    '[%d / %d] Plugins folder changed. The gunicorn will be restarted the next time the '
                    'plugin directory is checked, if there is no change in it.',
                    num_ready_workers_running, num_workers_running
                )
                self._restart_on_next_plugin_check = True
                self._last_plugin_state = new_state
            elif self._restart_on_next_plugin_check:
                self.log.debug(
                    '[%d / %d] Starts reloading the gunicorn configuration.',
                    num_ready_workers_running, num_workers_running
                )
                self._restart_on_next_plugin_check = False
                self._last_refresh_time = time.time()
                self._reload_gunicorn()


@cli_utils.action_logging
def webserver(args):
    py2_deprecation_waring()
    print(settings.HEADER)

    access_logfile = args.access_logfile or conf.get('webserver', 'access_logfile')
    error_logfile = args.error_logfile or conf.get('webserver', 'error_logfile')
    num_workers = args.workers or conf.get('webserver', 'workers')
    worker_timeout = (args.worker_timeout or
                      conf.get('webserver', 'web_server_worker_timeout'))
    ssl_cert = args.ssl_cert or conf.get('webserver', 'web_server_ssl_cert')
    ssl_key = args.ssl_key or conf.get('webserver', 'web_server_ssl_key')
    if not ssl_cert and ssl_key:
        raise AirflowException(
            'An SSL certificate must also be provided for use with ' + ssl_key)
    if ssl_cert and not ssl_key:
        raise AirflowException(
            'An SSL key must also be provided for use with ' + ssl_cert)

    if args.debug:
        print(
            "Starting the web server on port {0} and host {1}.".format(
                args.port, args.hostname))
        if settings.RBAC:
            app, _ = create_app_rbac(None, testing=conf.getboolean('core', 'unit_test_mode'))
        else:
            app = create_app(None, testing=conf.getboolean('core', 'unit_test_mode'))
        app.run(debug=True, use_reloader=not app.config['TESTING'],
                port=args.port, host=args.hostname,
                ssl_context=(ssl_cert, ssl_key) if ssl_cert and ssl_key else None)
    else:
        os.environ['SKIP_DAGS_PARSING'] = 'True'
        app = cached_app_rbac(None) if settings.RBAC else cached_app(None)
        pid, stdout, stderr, log_file = setup_locations(
            "webserver", args.pid, args.stdout, args.stderr, args.log_file)
        os.environ.pop('SKIP_DAGS_PARSING')
        if args.daemon:
            handle = setup_logging(log_file)
            stdout = open(stdout, 'w+')
            stderr = open(stderr, 'w+')

        print(
            textwrap.dedent('''\
                Running the Gunicorn Server with:
                Workers: {num_workers} {workerclass}
                Host: {hostname}:{port}
                Timeout: {worker_timeout}
                Logfiles: {access_logfile} {error_logfile}
                =================================================================\
            '''.format(num_workers=num_workers, workerclass=args.workerclass,
                       hostname=args.hostname, port=args.port,
                       worker_timeout=worker_timeout, access_logfile=access_logfile,
                       error_logfile=error_logfile)))

        run_args = [
            'gunicorn',
            '--workers', str(num_workers),
            '--worker-class', str(args.workerclass),
            '--timeout', str(worker_timeout),
            '--bind', args.hostname + ':' + str(args.port),
            '--name', 'airflow-webserver',
            '--pid', str(pid),
            '--config', 'python:airflow.www.gunicorn_config',
        ]

        if args.access_logfile:
            run_args += ['--access-logfile', str(args.access_logfile)]

        if args.error_logfile:
            run_args += ['--error-logfile', str(args.error_logfile)]

        if args.daemon:
            run_args += ['--daemon']

        if ssl_cert:
            run_args += ['--certfile', ssl_cert, '--keyfile', ssl_key]

        webserver_module = 'www_rbac' if settings.RBAC else 'www'
        run_args += ["airflow." + webserver_module + ".app:cached_app()"]

        gunicorn_master_proc = None

        def kill_proc(signum, _):
            log.info("Received signal: %s. Closing gunicorn.", signum)
            gunicorn_master_proc.terminate()
            gunicorn_master_proc.wait()
            sys.exit(0)

        def monitor_gunicorn(gunicorn_master_pid):
            # These run forever until SIG{INT, TERM, KILL, ...} signal is sent
            GunicornMonitor(
                gunicorn_master_pid=gunicorn_master_pid,
                num_workers_expected=num_workers,
                master_timeout=conf.getint('webserver', 'web_server_master_timeout'),
                worker_refresh_interval=conf.getint('webserver', 'worker_refresh_interval', fallback=30),
                worker_refresh_batch_size=conf.getint('webserver', 'worker_refresh_batch_size', fallback=1),
                reload_on_plugin_change=conf.getboolean(
                    'webserver', 'reload_on_plugin_change', fallback=False
                ),
            ).start()

        if args.daemon:
            base, ext = os.path.splitext(pid)
            ctx = daemon.DaemonContext(
                pidfile=TimeoutPIDLockFile(base + "-monitor" + ext, -1),
                files_preserve=[handle],
                stdout=stdout,
                stderr=stderr,
                signal_map={
                    signal.SIGINT: kill_proc,
                    signal.SIGTERM: kill_proc
                },
            )
            with ctx:
                subprocess.Popen(run_args, close_fds=True)

                # Reading pid file directly, since Popen#pid doesn't
                # seem to return the right value with DaemonContext.
                while True:
                    try:
                        with open(pid) as f:
                            gunicorn_master_proc_pid = int(f.read())
                            break
                    except IOError:
                        log.debug("Waiting for gunicorn's pid file to be created.")
                        time.sleep(0.1)

                gunicorn_master_proc = psutil.Process(gunicorn_master_proc_pid)
                monitor_gunicorn(gunicorn_master_proc.pid)

            stdout.close()
            stderr.close()
        else:
            gunicorn_master_proc = subprocess.Popen(run_args, close_fds=True)

            signal.signal(signal.SIGINT, kill_proc)
            signal.signal(signal.SIGTERM, kill_proc)

            monitor_gunicorn(gunicorn_master_proc.pid)


@cli_utils.action_logging
def scheduler(args):
    py2_deprecation_waring()
    print(settings.HEADER)
    job = jobs.SchedulerJob(
        dag_id=args.dag_id,
        subdir=process_subdir(args.subdir),
        run_duration=args.run_duration,
        num_runs=args.num_runs,
        do_pickle=args.do_pickle)

    if args.daemon:
        pid, stdout, stderr, log_file = setup_locations("scheduler",
                                                        args.pid,
                                                        args.stdout,
                                                        args.stderr,
                                                        args.log_file)
        handle = setup_logging(log_file)
        stdout = open(stdout, 'w+')
        stderr = open(stderr, 'w+')

        ctx = daemon.DaemonContext(
            pidfile=TimeoutPIDLockFile(pid, -1),
            files_preserve=[handle],
            stdout=stdout,
            stderr=stderr,
        )
        with ctx:
            job.run()

        stdout.close()
        stderr.close()
    else:
        signal.signal(signal.SIGINT, sigint_handler)
        signal.signal(signal.SIGTERM, sigint_handler)
        signal.signal(signal.SIGQUIT, sigquit_handler)
        job.run()


@cli_utils.action_logging
def serve_logs(args):
    print("Starting flask")
    import flask
    flask_app = flask.Flask(__name__)

    @flask_app.route('/log/<path:filename>')
    def serve_logs(filename):  # noqa
        log = os.path.expanduser(conf.get('core', 'BASE_LOG_FOLDER'))
        return flask.send_from_directory(
            log,
            filename,
            mimetype="application/json",
            as_attachment=False)

    worker_log_server_port = int(conf.get('celery', 'WORKER_LOG_SERVER_PORT'))
    flask_app.run(host='0.0.0.0', port=worker_log_server_port)


def _serve_logs(env, skip_serve_logs=False):
    """Starts serve_logs sub-process"""
    if skip_serve_logs is False:
        sub_proc = subprocess.Popen(['airflow', 'serve_logs'], env=env, close_fds=True)
        return sub_proc
    return None


@cli_utils.action_logging
def kubernetes_generate_dag_yaml(args):
    from airflow.executors.kubernetes_executor import AirflowKubernetesScheduler, KubeConfig
    from airflow.kubernetes.pod_generator import PodGenerator
    from airflow.kubernetes.worker_configuration import WorkerConfiguration
    from kubernetes.client.api_client import ApiClient
    dag = get_dag(args)
    yaml_output_path = args.output_path
    kube_config = KubeConfig()
    for task in dag.tasks:
        ti = TaskInstance(task, args.execution_date)
        pod = PodGenerator.construct_pod(
            dag_id=args.dag_id,
            task_id=ti.task_id,
            pod_id=AirflowKubernetesScheduler._create_pod_id(  # pylint: disable=W0212
                args.dag_id, ti.task_id),
            try_number=ti.try_number,
            date=ti.execution_date,
            command=ti.command_as_list(),
            kube_executor_config=PodGenerator.from_obj(ti.executor_config),
            worker_uuid="worker-config",
            namespace=kube_config.executor_namespace,
            worker_config=WorkerConfiguration(kube_config=kube_config).as_pod()
        )
        api_client = ApiClient()
        date_string = AirflowKubernetesScheduler._datetime_to_label_safe_datestring(  # pylint: disable=W0212
            args.execution_date)
        yaml_file_name = "{}_{}_{}.yml".format(args.dag_id, ti.task_id, date_string)
        os.makedirs(os.path.dirname(yaml_output_path + "/airflow_yaml_output/"), exist_ok=True)
        with open(yaml_output_path + "/airflow_yaml_output/" + yaml_file_name, "w") as output:
            sanitized_pod = api_client.sanitize_for_serialization(pod)
            output.write(yaml.dump(sanitized_pod))
    print("YAML output can be found at {}/airflow_yaml_output/".format(yaml_output_path))


@cli_utils.action_logging
def generate_pod_template(args):
    from airflow.executors.kubernetes_executor import KubeConfig
    from airflow.kubernetes.worker_configuration import WorkerConfiguration
    from kubernetes.client.api_client import ApiClient
    kube_config = KubeConfig()
    worker_configuration_pod = WorkerConfiguration(kube_config=kube_config).as_pod()
    api_client = ApiClient()
    yaml_file_name = "airflow_template.yml"
    yaml_output_path = args.output_path
    if not os.path.exists(yaml_output_path):
        os.makedirs(yaml_output_path)
    with open(yaml_output_path + "/" + yaml_file_name, "w") as output:
        sanitized_pod = api_client.sanitize_for_serialization(worker_configuration_pod)
        sanitized_pod = json.dumps(sanitized_pod)
        sanitized_pod = json.loads(sanitized_pod)
        output.write(yaml.safe_dump(sanitized_pod))
    output_string = """
Congratulations on migrating your kubernetes configs to the pod_template_file!

This is a critical first step on your migration to Airflow 2.0.

Please check the following file and ensure that all configurations are correct: {yaml_file_name}


Please place this file in a desired location on your machine and set the following airflow.cfg config:

```
[kubernetes]
    pod_template_file=/path/to/{yaml_file_name}
```

You will now have full access to the Kubernetes API.

Please note that the following configs will no longer be considered when the pod_template_file is set:

worker_container_image_pull_policy
airflow_configmap
airflow_local_settings_configmap
dags_in_image
dags_volume_subpath
dags_volume_mount_point
dags_volume_claim
logs_volume_subpath
logs_volume_claim
dags_volume_host
logs_volume_host
env_from_configmap_ref
env_from_secret_ref
git_repo
git_branch
git_sync_depth
git_subpath
git_sync_rev
git_user
git_password
git_sync_root
git_sync_dest
git_dags_folder_mount_point
git_ssh_key_secret_name
git_ssh_known_hosts_configmap_name
git_sync_credentials_secret
git_sync_container_repository
git_sync_container_tag
git_sync_init_container_name
git_sync_run_as_user
worker_service_account_name
image_pull_secrets
gcp_service_account_keys
affinity
tolerations
run_as_user
fs_group
[kubernetes_node_selectors]
[kubernetes_annotations]
[kubernetes_environment_variables]
[kubernetes_secrets]
[kubernetes_labels]


Happy Airflowing!

""".format(yaml_file_name=yaml_file_name)
    print(output_string)


@cli_utils.action_logging
def worker(args):
    env = os.environ.copy()
    env['AIRFLOW_HOME'] = settings.AIRFLOW_HOME

    if not settings.validate_session():
        log.error("Worker exiting... database connection precheck failed! ")
        sys.exit(1)

    # Celery worker
    from airflow.executors.celery_executor import app as celery_app
    from celery import maybe_patch_concurrency
    from celery.bin import worker

    autoscale = args.autoscale
    skip_serve_logs = args.skip_serve_logs

    if autoscale is None and conf.has_option("celery", "worker_autoscale"):
        autoscale = conf.get("celery", "worker_autoscale")

    worker = worker.worker(app=celery_app)
    options = {
        'optimization': 'fair',
        'O': 'fair',
        'queues': args.queues,
        'concurrency': args.concurrency,
        'autoscale': autoscale,
        'hostname': args.celery_hostname,
        'loglevel': conf.get('core', 'LOGGING_LEVEL'),
    }

    if conf.has_option("celery", "pool"):
        pool = conf.get("celery", "pool")
        options["pool"] = pool
        # Celery pools of type eventlet and gevent use greenlets, which
        # requires monkey patching the app:
        # https://eventlet.net/doc/patching.html#monkey-patch
        # Otherwise task instances hang on the workers and are never
        # executed.
        maybe_patch_concurrency(['-P', pool])

    if args.daemon:
        pid, stdout, stderr, log_file = setup_locations("worker",
                                                        args.pid,
                                                        args.stdout,
                                                        args.stderr,
                                                        args.log_file)
        handle = setup_logging(log_file)
        stdout = open(stdout, 'w+')
        stderr = open(stderr, 'w+')

        ctx = daemon.DaemonContext(
            pidfile=TimeoutPIDLockFile(pid, -1),
            files_preserve=[handle],
            stdout=stdout,
            stderr=stderr,
        )
        with ctx:
            sp = _serve_logs(env, skip_serve_logs)
            worker.run(**options)

        stdout.close()
        stderr.close()
    else:
        signal.signal(signal.SIGINT, sigint_handler)
        signal.signal(signal.SIGTERM, sigint_handler)

        sp = _serve_logs(env, skip_serve_logs)

        worker.run(**options)

    if sp:
        sp.kill()


def initdb(args):  # noqa
    py2_deprecation_waring()
    print("DB: " + repr(settings.engine.url))
    db.initdb(settings.RBAC)
    print("Done.")


def resetdb(args):
    py2_deprecation_waring()
    print("DB: " + repr(settings.engine.url))
    if args.yes or input("This will drop existing tables "
                         "if they exist. Proceed? "
                         "(y/n)").upper() == "Y":
        db.resetdb(settings.RBAC)
    else:
        print("Bail.")


@cli_utils.action_logging
def shell(args):
    """Run a shell that allows to access database access"""
    url = settings.engine.url
    print("DB: " + repr(url))

    if url.get_backend_name() == 'mysql':
        with NamedTemporaryFile(suffix="my.cnf") as f:
            content = textwrap.dedent("""
                [client]
                host     = {}
                user     = {}
                password = {}
                port     = {}
                database = {}
                """.format(url.host, url.username, url.password or "", url.port or "", url.database)).strip()
            f.write(content.encode())
            f.flush()
            subprocess.Popen(["mysql", "--defaults-extra-file={}".format(f.name)]).wait()
    elif url.get_backend_name() == 'sqlite':
        subprocess.Popen(["sqlite3", url.database]).wait()
    elif url.get_backend_name() == 'postgresql':
        env = os.environ.copy()
        env['PGHOST'] = url.host or ""
        env['PGPORT'] = url.port or ""
        env['PGUSER'] = url.username or ""
        # PostgreSQL does not allow the use of PGPASSFILE if the current user is root.
        env["PGPASSWORD"] = url.password or ""
        env['PGDATABASE'] = url.database
        subprocess.Popen(["psql"], env=env).wait()
    else:
        raise AirflowException("Unknown driver: {}".format(url.drivername))


@cli_utils.action_logging
def upgradedb(args):  # noqa
    py2_deprecation_waring()
    print("DB: " + repr(settings.engine.url))
    db.upgradedb()


@cli_utils.action_logging
def checkdb(args):  # noqa
    py2_deprecation_waring()
    print("DB: " + repr(settings.engine.url))
    db.checkdb()


def version(args):  # noqa
    py2_deprecation_waring()
    print(airflow.__version__)


alternative_conn_specs = ['conn_type', 'conn_host',
                          'conn_login', 'conn_password', 'conn_schema', 'conn_port']


@cli_utils.action_logging
def connections(args):
    if args.list:
        # Check that no other flags were passed to the command
        invalid_args = list()
        for arg in ['conn_id', 'conn_uri', 'conn_extra'] + alternative_conn_specs:
            if getattr(args, arg) is not None:
                invalid_args.append(arg)
        if invalid_args:
            msg = ('\n\tThe following args are not compatible with the ' +
                   '--list flag: {invalid!r}\n')
            msg = msg.format(invalid=invalid_args)
            print(msg)
            return

        with db.create_session() as session:
            conns = session.query(Connection.conn_id, Connection.conn_type,
                                  Connection.host, Connection.port,
                                  Connection.is_encrypted,
                                  Connection.is_extra_encrypted,
                                  Connection.extra).all()
            conns = [map(reprlib.repr, conn) for conn in conns]
            msg = tabulate(conns, ['Conn Id', 'Conn Type', 'Host', 'Port',
                                   'Is Encrypted', 'Is Extra Encrypted', 'Extra'],
                           tablefmt="fancy_grid")
            if sys.version_info[0] < 3:
                msg = msg.encode('utf-8')
            print(msg)
            return

    if args.delete:
        # Check that only the `conn_id` arg was passed to the command
        invalid_args = list()
        for arg in ['conn_uri', 'conn_extra'] + alternative_conn_specs:
            if getattr(args, arg) is not None:
                invalid_args.append(arg)
        if invalid_args:
            msg = ('\n\tThe following args are not compatible with the ' +
                   '--delete flag: {invalid!r}\n')
            msg = msg.format(invalid=invalid_args)
            print(msg)
            return

        if args.conn_id is None:
            print('\n\tTo delete a connection, you Must provide a value for ' +
                  'the --conn_id flag.\n')
            return

        with db.create_session() as session:
            try:
                to_delete = (session
                             .query(Connection)
                             .filter(Connection.conn_id == args.conn_id)
                             .one())
            except exc.NoResultFound:
                msg = '\n\tDid not find a connection with `conn_id`={conn_id}\n'
                msg = msg.format(conn_id=args.conn_id)
                print(msg)
                return
            except exc.MultipleResultsFound:
                msg = ('\n\tFound more than one connection with ' +
                       '`conn_id`={conn_id}\n')
                msg = msg.format(conn_id=args.conn_id)
                print(msg)
                return
            else:
                deleted_conn_id = to_delete.conn_id
                session.delete(to_delete)
                msg = '\n\tSuccessfully deleted `conn_id`={conn_id}\n'
                msg = msg.format(conn_id=deleted_conn_id)
                print(msg)
            return

    if args.add:
        # Check that the conn_id and conn_uri args were passed to the command:
        missing_args = list()
        invalid_args = list()
        if not args.conn_id:
            missing_args.append('conn_id')
        if args.conn_uri:
            for arg in alternative_conn_specs:
                if getattr(args, arg) is not None:
                    invalid_args.append(arg)
        elif not args.conn_type:
            missing_args.append('conn_uri or conn_type')
        if missing_args:
            msg = ('\n\tThe following args are required to add a connection:' +
                   ' {missing!r}\n'.format(missing=missing_args))
            print(msg)
        if invalid_args:
            msg = ('\n\tThe following args are not compatible with the ' +
                   '--add flag and --conn_uri flag: {invalid!r}\n')
            msg = msg.format(invalid=invalid_args)
            print(msg)
        if missing_args or invalid_args:
            return

        if args.conn_uri:
            new_conn = Connection(conn_id=args.conn_id, uri=args.conn_uri)
        else:
            new_conn = Connection(conn_id=args.conn_id,
                                  conn_type=args.conn_type,
                                  host=args.conn_host,
                                  login=args.conn_login,
                                  password=args.conn_password,
                                  schema=args.conn_schema,
                                  port=args.conn_port)
        if args.conn_extra is not None:
            new_conn.set_extra(args.conn_extra)

        with db.create_session() as session:
            if not (session.query(Connection)
                           .filter(Connection.conn_id == new_conn.conn_id).first()):
                session.add(new_conn)
                msg = '\n\tSuccessfully added `conn_id`={conn_id} : {uri}\n'
                msg = msg.format(conn_id=new_conn.conn_id,
                                 uri=args.conn_uri or
                                 urlunparse((args.conn_type,
                                            '{login}:{password}@{host}:{port}'
                                             .format(login=args.conn_login or '',
                                                     password='******' or '',
                                                     host=args.conn_host or '',
                                                     port=args.conn_port or ''),
                                             args.conn_schema or '', '', '', '')))
                print(msg)
            else:
                msg = '\n\tA connection with `conn_id`={conn_id} already exists\n'
                msg = msg.format(conn_id=new_conn.conn_id)
                print(msg)

        return


@cli_utils.action_logging
def flower(args):
    broka = conf.get('celery', 'BROKER_URL')
    address = '--address={}'.format(args.hostname)
    port = '--port={}'.format(args.port)
    api = ''
    if args.broker_api:
        api = '--broker_api=' + args.broker_api

    url_prefix = ''
    if args.url_prefix:
        url_prefix = '--url-prefix=' + args.url_prefix

    basic_auth = ''
    if args.basic_auth:
        basic_auth = '--basic_auth=' + args.basic_auth

    flower_conf = ''
    if args.flower_conf:
        flower_conf = '--conf=' + args.flower_conf

    if args.daemon:
        pid, stdout, stderr, log_file = setup_locations("flower",
                                                        args.pid,
                                                        args.stdout,
                                                        args.stderr,
                                                        args.log_file)
        stdout = open(stdout, 'w+')
        stderr = open(stderr, 'w+')

        ctx = daemon.DaemonContext(
            pidfile=TimeoutPIDLockFile(pid, -1),
            stdout=stdout,
            stderr=stderr,
        )

        with ctx:
            os.execvp("flower", ['flower', '-b',
                                 broka, address, port, api, flower_conf, url_prefix, basic_auth])

        stdout.close()
        stderr.close()
    else:
        signal.signal(signal.SIGINT, sigint_handler)
        signal.signal(signal.SIGTERM, sigint_handler)

        os.execvp("flower", ['flower', '-b',
                             broka, address, port, api, flower_conf, url_prefix, basic_auth])


@cli_utils.action_logging
def kerberos(args):  # noqa
    print(settings.HEADER)
    import airflow.security.kerberos

    if args.daemon:
        pid, stdout, stderr, log_file = setup_locations("kerberos",
                                                        args.pid,
                                                        args.stdout,
                                                        args.stderr,
                                                        args.log_file)
        stdout = open(stdout, 'w+')
        stderr = open(stderr, 'w+')

        ctx = daemon.DaemonContext(
            pidfile=TimeoutPIDLockFile(pid, -1),
            stdout=stdout,
            stderr=stderr,
        )

        with ctx:
            airflow.security.kerberos.run(principal=args.principal, keytab=args.keytab)

        stdout.close()
        stderr.close()
    else:
        airflow.security.kerberos.run(principal=args.principal, keytab=args.keytab)


@cli_utils.action_logging
def create_user(args):
    fields = {
        'role': args.role,
        'username': args.username,
        'email': args.email,
        'firstname': args.firstname,
        'lastname': args.lastname,
    }
    empty_fields = [k for k, v in fields.items() if not v]
    if empty_fields:
        raise SystemExit('Required arguments are missing: {}.'.format(
            ', '.join(empty_fields)))

    appbuilder = cached_appbuilder()
    role = appbuilder.sm.find_role(args.role)
    if not role:
        raise SystemExit('{} is not a valid role.'.format(args.role))

    if args.use_random_password:
        password = ''.join(random.choice(string.printable) for _ in range(16))
    elif args.password:
        password = args.password
    else:
        password = getpass.getpass('Password:')
        password_confirmation = getpass.getpass('Repeat for confirmation:')
        if password != password_confirmation:
            raise SystemExit('Passwords did not match!')

    if appbuilder.sm.find_user(args.username):
        print('{} already exist in the db'.format(args.username))
        return
    user = appbuilder.sm.add_user(args.username, args.firstname, args.lastname,
                                  args.email, role, password)
    if user:
        print('{} user {} created.'.format(args.role, args.username))
    else:
        raise SystemExit('Failed to create user.')


@cli_utils.action_logging
def delete_user(args):
    if not args.username:
        raise SystemExit('Required arguments are missing: username')

    appbuilder = cached_appbuilder()

    try:
        u = next(u for u in appbuilder.sm.get_all_users() if u.username == args.username)
    except StopIteration:
        raise SystemExit('{} is not a valid user.'.format(args.username))

    if appbuilder.sm.del_register_user(u):
        print('User {} deleted.'.format(args.username))
    else:
        raise SystemExit('Failed to delete user.')


@cli_utils.action_logging
def list_users(args):
    appbuilder = cached_appbuilder()
    users = appbuilder.sm.get_all_users()
    fields = ['id', 'username', 'email', 'first_name', 'last_name', 'roles']
    users = [[user.__getattribute__(field) for field in fields] for user in users]
    msg = tabulate(users, [field.capitalize().replace('_', ' ') for field in fields],
                   tablefmt="fancy_grid")
    if sys.version_info[0] < 3:
        msg = msg.encode('utf-8')
    print(msg)


@cli_utils.action_logging
def list_dag_runs(args, dag=None):
    if dag:
        args.dag_id = dag.dag_id

    dagbag = DagBag()

    if args.dag_id not in dagbag.dags:
        error_message = "Dag id {} not found".format(args.dag_id)
        raise AirflowException(error_message)

    dag_runs = list()
    state = args.state.lower() if args.state else None
    for run in DagRun.find(dag_id=args.dag_id,
                           state=state,
                           no_backfills=args.no_backfill):
        dag_runs.append({
            'id': run.id,
            'run_id': run.run_id,
            'state': run.state,
            'dag_id': run.dag_id,
            'execution_date': run.execution_date.isoformat(),
            'start_date': ((run.start_date or '') and
                           run.start_date.isoformat()),
        })
    if not dag_runs:
        print('No dag runs for {dag_id}'.format(dag_id=args.dag_id))

    s = textwrap.dedent("""\n
    {line}
    DAG RUNS
    {line}
    {dag_run_header}
    """)

    dag_runs.sort(key=lambda x: x['execution_date'], reverse=True)
    dag_run_header = '%-3s | %-20s | %-10s | %-20s | %-20s |' % ('id',
                                                                 'run_id',
                                                                 'state',
                                                                 'execution_date',
                                                                 'state_date')
    print(s.format(dag_run_header=dag_run_header,
                   line='-' * 120))
    for dag_run in dag_runs:
        record = '%-3s | %-20s | %-10s | %-20s | %-20s |' % (dag_run['id'],
                                                             dag_run['run_id'],
                                                             dag_run['state'],
                                                             dag_run['execution_date'],
                                                             dag_run['start_date'])
        print(record)


@cli_utils.action_logging
def sync_perm(args): # noqa
    if settings.RBAC:
        appbuilder = cached_appbuilder()
        print('Updating permission, view-menu for all existing roles')
        appbuilder.sm.sync_roles()
        print('Updating permission on all DAG views')
        dags = DagBag(store_serialized_dags=settings.STORE_SERIALIZED_DAGS).dags.values()
        for dag in dags:
            appbuilder.sm.sync_perm_for_dag(
                dag.dag_id,
                dag.access_control)
    else:
        print('The sync_perm command only works for rbac UI.')


def config(args):
    """Show current application configuration"""
    with io.StringIO() as output:
        conf.write(output)
        code = output.getvalue()
        if cli_utils.should_use_colors(args):
            code = pygments.highlight(
                code=code, formatter=TerminalFormatter(), lexer=IniLexer()
            )
        print(code)


class Anonymizer(Protocol):
    """Anonymizer protocol."""

    def process_path(self, value):
        """Remove pii from paths"""

    def process_username(self, value):
        """Remove pii from ussername"""

    def process_url(self, value):
        """Remove pii from URL"""


class NullAnonymizer(Anonymizer):
    """Do nothing."""

    def _identity(self, value):
        return value

    process_path = process_username = process_url = _identity

    del _identity


class PiiAnonymizer(Anonymizer):
    """Remove personally identifiable info from path."""

    def __init__(self):
        home_path = os.path.expanduser("~")
        username = getpass.getuser()
        self._path_replacements = OrderedDict([
            (home_path, "${HOME}"), (username, "${USER}")
        ])

    def process_path(self, value):
        if not value:
            return value
        for src, target in self._path_replacements.items():
            value = value.replace(src, target)
        return value

    def process_username(self, value):
        if not value:
            return value
        return value[0] + "..." + value[-1]

    def process_url(self, value):
        if not value:
            return value

        url_parts = urlsplit(value)
        netloc = None
        if url_parts.netloc:
            # unpack
            userinfo = None
            host = None
            username = None
            password = None

            if "@" in url_parts.netloc:
                userinfo, _, host = url_parts.netloc.partition("@")
            else:
                host = url_parts.netloc
            if userinfo:
                if ":" in userinfo:
                    username, _, password = userinfo.partition(":")
                else:
                    username = userinfo

            # anonymize
            username = self.process_username(username) if username else None
            password = "PASSWORD" if password else None

            # pack
            if username and password and host:
                netloc = username + ":" + password + "@" + host
            elif username and host:
                netloc = username + "@" + host
            elif password and host:
                netloc = ":" + password + "@" + host
            elif host:
                netloc = host
            else:
                netloc = ""

        return urlunsplit((url_parts.scheme, netloc, url_parts.path, url_parts.query, url_parts.fragment))


class OperatingSystem:
    """Operating system"""

    WINDOWS = "Windows"
    LINUX = "Linux"
    MACOSX = "Mac OS"
    CYGWIN = "Cygwin"

    @staticmethod
    def get_current():
        """Get current operating system"""
        if os.name == "nt":
            return OperatingSystem.WINDOWS
        elif "linux" in sys.platform:
            return OperatingSystem.LINUX
        elif "darwin" in sys.platform:
            return OperatingSystem.MACOSX
        elif "cygwin" in sys.platform:
            return OperatingSystem.CYGWIN
        return None


class Architecture:
    """Compute architecture"""

    X86_64 = "x86_64"
    X86 = "x86"
    PPC = "ppc"
    ARM = "arm"

    @staticmethod
    def get_current():
        """Get architecture"""
        return _MACHINE_TO_ARCHITECTURE.get(platform.machine().lower())


_MACHINE_TO_ARCHITECTURE = {
    "amd64": Architecture.X86_64,
    "x86_64": Architecture.X86_64,
    "i686-64": Architecture.X86_64,
    "i386": Architecture.X86,
    "i686": Architecture.X86,
    "x86": Architecture.X86,
    "ia64": Architecture.X86,  # Itanium is different x64 arch, treat it as the common x86.
    "powerpc": Architecture.PPC,
    "power macintosh": Architecture.PPC,
    "ppc64": Architecture.PPC,
    "armv6": Architecture.ARM,
    "armv6l": Architecture.ARM,
    "arm64": Architecture.ARM,
    "armv7": Architecture.ARM,
    "armv7l": Architecture.ARM,
}


class AirflowInfo:
    """All information related to Airflow, system and other."""

    def __init__(self, anonymizer):
        self.airflow_version = airflow_version
        self.system = SystemInfo(anonymizer)
        self.tools = ToolsInfo(anonymizer)
        self.paths = PathsInfo(anonymizer)
        self.config = ConfigInfo(anonymizer)

    def __str__(self):
        return (
            textwrap.dedent(
                """\
                Apache Airflow [{version}]

                {system}

                {tools}

                {paths}

                {config}
                """
            )
            .strip()
            .format(
                version=self.airflow_version,
                system=self.system,
                tools=self.tools,
                paths=self.paths,
                config=self.config,
            )
        )


class SystemInfo:
    """Basic system and python information"""

    def __init__(self, anonymizer):
        self.operating_system = OperatingSystem.get_current()
        self.arch = Architecture.get_current()
        self.uname = platform.uname()
        self.locale = locale.getdefaultlocale()
        self.python_location = anonymizer.process_path(sys.executable)
        self.python_version = sys.version.replace("\n", " ")

    def __str__(self):
        return (
            textwrap.dedent(
                """\
                Platform: [{os}, {arch}] {uname}
                Locale: {locale}
                Python Version: [{python_version}]
                Python Location: [{python_location}]
                """
            )
            .strip()
            .format(
                os=self.operating_system or "NOT AVAILABLE",
                arch=self.arch or "NOT AVAILABLE",
                uname=self.uname,
                locale=self.locale,
                python_version=self.python_version,
                python_location=self.python_location,
            )
        )


class PathsInfo:
    """Path information"""

    def __init__(self, anonymizer):
        system_path = os.environ.get("PATH", "").split(os.pathsep)

        self.airflow_home = anonymizer.process_path(get_airflow_home())
        self.system_path = [anonymizer.process_path(p) for p in system_path]
        self.python_path = [anonymizer.process_path(p) for p in sys.path]
        self.airflow_on_path = any(
            os.path.exists(os.path.join(path_elem, "airflow")) for path_elem in system_path
        )

    def __str__(self):
        return (
            textwrap.dedent(
                """\
                Airflow Home: [{airflow_home}]
                System PATH: [{system_path}]
                Python PATH: [{python_path}]
                airflow on PATH: [{airflow_on_path}]
                """
            )
            .strip()
            .format(
                airflow_home=self.airflow_home,
                system_path=os.pathsep.join(self.system_path),
                python_path=os.pathsep.join(self.python_path),
                airflow_on_path=self.airflow_on_path,
            )
        )


class ConfigInfo:
    """"Most critical config properties"""

    def __init__(self, anonymizer):
        self.executor = conf.get("core", "executor")
        self.dags_folder = anonymizer.process_path(
            conf.get("core", "dags_folder", fallback="NOT AVAILABLE")
        )
        self.plugins_folder = anonymizer.process_path(
            conf.get("core", "plugins_folder", fallback="NOT AVAILABLE")
        )
        self.base_log_folder = anonymizer.process_path(
            conf.get("core", "base_log_folder", fallback="NOT AVAILABLE")
        )
        self.sql_alchemy_conn = anonymizer.process_url(
            conf.get("core", "SQL_ALCHEMY_CONN", fallback="NOT AVAILABLE")
        )

    def __str__(self):
        return (
            textwrap.dedent(
                """\
                Executor: [{executor}]
                SQL Alchemy Conn: [{sql_alchemy_conn}]
                DAGS Folder: [{dags_folder}]
                Plugins Folder: [{plugins_folder}]
                Base Log Folder: [{base_log_folder}]
                """
            )
            .strip()
            .format(
                executor=self.executor,
                sql_alchemy_conn=self.sql_alchemy_conn,
                dags_folder=self.dags_folder,
                plugins_folder=self.plugins_folder,
                base_log_folder=self.base_log_folder,
            )
        )


class ToolsInfo:
    """The versions of the tools that Airflow uses"""

    def __init__(self, anonymize):
        del anonymize  # Nothing to anonymize here.
        self.git_version = self._get_version(["git", "--version"])
        self.ssh_version = self._get_version(["ssh", "-V"])
        self.kubectl_version = self._get_version(["kubectl", "version", "--short=True", "--client=True"])
        self.gcloud_version = self._get_version(["gcloud", "version"], grep=b"Google Cloud SDK")
        self.cloud_sql_proxy_version = self._get_version(["cloud_sql_proxy", "--version"])
        self.mysql_version = self._get_version(["mysql", "--version"])
        self.sqlite3_version = self._get_version(["sqlite3", "--version"])
        self.psql_version = self._get_version(["psql", "--version"])

    def _get_version(self, cmd, grep=None):
        """Return tools version."""
        try:
            proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        except OSError:
            return "NOT AVAILABLE"
        stdoutdata, _ = proc.communicate()
        data = [f for f in stdoutdata.split(b"\n") if f]
        if grep:
            data = [line for line in data if grep in line]
        if len(data) != 1:
            return "NOT AVAILABLE"
        else:
            return data[0].decode()

    def __str__(self):
        return (
            textwrap.dedent(
                """\
                git: [{git}]
                ssh: [{ssh}]
                kubectl: [{kubectl}]
                gcloud: [{gcloud}]
                cloud_sql_proxy: [{cloud_sql_proxy}]
                mysql: [{mysql}]
                sqlite3: [{sqlite3}]
                psql: [{psql}]
                """
            )
            .strip()
            .format(
                git=self.git_version,
                ssh=self.ssh_version,
                kubectl=self.kubectl_version,
                gcloud=self.gcloud_version,
                cloud_sql_proxy=self.cloud_sql_proxy_version,
                mysql=self.mysql_version,
                sqlite3=self.sqlite3_version,
                psql=self.psql_version,
            )
        )


class FileIoException(Exception):
    """Raises when error happens in FileIo.io integration"""


@tenacity.retry(
    stop=tenacity.stop_after_attempt(5),
    wait=tenacity.wait_exponential(multiplier=1, max=10),
    retry=tenacity.retry_if_exception_type(FileIoException),
    before=tenacity.before_log(log, logging.DEBUG),
    after=tenacity.after_log(log, logging.DEBUG),
)
def _upload_text_to_fileio(content):
    """Uload text file to File.io service and return lnk"""
    resp = requests.post("https://file.io", files={"file": ("airflow-report.txt", content)})
    if not resp.ok:
        raise FileIoException("Failed to send report to file.io service.")
    try:
        return resp.json()["link"]
    except ValueError as e:
        log.debug(e)
        raise FileIoException("Failed to send report to file.io service.")


def _send_report_to_fileio(info):
    print("Uploading report to file.io service.")
    try:
        link = _upload_text_to_fileio(str(info))
        print("Report uploaded.")
        print()
        print("Link:\t", link)
        print()
    except FileIoException as ex:
        print(str(ex))


def info(args):
    """
    Show information related to Airflow, system and other.
    """
    # Enforce anonymization, when file_io upload is tuned on.
    anonymizer = PiiAnonymizer() if args.anonymize or args.file_io else NullAnonymizer()
    info = AirflowInfo(anonymizer)
    if args.file_io:
        _send_report_to_fileio(info)
    else:
        print(info)


def upgrade_check(args):
    if args.save:
        filename = args.save
        if not filename.lower().endswith(".json"):
            print("Only JSON files are supported", file=sys.stderr)
        formatter = JSONFormatter(args.save)
    else:
        formatter = ConsoleFormatter()
    all_problems = check_upgrade(formatter)
    if all_problems:
        sys.exit(1)


class Arg(object):
    def __init__(self, flags=None, help=None, action=None, default=None, nargs=None,
                 type=None, choices=None, metavar=None):
        self.flags = flags
        self.help = help
        self.action = action
        self.default = default
        self.nargs = nargs
        self.type = type
        self.choices = choices
        self.metavar = metavar


class CLIFactory(object):
    args = {
        # Shared
        'dag_id': Arg(("dag_id",), "The id of the dag"),
        'task_id': Arg(("task_id",), "The id of the task"),
        'execution_date': Arg(
            ("execution_date",), help="The execution date of the DAG",
            type=parsedate),
        'output_path': Arg(
            ('-o', '--output-path'),
            help="output path for yaml file",
            default=os.getcwd()
        ),
        'task_regex': Arg(
            ("-t", "--task_regex"),
            "The regex to filter specific task_ids to backfill (optional)"),
        'subdir': Arg(
            ("-sd", "--subdir"),
            "File location or directory from which to look for the dag. "
            "Defaults to '[AIRFLOW_HOME]/dags' where [AIRFLOW_HOME] is the "
            "value you set for 'AIRFLOW_HOME' config you set in 'airflow.cfg' ",
            default=DAGS_FOLDER),
        'start_date': Arg(
            ("-s", "--start_date"), "Override start_date YYYY-MM-DD",
            type=parsedate),
        'end_date': Arg(
            ("-e", "--end_date"), "Override end_date YYYY-MM-DD",
            type=parsedate),
        'dry_run': Arg(
            ("-dr", "--dry_run"),
            "Perform a dry run for each task. Only renders Template Fields "
            "for each task, nothing else",
            "store_true"),
        'pid': Arg(
            ("--pid",), "PID file location",
            nargs='?'),
        'daemon': Arg(
            ("-D", "--daemon"), "Daemonize instead of running "
                                "in the foreground",
            "store_true"),
        'stderr': Arg(
            ("--stderr",), "Redirect stderr to this file"),
        'stdout': Arg(
            ("--stdout",), "Redirect stdout to this file"),
        'log_file': Arg(
            ("-l", "--log-file"), "Location of the log file"),
        'yes': Arg(
            ("-y", "--yes"),
            "Do not prompt to confirm reset. Use with care!",
            "store_true",
            default=False),
        'username': Arg(
            ('-u', '--username',),
            help='Username of the user',
            type=str),

        # list_dag_runs
        'no_backfill': Arg(
            ("--no_backfill",),
            "filter all the backfill dagruns given the dag id", "store_true"),
        'state': Arg(
            ("--state",),
            "Only list the dag runs corresponding to the state"
        ),

        # backfill
        'mark_success': Arg(
            ("-m", "--mark_success"),
            "Mark jobs as succeeded without running them", "store_true"),
        'verbose': Arg(
            ("-v", "--verbose"),
            "Make logging output more verbose", "store_true"),
        'local': Arg(
            ("-l", "--local"),
            "Run the task using the LocalExecutor", "store_true"),
        'donot_pickle': Arg(
            ("-x", "--donot_pickle"), (
                "Do not attempt to pickle the DAG object to send over "
                "to the workers, just tell the workers to run their version "
                "of the code."),
            "store_true"),
        'bf_ignore_dependencies': Arg(
            ("-i", "--ignore_dependencies"),
            (
                "Skip upstream tasks, run only the tasks "
                "matching the regexp. Only works in conjunction "
                "with task_regex"),
            "store_true"),
        'bf_ignore_first_depends_on_past': Arg(
            ("-I", "--ignore_first_depends_on_past"),
            (
                "Ignores depends_on_past dependencies for the first "
                "set of tasks only (subsequent executions in the backfill "
                "DO respect depends_on_past)."),
            "store_true"),
        'pool': Arg(("--pool",), "Resource pool to use"),
        'delay_on_limit': Arg(
            ("--delay_on_limit",),
            help=("Amount of time in seconds to wait when the limit "
                  "on maximum active dag runs (max_active_runs) has "
                  "been reached before trying to execute a dag run "
                  "again."),
            type=float,
            default=1.0),
        'reset_dag_run': Arg(
            ("--reset_dagruns",),
            (
                "if set, the backfill will delete existing "
                "backfill-related DAG runs and start "
                "anew with fresh, running DAG runs"),
            "store_true"),
        'rerun_failed_tasks': Arg(
            ("--rerun_failed_tasks",),
            (
                "if set, the backfill will auto-rerun "
                "all the failed tasks for the backfill date range "
                "instead of throwing exceptions"),
            "store_true"),
        'run_backwards': Arg(
            ("-B", "--run_backwards",),
            (
                "if set, the backfill will run tasks from the most "
                "recent day first.  if there are tasks that depend_on_past "
                "this option will throw an exception"),
            "store_true"),

        # list_tasks
        'tree': Arg(("-t", "--tree"), "Tree view", "store_true"),
        # list_dags
        'report': Arg(
            ("-r", "--report"), "Show DagBag loading report", "store_true"),
        # clear
        'upstream': Arg(
            ("-u", "--upstream"), "Include upstream tasks", "store_true"),
        'only_failed': Arg(
            ("-f", "--only_failed"), "Only failed jobs", "store_true"),
        'only_running': Arg(
            ("-r", "--only_running"), "Only running jobs", "store_true"),
        'downstream': Arg(
            ("-d", "--downstream"), "Include downstream tasks", "store_true"),
        'no_confirm': Arg(
            ("-c", "--no_confirm"),
            "Do not request confirmation", "store_true"),
        'exclude_subdags': Arg(
            ("-x", "--exclude_subdags"),
            "Exclude subdags", "store_true"),
        'exclude_parentdag': Arg(
            ("-xp", "--exclude_parentdag"),
            "Exclude ParentDAGS if the task cleared is a part of a SubDAG",
            "store_true"),
        'dag_regex': Arg(
            ("-dx", "--dag_regex"),
            "Search dag_id as regex instead of exact string", "store_true"),
        # show_dag
        'save': Arg(
            ("-s", "--save"),
            "Saves the result to the indicated file. The file format is determined by the file extension.\n"
            "\n"
            "To see more information about supported format for show_dags command, see: "
            "https://www.graphviz.org/doc/info/output.html\n"
            "\n"
            "If you want to create a PNG file then you should execute the following command:\n"
            "airflow show_dag <DAG_ID> --save output.png\n"
            "\n"
            "If you want to create a DOT file then you should execute the following command:\n"
            "airflow show_dag <DAG_ID> --save output.dot\n"
        ),
        'imgcat': Arg(
            ("--imgcat", ),
            "Displays graph using the imgcat tool. \n"
            "\n"
            "For more information, see: https://www.iterm2.com/documentation-images.html",
            action='store_true'),
        # trigger_dag
        'run_id': Arg(("-r", "--run_id"), "Helps to identify this run"),
        'conf': Arg(
            ('-c', '--conf'),
            "JSON string that gets pickled into the DagRun's conf attribute"),
        'exec_date': Arg(
            ("-e", "--exec_date"), help="The execution date of the DAG",
            type=parsedate),
        # pool
        'pool_set': Arg(
            ("-s", "--set"),
            nargs=3,
            metavar=('NAME', 'SLOT_COUNT', 'POOL_DESCRIPTION'),
            help="Set pool slot count and description, respectively"),
        'pool_get': Arg(
            ("-g", "--get"),
            metavar='NAME',
            help="Get pool info"),
        'pool_delete': Arg(
            ("-x", "--delete"),
            metavar="NAME",
            help="Delete a pool"),
        'pool_import': Arg(
            ("-i", "--import"),
            metavar="FILEPATH",
            help="Import pool from JSON file"),
        'pool_export': Arg(
            ("-e", "--export"),
            metavar="FILEPATH",
            help="Export pool to JSON file"),
        # variables
        'set': Arg(
            ("-s", "--set"),
            nargs=2,
            metavar=('KEY', 'VAL'),
            help="Set a variable"),
        'get': Arg(
            ("-g", "--get"),
            metavar='KEY',
            help="Get value of a variable"),
        'default': Arg(
            ("-d", "--default"),
            metavar="VAL",
            default=None,
            help="Default value returned if variable does not exist"),
        'json': Arg(
            ("-j", "--json"),
            help="Deserialize JSON variable",
            action="store_true"),
        'var_import': Arg(
            ("-i", "--import"),
            metavar="FILEPATH",
            help="Import variables from JSON file"),
        'var_export': Arg(
            ("-e", "--export"),
            metavar="FILEPATH",
            help="Export variables to JSON file"),
        'var_delete': Arg(
            ("-x", "--delete"),
            metavar="KEY",
            help="Delete a variable"),
        # kerberos
        'principal': Arg(
            ("principal",), "kerberos principal", nargs='?'),
        'keytab': Arg(
            ("-kt", "--keytab"), "keytab",
            nargs='?', default=conf.get('kerberos', 'keytab')),
        # run
        # TODO(aoen): "force" is a poor choice of name here since it implies it overrides
        # all dependencies (not just past success), e.g. the ignore_depends_on_past
        # dependency. This flag should be deprecated and renamed to 'ignore_ti_state' and
        # the "ignore_all_dependencies" command should be called the"force" command
        # instead.
        'interactive': Arg(
            ('-int', '--interactive'),
            help='Do not capture standard output and error streams '
                 '(useful for interactive debugging)',
            action='store_true'),
        'force': Arg(
            ("-f", "--force"),
            "Ignore previous task instance state, rerun regardless if task already "
            "succeeded/failed",
            "store_true"),
        'raw': Arg(("-r", "--raw"), argparse.SUPPRESS, "store_true"),
        'ignore_all_dependencies': Arg(
            ("-A", "--ignore_all_dependencies"),
            "Ignores all non-critical dependencies, including ignore_ti_state and "
            "ignore_task_deps",
            "store_true"),
        # TODO(aoen): ignore_dependencies is a poor choice of name here because it is too
        # vague (e.g. a task being in the appropriate state to be run is also a dependency
        # but is not ignored by this flag), the name 'ignore_task_dependencies' is
        # slightly better (as it ignores all dependencies that are specific to the task),
        # so deprecate the old command name and use this instead.
        'ignore_dependencies': Arg(
            ("-i", "--ignore_dependencies"),
            "Ignore task-specific dependencies, e.g. upstream, depends_on_past, and "
            "retry delay dependencies",
            "store_true"),
        'ignore_depends_on_past': Arg(
            ("-I", "--ignore_depends_on_past"),
            "Ignore depends_on_past dependencies (but respect "
            "upstream dependencies)",
            "store_true"),
        'ship_dag': Arg(
            ("--ship_dag",),
            "Pickles (serializes) the DAG and ships it to the worker",
            "store_true"),
        'pickle': Arg(
            ("-p", "--pickle"),
            "Serialized pickle object of the entire dag (used internally)"),
        'job_id': Arg(("-j", "--job_id"), argparse.SUPPRESS),
        'cfg_path': Arg(
            ("--cfg_path",), "Path to config file to use instead of airflow.cfg"),
        # webserver
        'port': Arg(
            ("-p", "--port"),
            default=conf.get('webserver', 'WEB_SERVER_PORT'),
            type=int,
            help="The port on which to run the server"),
        'ssl_cert': Arg(
            ("--ssl_cert",),
            default=conf.get('webserver', 'WEB_SERVER_SSL_CERT'),
            help="Path to the SSL certificate for the webserver"),
        'ssl_key': Arg(
            ("--ssl_key",),
            default=conf.get('webserver', 'WEB_SERVER_SSL_KEY'),
            help="Path to the key to use with the SSL certificate"),
        'workers': Arg(
            ("-w", "--workers"),
            default=conf.get('webserver', 'WORKERS'),
            type=int,
            help="Number of workers to run the webserver on"),
        'workerclass': Arg(
            ("-k", "--workerclass"),
            default=conf.get('webserver', 'WORKER_CLASS'),
            choices=['sync', 'eventlet', 'gevent', 'tornado'],
            help="The worker class to use for Gunicorn"),
        'worker_timeout': Arg(
            ("-t", "--worker_timeout"),
            default=conf.get('webserver', 'WEB_SERVER_WORKER_TIMEOUT'),
            type=int,
            help="The timeout for waiting on webserver workers"),
        'hostname': Arg(
            ("-hn", "--hostname"),
            default=conf.get('webserver', 'WEB_SERVER_HOST'),
            help="Set the hostname on which to run the web server"),
        'debug': Arg(
            ("-d", "--debug"),
            "Use the server that ships with Flask in debug mode",
            "store_true"),
        'access_logfile': Arg(
            ("-A", "--access_logfile"),
            default=conf.get('webserver', 'ACCESS_LOGFILE'),
            help="The logfile to store the webserver access log. Use '-' to print to "
                 "stderr."),
        'error_logfile': Arg(
            ("-E", "--error_logfile"),
            default=conf.get('webserver', 'ERROR_LOGFILE'),
            help="The logfile to store the webserver error log. Use '-' to print to "
                 "stderr."),
        # scheduler
        'dag_id_opt': Arg(("-d", "--dag_id"), help="The id of the dag to run"),
        'run_duration': Arg(
            ("-r", "--run-duration"),
            default=None, type=int,
            help="Set number of seconds to execute before exiting"),
        'num_runs': Arg(
            ("-n", "--num_runs"),
            default=conf.getint('scheduler', 'num_runs', fallback=-1), type=int,
            help="Set the number of runs to execute before exiting"),
        # worker
        'do_pickle': Arg(
            ("-p", "--do_pickle"),
            default=False,
            help=(
                "Attempt to pickle the DAG object to send over "
                "to the workers, instead of letting workers run their version "
                "of the code."),
            action="store_true"),
        'queues': Arg(
            ("-q", "--queues"),
            help="Comma delimited list of queues to serve",
            default=conf.get('celery', 'DEFAULT_QUEUE')),
        'concurrency': Arg(
            ("-c", "--concurrency"),
            type=int,
            help="The number of worker processes",
            default=conf.get('celery', 'worker_concurrency')),
        'celery_hostname': Arg(
            ("-cn", "--celery_hostname"),
            help=("Set the hostname of celery worker "
                  "if you have multiple workers on a single machine.")),
        # flower
        'broker_api': Arg(("-a", "--broker_api"), help="Broker api"),
        'flower_hostname': Arg(
            ("-hn", "--hostname"),
            default=conf.get('celery', 'FLOWER_HOST'),
            help="Set the hostname on which to run the server"),
        'flower_port': Arg(
            ("-p", "--port"),
            default=conf.get('celery', 'FLOWER_PORT'),
            type=int,
            help="The port on which to run the server"),
        'flower_conf': Arg(
            ("-fc", "--flower_conf"),
            help="Configuration file for flower"),
        'flower_url_prefix': Arg(
            ("-u", "--url_prefix"),
            default=conf.get('celery', 'FLOWER_URL_PREFIX'),
            help="URL prefix for Flower"),
        'flower_basic_auth': Arg(
            ("-ba", "--basic_auth"),
            default=conf.get('celery', 'FLOWER_BASIC_AUTH'),
            help=("Securing Flower with Basic Authentication. "
                  "Accepts user:password pairs separated by a comma. "
                  "Example: flower_basic_auth = user1:password1,user2:password2")),
        'task_params': Arg(
            ("-tp", "--task_params"),
            help="Sends a JSON params dict to the task"),
        'post_mortem': Arg(
            ("-pm", "--post_mortem"),
            action="store_true",
            help="Open debugger on uncaught exception",
        ),
        # connections
        'list_connections': Arg(
            ('-l', '--list'),
            help='List all connections',
            action='store_true'),
        'add_connection': Arg(
            ('-a', '--add'),
            help='Add a connection',
            action='store_true'),
        'delete_connection': Arg(
            ('-d', '--delete'),
            help='Delete a connection',
            action='store_true'),
        'conn_id': Arg(
            ('--conn_id',),
            help='Connection id, required to add/delete a connection',
            type=str),
        'conn_uri': Arg(
            ('--conn_uri',),
            help='Connection URI, required to add a connection without conn_type',
            type=str),
        'conn_type': Arg(
            ('--conn_type',),
            help='Connection type, required to add a connection without conn_uri',
            type=str),
        'conn_host': Arg(
            ('--conn_host',),
            help='Connection host, optional when adding a connection',
            type=str),
        'conn_login': Arg(
            ('--conn_login',),
            help='Connection login, optional when adding a connection',
            type=str),
        'conn_password': Arg(
            ('--conn_password',),
            help='Connection password, optional when adding a connection',
            type=str),
        'conn_schema': Arg(
            ('--conn_schema',),
            help='Connection schema, optional when adding a connection',
            type=str),
        'conn_port': Arg(
            ('--conn_port',),
            help='Connection port, optional when adding a connection',
            type=str),
        'conn_extra': Arg(
            ('--conn_extra',),
            help='Connection `Extra` field, optional when adding a connection',
            type=str),
        # create_user
        'role': Arg(
            ('-r', '--role',),
            help='Role of the user. Existing roles include Admin, '
                 'User, Op, Viewer, and Public',
            type=str),
        'firstname': Arg(
            ('-f', '--firstname',),
            help='First name of the user',
            type=str),
        'lastname': Arg(
            ('-l', '--lastname',),
            help='Last name of the user',
            type=str),
        'email': Arg(
            ('-e', '--email',),
            help='Email of the user',
            type=str),
        'password': Arg(
            ('-p', '--password',),
            help='Password of the user',
            type=str),
        'use_random_password': Arg(
            ('--use_random_password',),
            help='Do not prompt for password.  Use random string instead',
            default=False,
            action='store_true'),
        'autoscale': Arg(
            ('-a', '--autoscale'),
            help="Minimum and Maximum number of worker to autoscale"),
        'skip_serve_logs': Arg(
            ("-s", "--skip_serve_logs"),
            default=False,
            help="Don't start the serve logs process along with the workers.",
            action="store_true"),
        'color': Arg(
            ('--color',),
            help="Do emit colored output (default: auto)",
            choices={cli_utils.ColorMode.ON, cli_utils.ColorMode.OFF, cli_utils.ColorMode.AUTO},
            default=cli_utils.ColorMode.AUTO),
        # info
        'anonymize': Arg(
            ('--anonymize',),
            help=(
                'Minimize any personal identifiable information. '
                'Use it when sharing output with others.'
            ),
            action='store_true'
        ),
        'file_io': Arg(
            ('--file-io',),
            help=(
                'Send output to file.io service and returns link.'
            ),
            action='store_true'
        )
    }
    subparsers = (
        {
            'func': backfill,
            'help': "Run subsections of a DAG for a specified date range. "
                    "If reset_dag_run option is used,"
                    " backfill will first prompt users whether airflow "
                    "should clear all the previous dag_run and task_instances "
                    "within the backfill date range. "
                    "If rerun_failed_tasks is used, backfill "
                    "will auto re-run the previous failed task instances"
                    " within the backfill date range.",
            'args': (
                'dag_id', 'task_regex', 'start_date', 'end_date',
                'mark_success', 'local', 'donot_pickle', 'yes',
                'bf_ignore_dependencies', 'bf_ignore_first_depends_on_past',
                'subdir', 'pool', 'delay_on_limit', 'dry_run', 'verbose', 'conf',
                'reset_dag_run', 'rerun_failed_tasks', 'run_backwards'
            )
        }, {
            'func': list_dag_runs,
            'help': "List dag runs given a DAG id. If state option is given, it will only"
                    "search for all the dagruns with the given state. "
                    "If no_backfill option is given, it will filter out"
                    "all backfill dagruns for given dag id.",
            'args': (
                'dag_id', 'no_backfill', 'state'
            )
        }, {
            'func': list_tasks,
            'help': "List the tasks within a DAG",
            'args': ('dag_id', 'tree', 'subdir'),
        }, {
            'func': kubernetes_generate_dag_yaml,
            'help': "List dag runs given a DAG id. If state option is given, it will only"
                    "search for all the dagruns with the given state. "
                    "If no_backfill option is given, it will filter out"
                    "all backfill dagruns for given dag id.",
            'args': (
                'dag_id', 'output_path', 'subdir', 'execution_date'
            )

        }, {
            'func': generate_pod_template,
            'help': "Reads your airflow.cfg and migrates your configurations into a"
                    "airflow_template.yaml file. From this point a user can link"
                    "this file to airflow using the `pod_template_file` argument"
                    "and modify using the Kubernetes API",
            'args': ('output_path',),
        }, {
            'func': clear,
            'help': "Clear a set of task instance, as if they never ran",
            'args': (
                'dag_id', 'task_regex', 'start_date', 'end_date', 'subdir',
                'upstream', 'downstream', 'no_confirm', 'only_failed',
                'only_running', 'exclude_subdags', 'exclude_parentdag', 'dag_regex'),
        }, {
            'func': pause,
            'help': "Pause a DAG",
            'args': ('dag_id', 'subdir'),
        }, {
            'func': unpause,
            'help': "Resume a paused DAG",
            'args': ('dag_id', 'subdir'),
        }, {
            'func': trigger_dag,
            'help': "Trigger a DAG run",
            'args': ('dag_id', 'subdir', 'run_id', 'conf', 'exec_date'),
        }, {
            'func': delete_dag,
            'help': "Delete all DB records related to the specified DAG",
            'args': ('dag_id', 'yes',),
        }, {
            'func': show_dag,
            'help': "Displays DAG's tasks with their dependencies",
            'args': ('dag_id', 'subdir', 'save', 'imgcat',),
        }, {
            'func': pool,
            'help': "CRUD operations on pools",
            "args": ('pool_set', 'pool_get', 'pool_delete', 'pool_import', 'pool_export'),
        }, {
            'func': variables,
            'help': "CRUD operations on variables",
            "args": ('set', 'get', 'json', 'default',
                     'var_import', 'var_export', 'var_delete'),
        }, {
            'func': kerberos,
            'help': "Start a kerberos ticket renewer",
            'args': ('principal', 'keytab', 'pid',
                     'daemon', 'stdout', 'stderr', 'log_file'),
        }, {
            'func': render,
            'help': "Render a task instance's template(s)",
            'args': ('dag_id', 'task_id', 'execution_date', 'subdir'),
        }, {
            'func': run,
            'help': "Run a single task instance",
            'args': (
                'dag_id', 'task_id', 'execution_date', 'subdir',
                'mark_success', 'force', 'pool', 'cfg_path',
                'local', 'raw', 'ignore_all_dependencies', 'ignore_dependencies',
                'ignore_depends_on_past', 'ship_dag', 'pickle', 'job_id', 'interactive',),
        }, {
            'func': initdb,
            'help': "Initialize the metadata database",
            'args': tuple(),
        }, {
            'func': list_dags,
            'help': "List all the DAGs",
            'args': ('subdir', 'report'),
        }, {
            'func': dag_state,
            'help': "Get the status of a dag run",
            'args': ('dag_id', 'execution_date', 'subdir'),
        }, {
            'func': task_failed_deps,
            'help': (
                "Returns the unmet dependencies for a task instance from the perspective "
                "of the scheduler. In other words, why a task instance doesn't get "
                "scheduled and then queued by the scheduler, and then run by an "
                "executor)."),
            'args': ('dag_id', 'task_id', 'execution_date', 'subdir'),
        }, {
            'func': task_state,
            'help': "Get the status of a task instance",
            'args': ('dag_id', 'task_id', 'execution_date', 'subdir'),
        }, {
            'func': serve_logs,
            'help': "Serve logs generate by worker",
            'args': tuple(),
        }, {
            'func': test,
            'help': (
                "Test a task instance. This will run a task without checking for "
                "dependencies or recording its state in the database."),
            'args': (
                'dag_id', 'task_id', 'execution_date', 'subdir', 'dry_run',
                'task_params', 'post_mortem'),
        }, {
            'func': webserver,
            'help': "Start a Airflow webserver instance",
            'args': ('port', 'workers', 'workerclass', 'worker_timeout', 'hostname',
                     'pid', 'daemon', 'stdout', 'stderr', 'access_logfile',
                     'error_logfile', 'log_file', 'ssl_cert', 'ssl_key', 'debug'),
        }, {
            'func': resetdb,
            'help': "Burn down and rebuild the metadata database",
            'args': ('yes',),
        }, {
            'func': upgradedb,
            'help': "Upgrade the metadata database to latest version",
            'args': tuple(),
        }, {
            'func': checkdb,
            'help': "Check if the database can be reached.",
            'args': tuple(),
        }, {
            'func': shell,
            'help': "Runs a shell to access the database",
            'args': tuple(),
        }, {
            'func': scheduler,
            'help': "Start a scheduler instance",
            'args': ('dag_id_opt', 'subdir', 'run_duration', 'num_runs',
                     'do_pickle', 'pid', 'daemon', 'stdout', 'stderr',
                     'log_file'),
        }, {
            'func': worker,
            'help': "Start a Celery worker node",
            'args': ('do_pickle', 'queues', 'concurrency', 'celery_hostname',
                     'pid', 'daemon', 'stdout', 'stderr', 'log_file', 'autoscale', 'skip_serve_logs'),
        }, {
            'func': flower,
            'help': "Start a Celery Flower",
            'args': ('flower_hostname', 'flower_port', 'flower_conf', 'flower_url_prefix',
                     'flower_basic_auth', 'broker_api', 'pid', 'daemon', 'stdout', 'stderr', 'log_file'),
        }, {
            'func': version,
            'help': "Show the version",
            'args': tuple(),
        }, {
            'func': connections,
            'help': "List/Add/Delete connections",
            'args': ('list_connections', 'add_connection', 'delete_connection',
                     'conn_id', 'conn_uri', 'conn_extra') + tuple(alternative_conn_specs),
        }, {
            'func': create_user,
            'help': "Create an account for the Web UI (FAB-based)",
            'args': ('role', 'username', 'email', 'firstname', 'lastname',
                     'password', 'use_random_password'),
        }, {
            'func': delete_user,
            'help': "Delete an account for the Web UI",
            'args': ('username',),
        }, {
            'func': list_users,
            'help': "List accounts for the Web UI",
            'args': tuple(),
        },
        {
            'func': sync_perm,
            'help': "Update permissions for existing roles and DAGs.",
            'args': tuple(),
        },
        {
            'func': next_execution,
            'help': "Get the next execution datetime of a DAG.",
            'args': ('dag_id', 'subdir')
        },
        {
            'func': rotate_fernet_key,
            'help': 'Rotate all encrypted connection credentials and variables; see '
                    'https://airflow.readthedocs.io/en/stable/howto/secure-connections.html'
                    '#rotating-encryption-keys.',
            'args': (),
        },
        {
            'help': 'Show current application configuration',
            'func': config,
            'args': ('color', ),
        },
        {
            'help': 'Show information about current Airflow and environment',
            'func': info,
            'args': ('anonymize', 'file_io', ),
        },
        {
            'name': 'upgrade_check',
            'help': 'Check if you can upgrade to the new version.',
            'func': upgrade_check,
            'args': ('save', ),
        },
    )
    subparsers_dict = {sp['func'].__name__: sp for sp in subparsers}
    dag_subparsers = (
        'list_tasks', 'backfill', 'test', 'run', 'pause', 'unpause', 'list_dag_runs')

    @classmethod
    def get_parser(cls, dag_parser=False):
        """Creates and returns command line argument parser"""
        class DefaultHelpParser(argparse.ArgumentParser):
            """Override argparse.ArgumentParser.error and use print_help instead of print_usage"""
            def error(self, message):
                self.print_help()
                self.exit(2, '\n{} command error: {}, see help above.\n'.format(self.prog, message))
        parser = DefaultHelpParser()
        subparsers = parser.add_subparsers(
            help='sub-command help', dest='subcommand')
        subparsers.required = True

        subparser_list = cls.dag_subparsers if dag_parser else cls.subparsers_dict.keys()
        for sub in subparser_list:
            sub = cls.subparsers_dict[sub]
            sp = subparsers.add_parser(sub['func'].__name__, help=sub['help'])
            for arg in sub['args']:
                if 'dag_id' in arg and dag_parser:
                    continue
                arg = cls.args[arg]
                kwargs = {
                    f: v
                    for f, v in vars(arg).items() if f != 'flags' and v}
                sp.add_argument(*arg.flags, **kwargs)
            sp.set_defaults(func=sub['func'])
        return parser


def get_parser():
    return CLIFactory.get_parser()


def py2_deprecation_waring():

    if sys.version_info[0] != 2:
        return

    stream = sys.stderr
    try:
        from pip._vendor import colorama
        WINDOWS = (sys.platform.startswith("win") or
                   (sys.platform == 'cli' and os.name == 'nt'))
        if WINDOWS:
            stream = colorama.AnsiToWin32(sys.stderr)
    except Exception:
        colorama = None

    def should_color():
        # Don't colorize things if we do not have colorama or if told not to
        if not colorama:
            return False

        real_stream = (
            stream if not isinstance(stream, colorama.AnsiToWin32)
            else stream.wrapped
        )

        # If the stream is a tty we should color it
        if hasattr(real_stream, "isatty") and real_stream.isatty():
            return True

        if os.environ.get("TERM") and "color" in os.environ.get("TERM"):
            return True

        # If anything else we should not color it
        return False

    msg = (
        "DEPRECATION: Python 2.7 will reach the end of its life on January 1st, 2020. Airflow 1.10 "
        "will be the last release series to support Python 2\n"
    )
    if should_color():
        msg = "".join([colorama.Fore.YELLOW, msg, colorama.Style.RESET_ALL])
    stream.write(msg)
    stream.flush()
