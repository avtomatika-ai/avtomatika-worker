# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Dmitrii Gagarin aka madgagarin

from argparse import ArgumentParser, Namespace
from importlib import import_module
from os import getcwd
from subprocess import Popen
from sys import executable, path
from sys import exit as sys_exit
from time import sleep
from typing import Any

from avtomatika_worker.worker import Worker


def import_string(import_name: str) -> Any:
    """
    Imports an object by its string path (e.g. 'module.submodule:attribute').
    """
    try:
        module_path, obj_name = import_name.split(":")
    except ValueError:
        raise ImportError(f"Invalid format '{import_name}'. Use 'module:attribute'.") from None

    module = import_module(module_path)
    try:
        return getattr(module, obj_name)
    except AttributeError:
        raise ImportError(f"Module '{module_path}' has no attribute '{obj_name}'.") from None


def run_worker(args: Namespace) -> None:
    # Add current directory to path to allow importing local modules
    path.insert(0, getcwd())

    try:
        worker = import_string(args.app)
    except ImportError as e:
        print(f"Error importing worker: {e}")
        sys_exit(1)

    if not isinstance(worker, Worker):
        print(f"Error: Object '{args.app}' is not an instance of avtomatika_worker.Worker")
        sys_exit(1)

    if args.health_check:
        worker.run_with_health_check()
    else:
        worker.run()


def main() -> None:
    parser = ArgumentParser(description="Avtomatika Worker CLI")
    subparsers = parser.add_subparsers(dest="command", help="Commands")

    run_parser = subparsers.add_parser("run", help="Run a worker instance")
    run_parser.add_argument("--app", required=True, help="Path to the worker instance (e.g. 'my_module:worker')")
    run_parser.add_argument(
        "--health-check", action="store_true", default=True, help="Enable health check server (default: True)"
    )
    run_parser.add_argument(
        "--reload", action="store_true", help="Restart worker on code changes (requires 'watchdog')"
    )

    args = parser.parse_args()

    if args.command == "run":
        if args.reload:
            try:
                from watchdog.events import FileSystemEventHandler
                from watchdog.observers import Observer
            except ImportError:
                print(
                    "Error: '--reload' requires the 'watchdog' package. "
                    "Install it with 'pip install watchdog' or 'pip install avtomatika-worker[dev]'"
                )
                sys_exit(1)

            print(f"Watching for changes in {getcwd()}...")

            class ReloadHandler(FileSystemEventHandler):
                def __init__(self, cmd_args: Namespace) -> None:
                    self.cmd_args = cmd_args
                    self.process: Popen[bytes] | None = None
                    self.start_worker()

                def start_worker(self) -> None:
                    if self.process:
                        self.process.terminate()
                        self.process.wait()

                    # Filter out --reload from args to avoid recursion
                    new_args = [executable, "-m", "avtomatika_worker", "run", "--app", self.cmd_args.app]
                    if self.cmd_args.health_check:
                        new_args.append("--health-check")

                    self.process = Popen(new_args)

                def on_any_event(self, event: Any) -> None:
                    if event.is_directory or not getattr(event, "src_path", "").endswith(".py"):
                        return
                    print(f"Change detected in {event.src_path}. Restarting...")
                    self.start_worker()

            handler = ReloadHandler(args)
            observer = Observer()
            observer.schedule(handler, path=getcwd(), recursive=True)
            observer.start()
            try:
                while True:
                    sleep(1)
            except KeyboardInterrupt:
                observer.stop()
                if handler.process:
                    handler.process.terminate()
            observer.join()
        else:
            run_worker(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
