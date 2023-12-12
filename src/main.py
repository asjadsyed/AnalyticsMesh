#!/usr/bin/env python3

import argparse
import logging
import sys
import random
from typing import Optional, Tuple
import os

from analytics_mesh import AnalyticsMesh, DurabilityLevel

LOG_LEVELS = ["notset", "debug", "info", "warning", "error", "critical"]
LOGGER: logging.Logger = logging.getLogger(__name__)
SERVER_PORT = 6000
SEED_SERVER_ADDRESS = ("::", SERVER_PORT)
SEED_CLIENT_ADDRESSES = [("::1", SERVER_PORT)]


def configure_logger(log_level: Optional[str]) -> None:
    root_logger = logging.getLogger()
    if log_level is not None:
        root_logger.setLevel(getattr(logging, log_level.upper()))
    if not root_logger.hasHandlers():
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(
            logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        )
        root_logger.addHandler(handler)


def main(args: argparse.Namespace) -> None:
    configure_logger(args.log_level)

    with AnalyticsMesh(
        args.enable_server,
        args.enable_client,
        args.server_address,
        args.client_addresses,
        args.sketch_file,
        args.durability_level,
        args.atomicity,
    ) as am:
        # Cardinality estimation of machines which have interacted with the application.
        # am.update_sketch(uuid.getnode())

        # Cardinality estimation of processes which have interacted with the application.
        # am.update_sketch(f"{uuid.getnode()} | {os.getpid()} | {psutil.Process().create_time()}")

        # Cardinality estimation of distributed real-time streaming data from external sources.
        for line in sys.stdin:
            line = line.rstrip("\n")
            am.update_sketch(line)
            LOGGER.debug(
                "The set has ~%r elements",
                am.sketch.get_estimate(),
            )

        # Cardinality estimation of distributed real-time streaming data from internal application sources.
        while True:
            am.update_sketch(random.random())


def parse_address(address_str: str) -> Tuple[str, int]:
    host, _, port_str = address_str.rpartition(":")
    host = host.strip("[]")
    if host == "":
        raise argparse.ArgumentTypeError(
            "Host cannot be empty. Please specify a valid host."
        )
    try:
        port_int = int(port_str)
    except ValueError as e:
        raise argparse.ArgumentTypeError(
            f"Port must be an integer in the range 0-65535, '{port_str}' provided."
        ) from e
    if not 0 <= port_int <= 65535:
        raise argparse.ArgumentTypeError(
            f"Port must be an integer in the range 0-65535, '{port_int}' provided."
        )
    address_tuple = (host, port_int)
    return address_tuple


def validate_sketch_file(sketch_file: str) -> str:
    sketch_file_dirname, sketch_file_basename = os.path.split(sketch_file)
    sketch_file_dirname = sketch_file_dirname or "."
    if sketch_file_basename == "":
        raise argparse.ArgumentTypeError(
            "The sketch file name cannot be empty or a directory."
        )
    if os.path.isdir(sketch_file):
        raise argparse.ArgumentTypeError(
            f"The path '{sketch_file}' is a directory, not a file."
        )
    if os.path.exists(sketch_file) and not os.access(sketch_file, os.W_OK):
        raise argparse.ArgumentTypeError(f"The file '{sketch_file}' is not writable.")
    if not os.access(sketch_file_dirname, os.W_OK):
        raise argparse.ArgumentTypeError(
            f"No write permission in the directory '{sketch_file_dirname}'."
        )
    return sketch_file


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="AnalyticsMesh")
    parser.add_argument(
        "--sketch-file",
        default=None,
        type=validate_sketch_file,
        help="File path for persisting the sketch (optional).",
        required=False,
    )
    parser.add_argument(
        "--server",
        action=argparse.BooleanOptionalAction,
        default=True,
        dest="enable_server",
        help="Toggle server operation (default: %(default)s).",
    )
    parser.add_argument(
        "--client",
        action=argparse.BooleanOptionalAction,
        default=True,
        dest="enable_client",
        help="Toggle client operation (default: %(default)s).",
    )
    parser.add_argument(
        "--server-address",
        default=SEED_SERVER_ADDRESS,
        type=parse_address,
        help="The address for the server to listen on.",
        metavar="HOST:PORT",
        required=False,
    )
    parser.add_argument(
        "--client-addresses",
        default=SEED_CLIENT_ADDRESSES,
        type=parse_address,
        nargs="*",
        help="The addresses for the client to connect to.",
        metavar="HOST:PORT",
        required=False,
    )
    parser.add_argument(
        "--durability",
        type=DurabilityLevel,
        choices=DurabilityLevel,
        default=DurabilityLevel.VOLATILE,
        dest="durability_level",
        help="Durability level when persisting the sketch (default: %(default)s).",
        required=False,
    )
    parser.add_argument(
        "--atomicity",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Toggle atomicity (dependent on durability level).",
    )
    parser.add_argument(
        "--log-level",
        choices=LOG_LEVELS,
        default=None,
        type=str,
        help="Granularity level when logging events",
        required=False,
    )
    args = parser.parse_args()
    args.atomicity = args.atomicity or (
        args.durability_level != DurabilityLevel.VOLATILE
    )
    if args.durability_level == DurabilityLevel.VOLATILE:
        if args.atomicity:
            parser.error(
                f"Atomicity cannot be enabled with {args.durability_level} durability. "
                "Remove the --atomicity flag or adjust the --durability level."
            )
    else:
        if args.sketch_file is None:
            parser.error(
                f"A sketch file is required with {args.durability_level} durability. "
                "Provide a valid file path using --sketch-file "
                f"or set --durability to {DurabilityLevel.VOLATILE}."
            )
    return args


if __name__ == "__main__":
    main(parse_args())
