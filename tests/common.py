import os
from sys import platform

CWD = os.path.dirname(os.path.realpath(__file__))
ROOT_PATH = os.path.abspath(os.path.join(CWD, "../.."))

WORK_DIR = 'work'
RDB_PATH = os.path.join(CWD, 'rdbs')
LOG_DIR = os.path.abspath(os.path.join(CWD, "../logs"))

PORT = 6379
SERVER_PATH = f"{os.path.dirname(os.path.realpath(__file__))}/build/binaries/{os.environ['SERVER_VERSION']}/valkey-server"

def get_platform():
    return platform.lower()

def get_dynamic_lib_extension():
    system = get_platform()

    if system == "windows":
        return ".dll"
    elif system == "darwin":
        return ".dylib"
    elif system == "linux":
        return ".so"
    else:
        raise Exception(f"Unsupported platform: {system}")

PLATFORM = get_platform()
MODULE_PATH = os.path.abspath("{}/target/debug/libvalkey_timeseries{}".format(ROOT_PATH, get_dynamic_lib_extension()))

def get_server_version():
    version = os.getenv('VALKEY_VERSION')
    if version is None:
        version = "unstable"

    return version

def get_server_path(version):
    if version is None:
        version = get_server_version()
    path = os.path.join(CWD, "..", ".build/binaries/{}/valkey-server".format(version))
    return os.path.abspath(path)

def getModulePath():
    path = os.environ.get('MODULE_PATH')
    if path is None:
        path = os.path.join(ROOT_PATH, "target/debug/libvalkey_metrics{}".format(get_dynamic_lib_extension()))
        path = os.path.abspath(path)

    return path