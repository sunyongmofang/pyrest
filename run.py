from App import PyRest
import sys


if __name__ == '__main__':
    configPath = sys.argv[1] if len(sys.argv) > 1 else './prest.toml'
    PyRest.main(configPath, __name__)
