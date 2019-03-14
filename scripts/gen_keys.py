import argparse
import os
import random
import subprocess
import tempfile
import uuid


def generator(c, k):
    for id_val in random.sample(range(2 ** (4 * k)), c):
        yield b'{"id":"%x","key":"%x"}' % (id_val, uuid.uuid4().int)


def generate(c, k, schema_file, dir_path):
    command = ["avro-tools", "fromjson", "--schema-file", schema_file, "-"]

    with tempfile.NamedTemporaryFile(delete=False) as f:  # Atomic write
        with subprocess.Popen(command, stdin=subprocess.PIPE, stdout=f) as process:
            process.stdin.writelines(generator(c, k))
        os.replace(f.name, os.path.join(dir_path, f"keys-{c}.avro"))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--count', metavar='C', type=int,
                        help="Number of records to generate.", required=True)
    parser.add_argument('-k', '--key-size', metavar='K', type=int,
                        help="Default length of key.", required=True)
    parser.add_argument('--schema-file', metavar='path', type=str,
                        help="Path to schema file. (default: %(default)s)",
                        default="keys_schema.json")
    parser.add_argument('--dir', metavar='dir_path', type=str,
                        help="Save file to a directory. (default: %(default)s)",
                        default='.')
    args = parser.parse_args()

    generate(args.count, args.key_size, args.schema_file, args.dir)


if __name__ == '__main__':
    main()
