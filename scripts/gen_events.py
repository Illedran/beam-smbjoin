import argparse
import itertools
import os
import random
import subprocess
import tempfile


def generator(c, k, fraction):
    skewed_id_val = random.getrandbits(4 * k)
    for _ in itertools.repeat(None, c):
        if fraction > 0 and random.random() <= fraction:
            id_val = skewed_id_val
        else:
            id_val = random.getrandbits(4 * k)
        yield b'{"id":"%x","value":%f}' % (id_val, random.random())


def generate(c, k, fraction, schema_file, dir_path):
    command = ["avro-tools", "fromjson", "--schema-file", schema_file, "-"]

    with tempfile.NamedTemporaryFile(delete=False) as f:  # Atomic write
        with subprocess.Popen(command, stdin=subprocess.PIPE, stdout=f) as process:
            process.stdin.writelines(generator(c, k, fraction))
        os.replace(f.name,
                   os.path.join(dir_path, f"events-{c}-{int(fraction * 100)}.avro"))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--count', metavar='C', type=int,
                        help="Number of records to generate.", required=True)
    parser.add_argument('-k', '--key-size', metavar='K', type=int,
                        help="Default length of key.", required=True)
    parser.add_argument('-f', '--skewed-fraction', metavar='F', type=float,
                        help="Default fraction of skewed keys. (default: %(default)s)",
                        default=0.0)
    parser.add_argument('--schema-file', metavar='path', type=str,
                        help="Path to schema file. (default: %(default)s)",
                        default="events_schema.json")
    parser.add_argument('--dir', metavar='dir_path', type=str,
                        help="Save file to a directory. (default: %(default)s)",
                        default='.')
    args = parser.parse_args()

    generate(args.count, args.key_size, args.skewed_fraction, args.schema_file,
             args.dir)


if __name__ == '__main__':
    main()
