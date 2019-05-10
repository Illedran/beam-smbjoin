import argparse
import itertools
import os
import random
import subprocess
import tempfile
from concurrent.futures import ProcessPoolExecutor

key_bits = 24
max_key = 1 << key_bits
payload_bits = 32

def generator(c, i, B):
    for _ in itertools.repeat(None, c):
        key = (i + B * random.getrandbits(key_bits)) % max_key
        yield b'{"id":"%0*x","data1":"%0*x","data2":%f}' % (8, key, payload_bits, random.getrandbits(payload_bits * 4), random.random())


def generate(cpu_id, n, total_records, B, s, schema_file, dir_path):
    command = ["avro-tools", "fromjson", "--schema-file", schema_file, "-"]

    r = range(1, B + 1)
    H = [1. / (i ** s) for i in r]
    H_sum = sum(H)

    data = [int(n * i / H_sum) for i in H]
    total_count = sum(data)

    diff = n - total_count
    for i in range(diff):
        data[i] += 1

    print(data)
    with tempfile.NamedTemporaryFile(delete=False) as f:  # Atomic write
        with subprocess.Popen(command, stdin=subprocess.PIPE,
                              stdout=f) as process:
            for i in sorted(range(B), key=lambda x: random.random()):
                process.stdin.writelines(generator(data[i], i, B))
            os.replace(f.name,
                       os.path.join(dir_path,
                                    "part{}-c{}-b{}-s{:.2f}.avro".format(cpu_id, total_records, B, s)))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('count', metavar='C', type=int,
                        help="Number of records to generate.")
    parser.add_argument('--schema-file', metavar='path', type=str,
                        default=os.path.join(
                                os.path.dirname(os.path.abspath(__file__)),
                                "..",
                                "schema", "Record.avsc"),
                        help="Path to schema file. (default: %(default)s)")
    parser.add_argument('--dir', metavar='dir_path', type=str,
                        help="Save file to a directory. (default: %(default)s)",
                        default='.')

    group = parser.add_argument_group('How to generate skewed data',
                                      description="You can specify the following arguments to create data that is artificially skewed through partitioning skew for a identity function partitioner. "
                                                  "The frequencies of each bucket will follow a Zipf distribution with parameters B (number of buckets) and s (exponent). "
                                                  "The default parameters result in a uniform distribution of frequencies.")

    group.add_argument('-b', '--num-buckets', metavar='B', type=int,
                       help="Number of buckets. Should be power of 2. (default: %(default)s)",
                       default=1)
    group.add_argument('-s', '--shape', metavar='s', type=float,
                       help="Shape parameter of the Zipf distribution. (default: %(default)s)",
                       default=0)

    args = parser.parse_args()

    workers = os.cpu_count() if args.count > 1 << 20 else 1
    split = [int(args.count/workers) for _ in range(workers)]
    for i in range(args.count - sum(split)):
        split[i] += 1
    with ProcessPoolExecutor(max_workers=os.cpu_count()) as pool:
        for i in range(workers):
            pool.submit(generate, i, split[i], args.count, args.num_buckets, args.shape, args.schema_file,
                        args.dir)


if __name__ == '__main__':
    main()
