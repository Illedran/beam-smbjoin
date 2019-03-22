# scio-smbjoin

## Raison d'être:

Scio-SMBjoin. An implementation of SMB join using Scio.
Work done at Spotify for my thesis at KTH.

## How-to:

1) Generate events with `python3 scripts/gen_events.py`. See `gen_events.py --help` for more info.
2) Create bucketed events with (e.g.) `SMBMakeBucketsJob`.
3) Repeat for keys.
4) Join with `SMBJoinJob`.

## Features:

This project comes with number of preconfigured features, including:

### sbt-pack

Use `sbt-pack` instead of `sbt-assembly` to:
 * reduce build time
 * enable efficient dependency caching
 * reduce job submission time

To build package run:

```
sbt pack
```
<!---
### Testing

This template comes with an example of a test, to run tests:

```
sbt test
```
--->

### Scala style

Find style configuration in `scalastyle-config.xml`. To enforce style run:

```
sbt scalastyle
```

### REPL

To experiment with current codebase in [Scio REPL](https://github.com/spotify/scio/wiki/Scio-REPL)
simply:

```
sbt repl/run
```

---

This project is based on the [scio.g8](https://github.com/spotify/scio.g8).
