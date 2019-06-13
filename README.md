# beam-smbjoin

An implementation of SMB join using Beam and Scio.
Work done at Spotify for my thesis at KTH.

## How-to:

1) Generate events with `DataGeneratorKey` and `DataGeneratorEvent`. 
2) Create bucketed events with (e.g.) `SMBMakeBucketsJob`. For skew-adjusted SMB, use
`SMBMakeBucketsSkewAdjJob`.
3) Repeat for keys.
4) Join with `SMBJoinJob`.
5) Compare with `JoinJob`.

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
