# GRAL - A GRaph AnaLytics engine

This is a prototype. It strives to implement a server process `gral`
(single process, RAM only) for a graph analytics engine and implements
the API described in `gral/design/GraphAnaphAnalyticsEngine.md` in this
repository. Furthermore, there is a client program called `grupload`
which can do the following:

  - upload graphs from files into `gral`, 
  - create random graphs and write them into files,
  - trigger computations on graph data in `gral`
  - dump out results from `gral` to files.

## Example session:

Create a graph with 100 vertices and 100 random edges:

```
grupload randomize --vertices V --edges E --max-vertices=100 --max-edges=100 --key-size 20 --vertex-coll_name V
```

Upload it to the server process `gral` (which must already be running):

```
target/release/grupload upload --vertices V --edges E --threads 2
```

TO BE CONTINUED
