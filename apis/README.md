# Sudoku raw-stream API

`apis` exposes only the Sudoku appearance codec. It wraps an existing `net.Conn`
so callers read and write original bytes while the wire stream is encoded as
Sudoku traffic.

It does not provide HTTPMask, encryption, handshakes, UoT, or reverse proxy APIs.

```go
cfg := apis.DefaultConfig()
cfg.Key = "same-client-server-uuid"
cfg.ASCII = "prefer_entropy"
cfg.EnablePureDownlink = false // default: packed server-to-client traffic

rawClient, _ := net.Dial("tcp", serverAddr)
conn, _ := apis.ClientConn(rawClient, cfg)
```

On the accepting side:

```go
conn, _ := apis.ServerConn(rawAcceptedConn, cfg)
```

For mux, wrap the base connection first and then start the mux session:

```go
mux, _ := apis.NewMuxClient(conn)
stream, _ := mux.Dial("example.com:443")
```

The server side can use `apis.HandleMuxServer` or `apis.HandleMuxWithDialer` on
the matching wrapped connection.
