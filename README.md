# IOUring playground

## Build

```
cmake -G Ninja -B build -S . -DCMAKE_BUILD_TYPE=Release
cmake --build build
```

## Write to file

`./build/wf`

## Client-Server

Server uses IO Uring for syscalls. To connect client to server on localhost:

```
./build/srv 7070

./build/client 7070
```
