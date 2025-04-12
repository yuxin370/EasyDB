# EasyDB

EasyDB is a database system that supports most SQL statements.

# Getting Started

## Install Nix and Enable Flakes

The build environment is managed using Nix Flakes. Before using EasyDB, please ensure that Nix is properly installed and flakes are enabled. Run the following command to install Nix:

```shell
sh <(curl -L https://nixos.org/nix/install) --daemon
```

Then, modify `/etc/nix/nix.conf` and add the following lines to enable Flake support:

```
substituters = https://mirrors.tuna.tsinghua.edu.cn/nix-channels/store https://cache.nixos.org/
experimental-features = nix-command flakes
```

## Build

To compile the project, simply run the command below. Nix will automatically download all dependencies and build the project. The first build may take some time. The build output will be placed in the `result` directory in the project root.

```shell
nix build
```

The `result` directory structure is as follows:

```
result
├── bin
│   ├── easydb_client  # CLI client for EasyDB
│   └── easydb_server  # Server component of EasyDB
└── test               # Compiled binaries for testing
```

## Development Environment

Run the following command to open a new shell with all necessary dependencies set up for development. In this shell, you can use CMake to compile the project as usual. Tools like VSCode can recognize the environment if launched from this terminal (e.g., by running `code`).

```shell
nix develop
```

## Running

### CLI Interaction

First, build the EasyDB server, then start it with the following command:

```shell
./result/bin/easydb_server -p 8888 -d test.db
```

Open another terminal and run the client:

```shell
./result/bin/easydb_client -p 8888
```

### Web GUI Interaction

To enable Web GUI, start the EasyDB server with the web mode enabled:

```shell
./result/bin/easydb_server -p 8888 -d test.db -w
```

Activate the development environment:

```shell
nix develop .
```

Then start the web interface and the Python-based proxy server.

In one terminal:

```shell
cd web_client
python ./proxy/proxy_server.py 
```

In another terminal:

```shell
cd web_client
npm install
npm run dev
```

Finally, open your browser and visit [http://localhost:2000/](http://localhost:2000/) to access the web interface.
