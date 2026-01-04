# sshuf - Streaming Shuf

`sshuf` is a Python script that provides a streaming, memory-efficient version of the standard `shuf` command. It reads lines from standard input, shuffles them, and writes them to standard output.

This implementation uses a reservoir-like algorithm with an exponentially growing prediction of the total number of lines. This allows it to process streams of unknown or infinite length without storing the entire input in memory.

## Usage

```bash
./sshuf.py [OPTIONS]
```

The script reads from `stdin` and writes to `stdout`.

### Options

-   `-h`, `--help`: Show the help message and exit.
-   `-z`, `--zero-terminated`: Use a null byte (`\0`) as the line delimiter for both input and output. This is useful for safely handling filenames or other data that might contain newlines.
-   `--window-min N`: Sets the initial size of the shuffling window in lines. The window grows exponentially from this value. Defaults to 1024.

## Examples

### Basic Shuffle

Shuffle the numbers 1 through 10.

```bash
seq 10 | ./sshuf.py
```

### Shuffle with Null Delimiter

Shuffle a list of filenames that may contain special characters.

```bash
find . -print0 | ./sshuf.py -z | xargs -0 -n1 echo
```
