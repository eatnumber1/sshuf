#!/usr/bin/env python3

import sys
import random
import argparse

def read_lines_with_delimiter(stream, delimiter):
    """
    A generator that reads from a stream and yields lines based on a delimiter.
    """
    buffer = b""
    while True:
        chunk = stream.read(4096)
        if not chunk:
            if buffer:
                yield buffer
            break
        buffer += chunk
        parts = buffer.split(delimiter)
        buffer = parts.pop()
        for part in parts:
            yield part + delimiter

def streaming_shuffle(input_stream, output_stream, zero_terminated=False, window_min=1024):
    """
    Shuffles an input stream into an output stream using a reservoir-like
    algorithm with an exponentially growing prediction of the total number of lines.
    """
    lines = []
    predicted_n = window_min
    n = 0

    delimiter = b'\0' if zero_terminated else b'\n'
    line_iterator = read_lines_with_delimiter(input_stream, delimiter)

    for line in line_iterator:
        n += 1
        
        # Phase 1: Initial Buffering
        # Fill the buffer up to the minimum window size before writing anything.
        if n <= window_min:
            lines.append(line)
            continue

        # Phase 2: Streaming Shuffle
        # Once the buffer is full, start the shuffling process.
        if n > predicted_n:
            predicted_n *= 2

        k = random.randint(0, predicted_n - 1)

        if k < len(lines):
            # Swap the new line with a random existing line and write the old line
            old_line = lines[k]
            lines[k] = line
            output_stream.write(old_line)
        else:
            # The scheduled position is outside the current buffer.
            # Append the line to the buffer so it can grow.
            lines.append(line)

    # Write out any remaining lines in the buffer in random order
    random.shuffle(lines)
    output_stream.writelines(lines)

def main():
    """
    Parses arguments and runs the streaming shuffle.
    """
    parser = argparse.ArgumentParser(
        description="A streaming version of the Linux shuf command.",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument(
        "-z", "--zero-terminated",
        action="store_true",
        help="Line delimiter is NUL, not newline."
    )
    parser.add_argument(
        "--window-min",
        type=int,
        default=1024,
        metavar="N",
        help="Minimum window size in lines for shuffling. Defaults to 1024."
    )

    args = parser.parse_args()

    if args.window_min <= 0:
        sys.stderr.write("Error: --window-min must be a positive integer\n")
        sys.exit(1)

    try:
        streaming_shuffle(
            sys.stdin.buffer,
            sys.stdout.buffer,
            zero_terminated=args.zero_terminated,
            window_min=args.window_min
        )
    except BrokenPipeError:
        # This can happen if the process we are piping to closes its stdin,
        # for example, when piping to `head`. We can safely ignore this.
        pass
    except KeyboardInterrupt:
        # Also exit gracefully on Ctrl+C
        pass

if __name__ == "__main__":
    main()