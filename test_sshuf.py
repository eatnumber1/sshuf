import unittest
import io
import sys
from unittest.mock import patch, MagicMock
from sshuf import streaming_shuffle, main

class TestStreamingShuffle(unittest.TestCase):

    def run_test_with_params(self, test_func):
        """Helper to run a test with both newline and null delimiters."""
        for zero_terminated in [False, True]:
            with self.subTest(zero_terminated=zero_terminated):
                test_func(zero_terminated)

    def test_shuffle_preserves_lines(self):
        def test_logic(zero_terminated):
            delimiter = b'\0' if zero_terminated else b'\n'

            input_lines = [f"line {i}".encode('utf-8') + delimiter for i in range(100)]
            input_stream = io.BytesIO(b"".join(input_lines))
            output_stream = io.BytesIO()

            streaming_shuffle(input_stream, output_stream, zero_terminated=zero_terminated)

            output_lines = output_stream.getvalue().split(delimiter)
            if output_lines[-1] == b'':
                output_lines.pop()
            
            input_lines_no_delim = [line.strip(delimiter) for line in input_lines]

            self.assertEqual(len(output_lines), len(input_lines_no_delim))
            self.assertSetEqual(set(output_lines), set(input_lines_no_delim))
        
        self.run_test_with_params(test_logic)

    def test_shuffle_changes_order(self):
        def test_logic(zero_terminated):
            delimiter = b'\0' if zero_terminated else b'\n'

            input_lines = [f"line {i}".encode('utf-8') + delimiter for i in range(100)]
            input_stream = io.BytesIO(b"".join(input_lines))
            output_stream = io.BytesIO()

            streaming_shuffle(input_stream, output_stream, zero_terminated=zero_terminated)

            output_content = output_stream.getvalue()
            self.assertNotEqual(output_content, b"".join(input_lines))

        self.run_test_with_params(test_logic)

    def test_empty_input(self):
        def test_logic(zero_terminated):
            input_stream = io.BytesIO(b"")
            output_stream = io.BytesIO()
            streaming_shuffle(input_stream, output_stream, zero_terminated=zero_terminated)
            self.assertEqual(output_stream.getvalue(), b"")
        
        self.run_test_with_params(test_logic)

    def test_single_line_input(self):
        def test_logic(zero_terminated):
            delimiter = b'\0' if zero_terminated else b'\n'

            line = b"one line" + delimiter
            input_stream = io.BytesIO(line)
            output_stream = io.BytesIO()
            streaming_shuffle(input_stream, output_stream, zero_terminated=zero_terminated)
            self.assertEqual(output_stream.getvalue(), line)

        self.run_test_with_params(test_logic)

    def test_window_min_buffers_all_lines(self):
        def test_logic(zero_terminated):
            delimiter = b'\0' if zero_terminated else b'\n'

            input_lines = [f"line {i}".encode('utf-8') + delimiter for i in range(50)]
            input_stream = io.BytesIO(b"".join(input_lines))
            output_stream = MagicMock()

            streaming_shuffle(input_stream, output_stream, zero_terminated=zero_terminated, window_min=100)

            self.assertEqual(output_stream.write.call_count, 0)
            self.assertEqual(output_stream.writelines.call_count, 1)
            final_lines = output_stream.writelines.call_args[0][0]
            self.assertEqual(len(final_lines), 50)
            self.assertSetEqual(set(final_lines), set(input_lines))

        self.run_test_with_params(test_logic)

    def test_duplicate_lines(self):
        def test_logic(zero_terminated):
            delimiter = b'\0' if zero_terminated else b'\n'
            # Create input with duplicates: "A", "A", "B", "B"
            input_lines = [b"A" + delimiter, b"A" + delimiter, b"B" + delimiter, b"B" + delimiter]
            input_stream = io.BytesIO(b"".join(input_lines))
            output_stream = io.BytesIO()

            streaming_shuffle(input_stream, output_stream, zero_terminated=zero_terminated)

            output_content = output_stream.getvalue()
            output_split = output_content.split(delimiter)
            if output_split[-1] == b'':
                output_split.pop()
            
            # Expected: Two A's and two B's
            self.assertEqual(len(output_split), 4)
            self.assertEqual(output_split.count(b"A"), 2)
            self.assertEqual(output_split.count(b"B"), 2)

        self.run_test_with_params(test_logic)

    def test_no_trailing_delimiter(self):
        def test_logic(zero_terminated):
            delimiter = b'\0' if zero_terminated else b'\n'
            # Input: "line1\nline2" (no newline at very end)
            input_data = b"line1" + delimiter + b"line2"
            input_stream = io.BytesIO(input_data)
            output_stream = io.BytesIO()

            streaming_shuffle(input_stream, output_stream, zero_terminated=zero_terminated)

            output_content = output_stream.getvalue()
            
            # The output *might* add a trailing delimiter to the last line because
            # we read it, shuffle it, and then write it using the buffered lines.
            # However, our read_lines_with_delimiter yields the buffer *as is*.
            # If the last chunk is "line2", it yields "line2".
            # The writelines at the end writes it as "line2".
            # So the output should contain "line1" (with delim) and "line2" (maybe without).
            # The shuffler doesn't enforce adding a newline if one wasn't there.
            
            # Let's just check that we got both lines back.
            self.assertIn(b"line1" + delimiter, output_content)
            self.assertIn(b"line2", output_content)
            
            # Check total length matches input length (preserves bytes exactly)
            self.assertEqual(len(output_content), len(input_data))

        self.run_test_with_params(test_logic)

    def test_larger_input_exceeds_default_window(self):
        # Default window is 1024. Let's use 2000 lines to ensure growth.
        # This is not parameterized to save time, as the logic is the same.
        input_lines = [f"line {i}".encode('utf-8') + b'\n' for i in range(2000)]
        input_stream = io.BytesIO(b"".join(input_lines))
        output_stream = io.BytesIO()

        streaming_shuffle(input_stream, output_stream) # uses default window_min=1024

        output_lines = output_stream.getvalue().splitlines(keepends=True)
        self.assertEqual(len(output_lines), 2000)
        self.assertSetEqual(set(output_lines), set(input_lines))

    def test_invalid_utf8(self):
        # The byte 0x83 is invalid in UTF-8.
        invalid_line = b"filename_with_\x83_char\n"
        input_data = invalid_line + b"valid_line\n"
        input_stream = io.BytesIO(input_data)
        output_stream = io.BytesIO()

        streaming_shuffle(input_stream, output_stream)

        output_content = output_stream.getvalue()
        output_lines = output_content.split(b'\n')
        if output_lines[-1] == b'':
            output_lines.pop()

        self.assertEqual(len(output_lines), 2)
        self.assertIn(b"filename_with_\x83_char", output_content)
        self.assertIn(b"valid_line", output_content)

    def test_only_newlines(self):
        def test_logic(zero_terminated):
            delimiter = b'\0' if zero_terminated else b'\n'
            # Input: 3 empty lines (just delimiters)
            input_lines = [delimiter, delimiter, delimiter]
            input_stream = io.BytesIO(b"".join(input_lines))
            output_stream = io.BytesIO()

            streaming_shuffle(input_stream, output_stream, zero_terminated=zero_terminated)

            output_content = output_stream.getvalue()
            output_split = output_content.split(delimiter)
            if output_split[-1] == b'':
                output_split.pop()

            # Expect 3 empty lines
            self.assertEqual(len(output_split), 3)
            for line in output_split:
                self.assertEqual(line, b"")

        self.run_test_with_params(test_logic)

    def test_main_zero_terminated_arg(self):
        testargs = ["sshuf.py", "-z"]
        input_data = b"a\0b\0c\0"
        
        mock_stdin = MagicMock()
        mock_stdin.buffer = io.BytesIO(input_data)
        
        mock_stdout = MagicMock()
        mock_stdout.buffer = io.BytesIO()

        with patch.object(sys, 'argv', testargs):
            with patch('sys.stdin', mock_stdin):
                with patch('sys.stdout', mock_stdout):
                    main()
        
        output = mock_stdout.buffer.getvalue()
        self.assertSetEqual(set(output.split(b'\0')), set(input_data.split(b'\0')))

    @patch('sys.stderr', new_callable=io.StringIO)
    def test_window_min_positive_validation(self, mock_stderr):
        for val in [0, -1, -100]:
            with self.subTest(value=val):
                testargs = ["sshuf.py", "--window-min", str(val)]
                with patch.object(sys, 'argv', testargs):
                    with self.assertRaises(SystemExit) as cm:
                        main()
                    self.assertEqual(cm.exception.code, 1)
                self.assertIn("must be a positive integer", mock_stderr.getvalue())
                mock_stderr.seek(0)
                mock_stderr.truncate(0)

if __name__ == '__main__':
    unittest.main()