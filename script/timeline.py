import os
import sys
import json

def main(log_dir):
    print("Working dir", log_dir)
    all_logs = os.listdir(log_dir)
    json_output = {}
    for log_file in all_logs:
        with open(os.path.join(log_dir, log_file)) as f:
            for line in f.readlines():
                elements = line.split()
                if len(elements) >= 6 and elements[5] == "[TIMELINE]":
                    timestamp = int(elements[0])
                    ip_pid_tid = elements[1]
                    ip, pid, tid = ip_pid_tid.split(":")
                    filename_line = elements[2]
                    function_name = elements[3]
                    assert elements[4] == "]:"
                    timeline_id = elements[6]
                    timeline_tag = elements[7]
                    message = " ".join(elements[8:])
                    print(timestamp, ip, pid, tid, filename_line, function_name, message)


if __name__ == "__main__":
    assert len(sys.argv) == 2, "Usage: python timeline.py LOG_DIR"
    log_dir = sys.argv[1]
    main(log_dir)