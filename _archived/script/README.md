# How to run the timeline script 

Use `reduce_test.sh` as an example:
1. Run 
    ```bash
    bash reduce_test.sh
    ```
    You will get a bunch of log files under `log/YYMMDD-HHMMSS-reduce/`.
2. Run the script
    ```bash
    python script/timeline.py log/YYMMDD-HHMMSS-reduce/
    ```
    The result json file will be dumped to `log/YYMMDD-HHMMSS-reduce/timeline.json`.
3. open `chrome://tracing` in your chrome browser, and then load the json file above. Then you will see the timeline.
