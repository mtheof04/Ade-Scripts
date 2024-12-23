#!/bin/bash

# Enable strict error handling
set -euo pipefail
IFS=$'\n\t'

# Define paths and parameters
DUCKDB_PATH="./build/release/duckdb"
CMD_PIPE="cmd_pipe"
OUT_PIPE="out_pipe"
QUERY_TYPES=("sequential" "aggregations" "sorting" "filtering" "joins")
FREQUENCIES_GHZ=("1.0GHz" "1.5GHz" "2.0GHz" "2.6GHz")

# Define default scale factors
DEFAULT_SFS=(100)

# Function to create directory structure
create_directories() {

    # Remove existing Files folder
    if [ -d "Files" ]; then
        echo "Removing existing Files folder..."
        rm -rf Files
    fi

    if [ -f "Files.zip" ]; then
        echo "Removing existing Files.zip..."
        rm -f Files.zip
    fi

    echo "Creating directory structure..."

    # Loop through each scale factor and create the directories
    for SF in "${DEFAULT_SFS[@]}"; do
        FOLDER_PATH="Files/SF$SF"
        mkdir -p "$FOLDER_PATH"
        echo "Folder created: $FOLDER_PATH"
    done

    echo "Directory structure created successfully."
}

# Create necessary directories
mkdir -p Files


# Function to start DuckDB in a new process
start_duckdb() {
    local db_file="$1"

    # Validate that a database file was provided
    if [ -z "$db_file" ]; then
        echo "Error: No database file provided to start_duckdb function." >&2
        echo "Usage: start_duckdb <database_file>" >&2
        exit 1
    fi
    
    # Check if the specified database file exists
    if [ ! -f "$db_file" ]; then
        echo "Error: Database file '$db_file' does not exist." >&2
        exit 1
    fi

    # Create named pipes if they don't exist
    [[ ! -p $CMD_PIPE ]] && mkfifo $CMD_PIPE
    [[ ! -p $OUT_PIPE ]] && mkfifo $OUT_PIPE
    
    # Start DuckDB process
    tail -f "$CMD_PIPE" | "$DUCKDB_PATH" "$db_file" > "$OUT_PIPE" &
    DUCKDB_PID=$!  # Capture the DuckDB process ID
    echo "Started DuckDB process with PID $DUCKDB_PID"
}

# Function to clean up DuckDB process and pipes
cleanup() {
    if [ -n "${DUCKDB_PID:-}" ]; then
        kill "$DUCKDB_PID" 2>/dev/null || true  # Kill DuckDB process
        wait "$DUCKDB_PID" 2>/dev/null || true  # Ensure DuckDB process is fully terminated
        echo "Killed DuckDB process: $DUCKDB_PID"
    fi
    pkill -P $$ tail 2>/dev/null || true  # Kill lingering tail processes
    rm -f "$CMD_PIPE" "$OUT_PIPE"  # Remove pipes
    echo "Cleanup complete."
}

# Function to set CPU frequency using cpupower and turbostat
set_cpu_frequency() {
    local freq=$1
    
    echo "Setting CPU frequency to ${freq}"
    
    # Set both the minimum and maximum frequency to the specified value
    sudo cpupower frequency-set -d "${freq}" -u "${freq}"
    sleep 5
    
    # Show the frequency information to verify
    cpupower frequency-info

    # Run turbostat for 1 second to check the frequency
    sudo turbostat -i 1 -n 1 || true
}

# Function to start pcm-memory
start_pcm_memory() {
    local pcm_output_file="$1"
    PCM_PID=$(sudo bash -c "pcm-memory > \"$pcm_output_file\" 2>&1 & echo \$!")
    echo "Starting pcm-memory in process $PCM_PID..."
}

# Function to stop pcm
stop_pcm_memory() {
    echo "Stopping pcm-memory with PID $PCM_PID..."
    sudo kill "$PCM_PID"

    # Wait for pcm-memory to terminate
    while ps -p "$PCM_PID" > /dev/null 2>&1; do
        echo "Waiting for pcm-memory (PID $PCM_PID) to terminate..."
        sleep 1
    done

    echo "pcm-memory (PID $PCM_PID) has been stopped."
}


# Function to run queries with dynamic iteration counts
run_query() {
    local QUERY_NAME="$1"
    local QUERY_SQL="$2"
    local SF="$3"

    TARGET_TIME=80  # Target cumulative execution time in seconds
    cumulative_time=0
    benchmark_run=1
    ITER=0  # Initialize iteration counter
    MIN_ITERATIONS=2  # Define the minimum number of benchmark iterations

    
    # ----------------------------
    # Load TPC-H Extension
    # ----------------------------
    echo "LOAD tpch;" > "$CMD_PIPE"  # Send the LOAD tpch; command
    echo "Loading TPC-H extension..." >&2

    echo "" >&2
    echo "=== Executing $QUERY_NAME ===" >&2

    # ----------------------------
    # Warm-Up Run
    # ----------------------------
    echo "Starting warm-up run for $QUERY_NAME..."
    
    ITER=0  # Warm-up run is not counted in benchmark iterations
    
    ITER_DIR="${FREQ_DIR}/Iterations"
    WARM_UP_DIR="${FREQ_DIR}/WarmUp/${QUERY_NAME^}"  # Capitalize first letter of query
    mkdir -p "$WARM_UP_DIR"

    PERF_BASE_DIR="${FREQ_DIR}/PerfStat/${QUERY_NAME^}"
    mkdir -p "$PERF_BASE_DIR"

    PCM_BASE_DIR="${FREQ_DIR}/PCM/${QUERY_NAME^}"

    mkdir -p "$PCM_BASE_DIR"

    ILO_TIMESTAMPS_FILE="$WARM_UP_DIR/ilo_power_timestamps.txt"

    start_time_query=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
    
    # Execute the query
    echo "$QUERY_SQL; SELECT '---END---' as end_signal;" > "$CMD_PIPE"

    while IFS= read -r line; do
        echo "$line"
        [[ "$line" == *'---END---'* ]] && break
    done < "$OUT_PIPE"

    end_time_query=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
    
    # Time calculation and logging
    start_epoch=$(date -d "$start_time_query" +%s)
    end_epoch=$(date -d "$end_time_query" +%s)
    total_execution_time=$((end_epoch - start_epoch))
    
    echo "Warm-Up Run:"
    echo "Start Time = $start_time_query, End Time = $end_time_query" >> "$ILO_TIMESTAMPS_FILE"
    echo "Total Query Execution Time: ${total_execution_time} seconds" | tee -a "$ILO_TIMESTAMPS_FILE"

    echo "Warm-up run completed in ${total_execution_time} seconds."
    echo ""

    # ----------------------------
    # Benchmark Runs
    # ----------------------------

    perf stat -e task-clock,context-switches,cpu-migrations,page-faults,cycles,instructions,branches,branch-instructions,branch-misses,bus-cycles,cache-references,cache-misses,cpu-cycles,ref-cycles,LLC-loads,LLC-load-misses,LLC-stores -p "$DUCKDB_PID" -o "$PERF_BASE_DIR/perf_stats_process.txt" &
    PERF_PID=$!

    # Start perf monitoring for overall system (System-wide Level 1)
    echo "Starting perf monitoring (System-wide Level 1)..."
    perf stat -a --topdown --td-level 1 -o "$PERF_BASE_DIR/perf_stats_system.txt" &
    PERF_SYSTEM_PID=$!

    # Check if perf started successfully
    if ! ps -p "$PERF_PID" > /dev/null 2>&1; then
        echo "Error: perf stat failed to start. Check permissions or kernel settings."
        cleanup
        exit 1
    fi

    # Start pcm-memory in the background and redirect output to the text file
    start_pcm_memory "$PCM_BASE_DIR/pcm_stats.txt"
    
    echo "Starting benchmark runs for $QUERY_NAME..."

    start_time_total=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

    while (( ITER < MIN_ITERATIONS )) || (( cumulative_time < TARGET_TIME )); do 
        ITER=$((ITER + 1))
        echo "Processing Benchmark Iteration $ITER for $QUERY_NAME at frequency ${freq}"

        ITER_DIR="${FREQ_DIR}/Iterations/Iteration$ITER"
        QUERY_DIR="${ITER_DIR}/${QUERY_NAME^}"  # Capitalize first letter of query
        mkdir -p "$QUERY_DIR"

        IOSTAT_LOG="$QUERY_DIR/iostat.log"
        MPSTAT_LOG="$QUERY_DIR/mpstat.log"
        ILO_TIMESTAMPS_FILE="$QUERY_DIR/ilo_power_timestamps.txt"

        echo "Starting system monitoring for Iteration $ITER..."
        iostat 1 >> "$IOSTAT_LOG" &
        IOSTAT_PID=$!
        mpstat 1 >> "$MPSTAT_LOG" &
        MPSTAT_PID=$!

        start_time_query=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
        
        # Execute the query
        echo "$QUERY_SQL; SELECT '---END---' as end_signal;" > "$CMD_PIPE"

        while IFS= read -r line; do
            echo "$line"
            [[ "$line" == *'---END---'* ]] && break
        done < "$OUT_PIPE"

        end_time_query=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
        
        # Time calculation and logging
        start_epoch=$(date -d "$start_time_query" +%s)
        end_epoch=$(date -d "$end_time_query" +%s)
        iteration_time=$((end_epoch - start_epoch))
        cumulative_time=$((cumulative_time + iteration_time))
        
        echo "Start Time = $start_time_query, End Time = $end_time_query" >> "$ILO_TIMESTAMPS_FILE"
        echo "Total Query Execution Time for Iteration $ITER: ${iteration_time} seconds" | tee -a "$ILO_TIMESTAMPS_FILE"
        echo "Cumulative Execution Time: ${cumulative_time} seconds" | tee -a "$ILO_TIMESTAMPS_FILE"

        # Kill the monitoring processes
        if [ -n "$IOSTAT_PID" ] && kill -0 "$IOSTAT_PID" 2>/dev/null; then
            kill "$IOSTAT_PID"
            echo "Stopped iostat (PID: $IOSTAT_PID)"
        fi
        if [ -n "$MPSTAT_PID" ] && kill -0 "$MPSTAT_PID" 2>/dev/null; then
            kill "$MPSTAT_PID"
            echo "Stopped mpstat (PID: $MPSTAT_PID)"
        fi

        echo "Iteration $ITER completed in ${iteration_time} seconds. Cumulative time: ${cumulative_time} seconds."
        echo ""

    done

    end_time_total=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
    start_epoch_total=$(date -d "$start_time_total" +%s)
    end_epoch_total=$(date -d "$end_time_total" +%s)
    total_time_all_iterations=$((end_epoch_total - start_epoch_total))

    # Stop perf monitoring processes with SIGINT to ensure output is written
    echo "Stopping perf monitoring..."
    set +e  # Disable 'exit on error' to ensure cleanup completes even on error

    kill -SIGINT "$PERF_PID" 2>/dev/null || true
    wait "$PERF_PID" 2>/dev/null || true
    echo "Perf stats saved to $PERF_BASE_DIR/perf_stats_process.txt."

    # Stop system-wide perf monitoring
    kill -SIGINT "$PERF_SYSTEM_PID" 2>/dev/null || true
    wait "$PERF_SYSTEM_PID" 2>/dev/null || true
    echo "System-wide perf stats saved to $PERF_BASE_DIR/perf_stats_system.txt."

    set -e  # Re-enable 'exit on error'

    echo "Perf monitoring completed for $QUERY_NAME."

    # Stop pcm-memory
    stop_pcm_memory

    echo "pcm-memory output saved to $PCM_BASE_DIR/pcm_stats.txt."

    ILO_TIMESTAMPS_ALL="${FREQ_DIR}/ilo_power_timestamps_all.txt"
    echo "$QUERY_NAME:" >> "$ILO_TIMESTAMPS_ALL"
    echo "Start Time = $start_time_total, End Time = $end_time_total" >> "$ILO_TIMESTAMPS_ALL"
    echo "Total Time for All Iterations of $QUERY_NAME: ${total_time_all_iterations} seconds" | tee -a "$ILO_TIMESTAMPS_ALL"
    echo "" >&2

    echo "Executing iLO power for $QUERY_NAME on iteration $ITER..."
    
    # Execute a final iLO power script
    ILO_PREFIX_FINAL="${FREQ_DIR}/ilo_power_all.txt"
    echo "Executing iLO power script at frequency ${freq}..."
    python3 iLO_power.py username password URL >> "$ILO_PREFIX_FINAL"
    echo "------------------------------------------------------------------------------------" >> "$ILO_PREFIX_FINAL"  

    cleanup   

    # Define the temporary file/directory name based on DATABASE_FILE
    TMP_DATABASE_FILE="${DATABASE_FILE}.tmp"

    # Check if the temporary file or directory exists
    if [ -e "$TMP_DATABASE_FILE" ]; then
        rm -rf "$TMP_DATABASE_FILE"
        if [ $? -eq 0 ]; then
            echo "Deleted existing temporary directory: $TMP_DATABASE_FILE" >&2
        else
            echo "Failed to delete temporary directory: $TMP_DATABASE_FILE" >&2
            exit 1
        fi
    else
        echo "No existing temporary file or directory found for: $TMP_DATABASE_FILE" >&2
    fi
     
}

# Ensure cleanup runs on script exit or interrupt
trap cleanup EXIT INT TERM

# Query definitions
SEQUENTIAL="
SELECT
    l_orderkey,
    l_partkey,
    l_suppkey,
    l_linenumber,
    l_quantity,
    l_extendedprice,
    l_discount,
    l_tax,
    l_returnflag,
    l_linestatus,
    l_shipdate,
    l_commitdate,
FROM lineitem;"

JOINS="
SELECT
    l.l_orderkey,
    l.l_partkey,
    l.l_suppkey,
    l.l_linenumber,
    l.l_quantity,
    l.l_extendedprice,
    l.l_discount,
    l.l_tax,
    l.l_returnflag,
    l.l_linestatus,
    l.l_shipdate,
    l.l_commitdate,
    o.o_orderpriority
FROM lineitem l
JOIN orders o ON l.l_orderkey = o.o_orderkey;"

AGGREGATIONS="
SELECT 
    l_orderkey,
    l_partkey,
    l_suppkey,
    l_linenumber,
    l_quantity,
    l_extendedprice,
    l_discount,
    l_tax,
    l_returnflag,
    l_linestatus,
    l_shipdate,
    l_commitdate,
    AVG(l_extendedprice) AS avg_price
FROM lineitem
GROUP BY 
    l_orderkey,
    l_partkey,
    l_suppkey,
    l_linenumber,
    l_quantity,
    l_extendedprice,
    l_discount,
    l_tax,
    l_returnflag,
    l_linestatus,
    l_shipdate,
    l_commitdate;"

SORTING="
SELECT
    l_orderkey,
    l_partkey,
    l_suppkey,
    l_linenumber,
    l_quantity,
    l_extendedprice,
    l_discount,
    l_tax,
    l_returnflag,
    l_linestatus,
    l_shipdate,
    l_commitdate,
FROM lineitem
ORDER BY l_extendedprice DESC;"

FILTERING="
SELECT
    l_orderkey,
    l_partkey,
    l_suppkey,
    l_linenumber,
    l_quantity,
    l_extendedprice,
    l_discount,
    l_tax,
    l_returnflag,
    l_linestatus,
    l_shipdate,
    l_commitdate,
FROM lineitem
WHERE (l_shipdate BETWEEN '1992-01-01' AND '1994-12-31')
   OR (l_shipdate BETWEEN '1996-01-01' AND '1997-12-31');"

echo "Starting query executions..." >&2

create_directories

for SF in "${DEFAULT_SFS[@]}"; do
    
    for freq in "${FREQUENCIES_GHZ[@]}"; do
        # Define the directory path based on the frequency
        if [ "$FREQUENCY_CHANGE" == "yes" ]; then
            set_cpu_frequency "$freq"
            FREQ_DIR="Files/SF${SF}/F${freq%GHz}"
            echo "$FREQ_DIR"
        else
            FREQ_DIR="Files"
        fi

        mkdir -p "$FREQ_DIR"

        for QUERY in "${QUERY_TYPES[@]}"; do

            echo "Wait 10 minutes to stabilize the machine..."
            sleep 600

            echo "Processing query type: $QUERY at frequency ${freq}"
            
            QUERY_DIR="${FREQ_DIR}/Iterations/Iteration1/${QUERY^}"
            mkdir -p "$QUERY_DIR"

            echo "Executing iLO power for $QUERY..."
            python3 iLO_power.py username password URL >> "${QUERY_DIR}/ilo_power_idle.txt"
        
            DATABASE_FILE="${SF}.db"

            echo "Using database: $DATABASE_FILE for Scale Factor: $SF" >&2
            
            # Start DuckDB
            start_duckdb "$DATABASE_FILE"

            echo "Wait 10 seconds to stabilize the machine..."
            sleep 10

            case "$QUERY" in
                sequential)
                    run_query "Sequential" "$SEQUENTIAL" "$SF"
                    ;;
                joins)
                    run_query "Joins" "$JOINS" "$SF"
                    ;;
                aggregations)
                    run_query "Aggregations" "$AGGREGATIONS" "$SF"
                    ;;
                sorting)
                    run_query "Sorting" "$SORTING" "$SF"
                    ;;
                filtering)
                    run_query "Filtering" "$FILTERING" "$SF"
                    ;;
                *)
                    echo "Unknown query type: $QUERY"
                    exit 1
                    ;;
            esac
        done
    done
done

echo "All queries executed successfully." >&2
echo "Zipping the Files directory into Files.zip..."
zip -r "Files.zip" Files
echo "Zipping completed: Files.zip"
echo "Script execution completed."
