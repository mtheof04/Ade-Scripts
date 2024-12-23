#!/bin/bash

# Define default scale factors
DEFAULT_SFS=(300)

# Number of iterations
NUM_ITERATIONS=4

# Frequencies to loop over
FREQUENCIES_GHZ=("1.0GHz" "1.5GHz" "2.0GHz" "2.6GHz")

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

create_directories

# Path to DuckDB
DUCKDB_CMD="./build/release/duckdb"

# Function to set CPU frequency using cpupower
set_cpu_frequency() {
    local freq=$1

    sudo cpupower frequency-set -g userspace
    sleep 5

    echo "Setting CPU frequency to ${freq}"
    
    # Set both the minimum and maximum frequency to the specified value
    sudo cpupower frequency-set -d ${freq} -u ${freq}

    # Show the frequency information to verify
    cpupower frequency-info

    # Run turbostat for 1 second to check the frequency
    sudo turbostat -i 1 -n 1 || true
}

# Function to drop caches
drop_caches() {
    echo "Attempting to drop caches..." >&2

    sudo sh -c 'echo 1 > /proc/sys/vm/drop_caches' && \
        echo "Caches (level 1) dropped successfully" >&2 || \
        echo "Failed to drop caches (level 1)" >&2

    sudo sh -c 'echo 2 > /proc/sys/vm/drop_caches' && \
        echo "Caches (level 2) dropped successfully" >&2 || \
        echo "Failed to drop caches (level 2)" >&2

    sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches' && \
        echo "Caches (level 3) dropped successfully" >&2 || \
        echo "Failed to drop caches (level 3)" >&2
}

# Function to start pcm-memory
start_pcm_memory() {
    local pcm_output_file="$1"
    PCM_PID=$(sudo bash -c "pcm-memory > \"$pcm_output_file\" 2>&1 & echo \$!")
    echo "Starting pcm-memory..."
}

# Function to stop pcm
stop_pcm_memory() {
    echo "Stopping pcm-memory"
    sudo kill "$PCM_PID"

    # Wait for pcm-memory to terminate
    while ps -p "$PCM_PID" > /dev/null 2>&1; do
        echo "Waiting for pcm-memory (PID $PCM_PID) to terminate..."
        sleep 1
    done

    echo "pcm-memory (PID $PCM_PID) has been stopped."
}

# Loop over scale factors
for SCALE_FACTOR in "${DEFAULT_SFS[@]}"; do

    # Data directory path based on the scale factor
    DATA_PATH="./SF${SCALE_FACTOR}"  # e.g. ./SF100 or ./SF300

    # Check if the data directory exists
    if [ ! -d "$DATA_PATH" ]; then
        echo "Data directory $DATA_PATH does not exist. Please ensure the data for scale factor $SCALE_FACTOR is available."
        continue
    fi

    # Loop over frequencies
    for freq in "${FREQUENCIES_GHZ[@]}"; do
        if [ "$freq" != "default" ]; then
            set_cpu_frequency "$freq"
            FREQ_DIR="Files/SF${SCALE_FACTOR}/F${freq%GHz}"
        else
            FREQ_DIR="Files/SF${SCALE_FACTOR}"
        fi

        mkdir -p "$FREQ_DIR"

        echo "Processing at frequency ${freq} for scale factor ${SCALE_FACTOR}..."

        # Loop over file formats
        for FILE_FORMAT in "csv" "parquet"; do

            echo "Running for file format: $FILE_FORMAT"

            FORMAT_DIR="${FREQ_DIR}/${FILE_FORMAT^}"
            mkdir -p "$FORMAT_DIR"

            drop_caches

            echo "Wait 10 minutes to stabilize the machine..."
            sleep 600
            
            echo "Executing iLO power for $FILE_FORMAT ..."
            python3 iLO_power.py username password URL >> "${FORMAT_DIR}/ilo_power_idle.txt"

            start_time_total=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

            # Step 1: Load data as many times as iterations
            for ITER in $(seq 1 $NUM_ITERATIONS); do

                drop_caches

                ITER_DIR="${FORMAT_DIR}/Iteration${ITER}"
                mkdir -p "$ITER_DIR"
                echo "Loading data into DuckDB for $FILE_FORMAT Iteration $ITER at frequency ${freq}..."

                # Define database file name
                DB_FILE=""

                IOSTAT_LOG="$ITER_DIR/iostat.log"
                MPSTAT_LOG="$ITER_DIR/mpstat.log"
                TOP_DUCKDB_LOG="$ITER_DIR/top_duckdb.log"

                # Check if DUCKDB_CMD is set and executable
                if [[ -z "$DUCKDB_CMD" || ! -x "$(command -v "$DUCKDB_CMD")" ]]; then
                    echo "Error: DuckDB command not found or not executable."
                    exit 1
                fi

                # Check if DATA_PATH exists
                if [[ -z "$DATA_PATH" || ! -d "$DATA_PATH" ]]; then
                    echo "Error: DATA_PATH is not set or does not exist."
                    exit 1
                fi

                # Create a named pipe for SQL commands
                SQL_PIPE=$(mktemp -u)
                mkfifo "$SQL_PIPE"
                if [[ ! -p "$SQL_PIPE" ]]; then
                    echo "Error: Failed to create named pipe."
                    cleanup
                fi

                # Start DuckDB reading from the named pipe, in background
                "$DUCKDB_CMD" "$DB_FILE" < "$SQL_PIPE" &
                DUCKDB_PID=$!
                echo "DuckDB started with PID: $DUCKDB_PID"

                # Give DuckDB a moment to start
                sleep 1

                # Verify DuckDB is running
                if ! ps -p "$DUCKDB_PID" > /dev/null 2>&1; then
                    echo "Error: DuckDB process failed to start."
                    cleanup
                fi

                # Start collecting data
                iostat 1 > "$IOSTAT_LOG" &
                IOSTAT_PID=$!
                mpstat 1 > "$MPSTAT_LOG" &
                MPSTAT_PID=$!

                perf stat -e task-clock,context-switches,cpu-migrations,page-faults,cycles,instructions,branches,branch-instructions,branch-misses,bus-cycles,cache-references,cache-misses,cpu-cycles,ref-cycles,LLC-loads,LLC-load-misses,LLC-stores -p "$DUCKDB_PID" -o "$ITER_DIR/perf_stats_metrics.txt" &
                PERF_PID=$!

                # Start perf monitoring for overall system (System-wide Level 1)
                echo "Starting perf monitoring (System-wide Level 1)..."
                perf stat -a --topdown --td-level 1 -o "$ITER_DIR/perf_stats_system.txt" &
                PERF_SYSTEM_PID=$!

                # Check if perf started successfully
                if ! ps -p "$PERF_PID" > /dev/null 2>&1; then
                    echo "Error: perf stat failed to start. Check permissions or kernel settings."
                    cleanup
                    exit 1
                fi

                # Start pcm-memory in the background and redirect output to the text file
                start_pcm_memory "$ITER_DIR/pcm_stats.txt"

                # Start top monitoring for DuckDB process
                echo "Starting top monitoring for DuckDB PID $DUCKDB_PID..."
                top -b -d 1 -p "$DUCKDB_PID" > "$TOP_DUCKDB_LOG" &
                TOP_PID=$!
                echo "Top monitoring started with PID: $TOP_PID"

                # Start the time tracker for the query execution
                start_time_query=$(date -u +"%Y-%m-%dT%H:%M:%SZ")


                # Send SQL commands to DuckDB via the named pipe
                {
                    echo "BEGIN TRANSACTION;"

                    # Create tables
                    echo "CREATE TABLE IF NOT EXISTS part (
                        p_partkey INTEGER,
                        p_name VARCHAR(55),
                        p_mfgr VARCHAR(25),
                        p_brand VARCHAR(10),
                        p_type VARCHAR(25),
                        p_size INTEGER,
                        p_container VARCHAR(10),
                        p_retailprice DECIMAL(15,2)
                    );"

                    echo "CREATE TABLE IF NOT EXISTS customer (
                        c_custkey INTEGER,
                        c_name VARCHAR(25),
                        c_address VARCHAR(40),
                        c_nationkey INTEGER,
                        c_phone VARCHAR(15),
                        c_acctbal DECIMAL(15,2),
                        c_mktsegment VARCHAR(10)
                    );"

                    echo "CREATE TABLE IF NOT EXISTS supplier (
                        s_suppkey INTEGER,
                        s_name VARCHAR(25),
                        s_address VARCHAR(40),
                        s_nationkey INTEGER,
                        s_phone VARCHAR(15),
                        s_acctbal DECIMAL(15,2)
                    );"

                    echo "CREATE TABLE IF NOT EXISTS nation (
                        n_nationkey INTEGER,
                        n_name VARCHAR(25),
                        n_regionkey INTEGER
                    );"

                    echo "CREATE TABLE IF NOT EXISTS region (
                        r_regionkey INTEGER,
                        r_name VARCHAR(25)
                    );"

                    echo "CREATE TABLE IF NOT EXISTS orders (
                        o_orderkey INTEGER,
                        o_custkey INTEGER,
                        o_orderstatus CHAR(1),
                        o_totalprice DECIMAL(15,2),
                        o_orderdate DATE,
                        o_orderpriority VARCHAR(15),
                        o_clerk VARCHAR(15),
                        o_shippriority INTEGER
                    );"

                    echo "CREATE TABLE IF NOT EXISTS partsupp (
                        ps_partkey INTEGER,
                        ps_suppkey INTEGER,
                        ps_availqty INTEGER,
                        ps_supplycost DECIMAL(15,2)
                    );"

                    echo "CREATE TABLE IF NOT EXISTS lineitem (
                        l_orderkey INTEGER,
                        l_partkey INTEGER,
                        l_suppkey INTEGER,
                        l_linenumber INTEGER,
                        l_quantity DECIMAL(15,2),
                        l_extendedprice DECIMAL(15,2),
                        l_discount DECIMAL(15,2),
                        l_tax DECIMAL(15,2),
                        l_returnflag CHAR(1),
                        l_linestatus CHAR(1),
                        l_shipdate DATE,
                        l_commitdate DATE,
                        l_receiptdate DATE,
                        l_shipinstruct VARCHAR(25),
                        l_shipmode VARCHAR(10)
                    );"

                    # Insert data based on format
                    echo "INSERT INTO part SELECT * FROM read_${FILE_FORMAT}('${DATA_PATH}/part.${FILE_FORMAT}');"
                    echo "INSERT INTO customer SELECT * FROM read_${FILE_FORMAT}('${DATA_PATH}/customer.${FILE_FORMAT}');"
                    echo "INSERT INTO supplier SELECT * FROM read_${FILE_FORMAT}('${DATA_PATH}/supplier.${FILE_FORMAT}');"
                    echo "INSERT INTO nation SELECT * FROM read_${FILE_FORMAT}('${DATA_PATH}/nation.${FILE_FORMAT}');"
                    echo "INSERT INTO region SELECT * FROM read_${FILE_FORMAT}('${DATA_PATH}/region.${FILE_FORMAT}');"
                    echo "INSERT INTO orders SELECT * FROM read_${FILE_FORMAT}('${DATA_PATH}/orders.${FILE_FORMAT}');"
                    echo "INSERT INTO partsupp SELECT * FROM read_${FILE_FORMAT}('${DATA_PATH}/partsupp.${FILE_FORMAT}');"
                    echo "INSERT INTO lineitem SELECT * FROM read_${FILE_FORMAT}('${DATA_PATH}/lineitem.${FILE_FORMAT}');"

                    echo "COMMIT;"
                } > "$SQL_PIPE"

                # Close the write end of the named pipe
                exec 3>&-

                # Wait for DuckDB process to complete
                wait "$DUCKDB_PID"
                echo "DuckDB process (PID $DUCKDB_PID) has completed."

                # Track the end time of the query
                end_time_query=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

                # Stop top monitoring
                if [[ -n "$TOP_PID" ]] && kill -0 "$TOP_PID" 2>/dev/null; then
                    kill "$TOP_PID"
                    wait "$TOP_PID" 2>/dev/null || true
                    echo "Top monitoring stopped for DuckDB PID: $DUCKDB_PID"
                fi

                # Remove named pipe
                rm -f "$SQL_PIPE"
                echo "Named pipe removed."

                echo "Data Load completed for $FILE_FORMAT Iteration $ITER at frequency ${freq}!"

                # Calculate total query execution time in seconds
                start_epoch=$(date -d "$start_time_query" +%s)
                end_epoch=$(date -d "$end_time_query" +%s)
                total_execution_time=$((end_epoch - start_epoch))

                # Log query execution times
                echo "Start Time = $start_time_query, End Time = $end_time_query" >> "$ITER_DIR/ilo_power_timestamps.txt"
                echo "Total Execution Time: ${total_execution_time} seconds" | tee -a "$ITER_DIR/ilo_power_timestamps.txt"

                # Stop collecting iostat and mpstat data
                if [ -n "$IOSTAT_PID" ] && kill -0 "$IOSTAT_PID" 2>/dev/null; then
                    kill "$IOSTAT_PID"
                    echo "Stopped iostat for $FILE_FORMAT Iteration $ITER"
                fi
                if [ -n "$MPSTAT_PID" ] && kill -0 "$MPSTAT_PID" 2>/dev/null; then
                    kill "$MPSTAT_PID"
                    echo "Stopped mpstat for $FILE_FORMAT Iteration $ITER"
                fi

                # Stop perf monitoring processes with SIGINT to ensure output is written
                echo "Stopping perf monitoring..."
                set +e  # Disable 'exit on error' to ensure cleanup completes even on error

                kill -SIGINT "$PERF_PID" 2>/dev/null || true
                wait "$PERF_PID" 2>/dev/null || true
                echo "Perf stats saved to $ITER_DIR/perf_stats_process.txt."

                # Stop system-wide perf monitoring
                kill -SIGINT "$PERF_SYSTEM_PID" 2>/dev/null || true
                wait "$PERF_SYSTEM_PID" 2>/dev/null || true
                echo "System-wide perf stats saved to $ITER_DIR/perf_stats_system.txt."

                set -e  # Re-enable 'exit on error'
                
                stop_pcm_memory
                
                echo "Executing iLO power script for $FILE_FORMAT at frequency ${freq}..."
                python3 iLO_power.py username password URL >> "${FORMAT_DIR}/ilo_power_all.txt"
                echo "------------------------------------------------------------------------------------" >> "${FORMAT_DIR}/ilo_power_all.txt"
                echo ""

            done

            # End the total timer and calculate total time for all iterations
            end_time_total=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
            start_epoch_total=$(date -d "$start_time_total" +%s)
            end_epoch_total=$(date -d "$end_time_total" +%s)
            total_time_all_iterations=$((end_epoch_total - start_epoch_total))

            # Log the total time taken for all iterations to a file
            ILO_TIMESTAMPS_ALL="$FORMAT_DIR/ilo_power_timestamps_all.txt"
            echo "Start Time = $start_time_total, End Time = $end_time_total" >> "$ILO_TIMESTAMPS_ALL"
            echo "Total Time for All Iterations: ${total_time_all_iterations} seconds" | tee -a "$ILO_TIMESTAMPS_ALL"

        done

    done

done

echo "All iterations for all file formats and scale factors completed."

echo "Zipping the Files directory into Files.zip..."
zip -r "Files.zip" Files
echo "Zipping completed: Files.zip"
