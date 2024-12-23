#!/bin/bash

# Define the frequency change option
FREQUENCY_CHANGE="yes"

# Define default scale factors
DEFAULT_SFS=(100)

if [ "$FREQUENCY_CHANGE" == "yes" ]; then
    FREQUENCIES_GHZ=("1.0GHz" "1.5GHz" "2.0GHz" "2.6GHz")
else
    FREQUENCIES_GHZ=("default")
fi

thread_counts=(48)

# Define the target cumulative time in seconds
TARGET_TIME=80

# Define the path to the DuckDB binary
DUCKDB_PATH="./build/release/duckdb"

# === Function Definitions ===

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

# Function to set CPU frequency using cpupower
set_cpu_frequency() {
    local freq=$1

    sudo cpupower frequency-set -g userspace
    
    echo "Setting CPU frequency to ${freq}"

    # Set both the minimum and maximum frequency to the specified value
    sudo cpupower frequency-set -d "${freq}" -u "${freq}"
    sleep 5

    # Show the frequency information to verify
    cpupower frequency-info

    # Run turbostat for 1 second to check the frequency
    sudo timeout 1s turbostat 1

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

# Function to start DuckDB in a new process
start_duckdb() {
    local sf="$1"
    local formatted_freq="$2"
    local CMD_PIPE="cmd_pipe_sf${sf}_f${formatted_freq}"
    local OUT_PIPE="out_pipe_sf${sf}_f${formatted_freq}"

    # Create named pipes if they don't exist
    [[ ! -p $CMD_PIPE ]] && mkfifo "$CMD_PIPE"
    [[ ! -p $OUT_PIPE ]] && mkfifo "$OUT_PIPE"

    # Start DuckDB process
    tail -f "$CMD_PIPE" | "$DUCKDB_PATH" "$DATABASE_FILE" > "$OUT_PIPE" &
    DUCKDB_PID=$!  # Capture the DuckDB process ID
    echo "Started DuckDB process with PID $DUCKDB_PID for Scale Factor: $sf, Frequency: $formatted_freq GHz"
}

# Function to clean up DuckDB process and pipes
cleanup() {
    local sf="$1"
    local formatted_freq="$2"
    local CMD_PIPE="cmd_pipe_sf${sf}_f${formatted_freq}"
    local OUT_PIPE="out_pipe_sf${sf}_f${formatted_freq}"

    if [ -n "${DUCKDB_PID:-}" ]; then
        kill "$DUCKDB_PID" 2>/dev/null || true  # Kill DuckDB process
        wait "$DUCKDB_PID" 2>/dev/null || true  # Ensure DuckDB process is fully terminated
        echo "Killed DuckDB process: $DUCKDB_PID for Scale Factor: $sf, Frequency: $formatted_freq GHz"
    fi
    pkill -P $$ tail 2>/dev/null || true  # Kill lingering tail processes
    rm -f "$CMD_PIPE" "$OUT_PIPE"  # Remove pipes
    echo "Cleanup complete for Scale Factor: $sf, Frequency: $formatted_freq GHz."
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


# === Main Script Execution ===

create_directories

# Check if DuckDB binary exists
if [ ! -x "$DUCKDB_PATH" ]; then
    echo "DuckDB binary not found at $DUCKDB_PATH. Please ensure DuckDB is built and located correctly."
    exit 1
fi

echo "Scale factors to be used: ${DEFAULT_SFS[*]}"
echo "Frequency change option: $FREQUENCY_CHANGE"

# Iterate over each scale factor
for sf in "${DEFAULT_SFS[@]}"; do
   # Function to process each scale factor
    DATABASE_FILE="${sf}.db"
    echo "Processing Scale Factor: $sf"
    echo "Using Database File: $DATABASE_FILE"

    # Loop through each frequency and thread
    for freq in "${FREQUENCIES_GHZ[@]}"; do
        formatted_freq=$(echo "$freq" | sed 's/^f//; s/GHz//')  # Remove 'f' prefix and 'GHz' suffix
        echo "Processing Frequency: $formatted_freq GHz for Scale Factor: $sf"
        set_cpu_frequency "$freq"

        for thread in "${thread_counts[@]}"; do

            # Define directories for saving iostat and mpstat files
            IOSTAT_DIR="Files/SF${sf}/F${formatted_freq}/T${thread}/Iostat/Iterations"
            CPU_LOAD_DIR="Files/SF${sf}/F${formatted_freq}/T${thread}/CpuLoad/Iterations"
            ILO_POWER_DIR="Files/SF${sf}/F${formatted_freq}/T${thread}/IloPower"
            QUERIES_TIMING_DIR="Files/SF${sf}/F${formatted_freq}/T${thread}/QueriesTiming"
            QUERIES_WARM_UP_DIR="Files/SF${sf}/F${formatted_freq}/T${thread}/WarmUp"
            PERF_BASE_DIR="Files/SF${sf}/F${formatted_freq}/T${thread}/PerfStat"
            PCM_BASE_DIR="Files/SF${sf}/F${formatted_freq}/T${thread}/PCM"

            # Create directories if they don't exist
            mkdir -p "$IOSTAT_DIR"
            mkdir -p "$CPU_LOAD_DIR"
            mkdir -p "$ILO_POWER_DIR"
            mkdir -p "$QUERIES_TIMING_DIR"
            mkdir -p "$QUERIES_WARM_UP_DIR"
            mkdir -p "$PERF_BASE_DIR"
            mkdir -p "$PCM_BASE_DIR"

            python3 iLO_power.py username password URL > "${ILO_POWER_DIR}/ilo_power_f${formatted_freq}_sf${sf}_t${thread}_idle.txt"

            # === Execute Each TPC-H Query in Numerical Order ===
            for query_number in $(seq 1 22); do

                drop_caches

                echo "Executing Query $query_number."

                echo "Waiting 10 minutes to stabilize" >&2
                sleep 600

                # Define prefixes for logging
                query_prefix="${QUERIES_TIMING_DIR}/query_f${formatted_freq}_sf${sf}_t${thread}_q${query_number}"
                query_warm_up_prefix="${QUERIES_WARM_UP_DIR}/query_f${formatted_freq}_sf${sf}_t${thread}_q${query_number}_warm_up"
                ilo_prefix="${ILO_POWER_DIR}/ilo_power_f${formatted_freq}_sf${sf}_t${thread}"         
                iostat_file="${IOSTAT_DIR}/iostat_f${formatted_freq}_sf${sf}_t${thread}_q${query_number}" 
                mpstat_file="${CPU_LOAD_DIR}/cpu_load_f${formatted_freq}_sf${sf}_t${thread}_q${query_number}"

                # Start DuckDB process
                start_duckdb "$sf" "$formatted_freq"

                # === Warm-Up Runs ===
                warmup_runs=1
                echo "Starting $warmup_runs warm-up runs for Query $query_number."

                for ((i=1; i<=warmup_runs; i++)); do
                    echo "Warm-Up Run $i for Query $query_number"

                    # Capture the start time for this run
                    start_time=$(date +%s.%N)
                    start_time_formatted=$(date -u -d "@$start_time" +"%Y-%m-%dT%H:%M:%SZ")

                    # Send query to DuckDB
                    echo "LOAD tpch; PRAGMA threads=$thread; PRAGMA tpch($query_number); SELECT '---END---' as end_signal;" > "cmd_pipe_sf${sf}_f${formatted_freq}"

                    # Read output until '---END---' is found
                    while IFS= read -r line; do
                        echo "$line" >> "${query_warm_up_prefix}.txt"
                        [[ "$line" == *'---END---'* ]] && break
                    done < "out_pipe_sf${sf}_f${formatted_freq}"

                    # Capture the end time for this run
                    end_time=$(date +%s.%N)
                    finish_time_formatted=$(date -u -d "@${end_time}" +"%Y-%m-%dT%H:%M:%SZ")

                    # Calculate the time in seconds with two decimal places
                    real_time=$(echo "scale=2; $end_time - $start_time" | bc)
                    formatted_real_time=$(printf "%.2f" "$real_time")

                    # Log that it's a warm-up run
                    echo "Warm-Up Run $i time: ${formatted_real_time}s" | tee -a "${query_warm_up_prefix}.txt"
                    echo "Iostat monitoring stopped and saved to ${iteration_iostat_file}" >&2
                    echo "Mpstat monitoring stopped and saved to ${iteration_mpstat_file}" >&2 
                    echo "" | tee -a "${query_warm_up_prefix}.txt"
                done

                # === Benchmark Runs ===

                perf stat -e task-clock,context-switches,cpu-migrations,page-faults,cycles,instructions,branches,branch-instructions,branch-misses,bus-cycles,cache-references,cache-misses,cpu-cycles,ref-cycles,LLC-loads,LLC-load-misses,LLC-stores -p "$DUCKDB_PID" -o "$PERF_BASE_DIR/perf_stats_process_f${formatted_freq}_sf${sf}_t${thread}_q${query_number}.txt" &
                PERF_PID=$!

                # Start perf monitoring for overall system (System-wide Level 1)
                echo "Starting perf monitoring (System-wide Level 1)..."
                perf stat -a --topdown --td-level 1 -o "$PERF_BASE_DIR/perf_stats_system_f${formatted_freq}_sf${sf}_t${thread}_q${query_number}.txt" &
                PERF_SYSTEM_PID=$!

                # Check if perf started successfully
                if ! ps -p "$PERF_PID" > /dev/null 2>&1; then
                    echo "Error: perf stat failed to start. Check permissions or kernel settings."
                    cleanup
                    exit 1
                fi

                # Start pcm-memory in the background and redirect output to the text file
                start_pcm_memory "$PCM_BASE_DIR/pcm_f${formatted_freq}_sf${sf}_t${thread}_q${query_number}.txt"

                echo "Starting benchmark runs for Query $query_number to accumulate at least $TARGET_TIME seconds."

                cumulative_time=0
                benchmark_run=1

                # Capture the start time total
                start_time_total=$(date +%s.%N)
                start_time_total_formatted=$(date -u -d "@${start_time_total}" +"%Y-%m-%dT%H:%M:%SZ")

                while (( $(echo "$cumulative_time < $TARGET_TIME" | bc -l) )); do
                    echo "Benchmark Run $benchmark_run for Query $query_number"

                    iteration_iostat_file="${IOSTAT_DIR}/iostat_f${formatted_freq}_sf${sf}_t${thread}_q${query_number}_it${benchmark_run}.txt"
                    iteration_mpstat_file="${CPU_LOAD_DIR}/cpu_load_f${formatted_freq}_sf${sf}_t${thread}_q${query_number}_it${benchmark_run}.txt"

                    iostat 1 > "${iteration_iostat_file}" &
                    IOSTAT_PID=$!
                    mpstat -P ALL 1 > "${iteration_mpstat_file}" &
                    MPSTAT_PID=$!

                    # Capture the start time for this run
                    start_time=$(date +%s.%N)
                    start_time_formatted=$(date -u -d "@$start_time" +"%Y-%m-%dT%H:%M:%SZ")

                    # Send query to DuckDB
                    echo "LOAD tpch; PRAGMA threads=$thread; PRAGMA tpch($query_number); SELECT '---END---' as end_signal;" > "cmd_pipe_sf${sf}_f${formatted_freq}"

                    # Read output until '---END---' is found
                    while IFS= read -r line; do
                        echo "$line" >> "${query_prefix}.txt"
                        [[ "$line" == *'---END---'* ]] && break
                    done < "out_pipe_sf${sf}_f${formatted_freq}"

                    # Capture the end time for this run
                    end_time=$(date +%s.%N)
                    finish_time_formatted=$(date -u -d "@${end_time}" +"%Y-%m-%dT%H:%M:%SZ")

                    # Calculate the time in seconds with two decimal places
                    real_time=$(echo "scale=2; $end_time - $start_time" | bc)
                    formatted_real_time=$(printf "%.2f" "$real_time")

                    # Update cumulative time
                    cumulative_time=$(echo "$cumulative_time + $real_time" | bc)

                    kill $IOSTAT_PID
                    kill $MPSTAT_PID

                    # Log the benchmark run's timing
                    echo "Benchmark Run $benchmark_run time: ${formatted_real_time}s" | tee -a "${query_prefix}.txt"
                    echo "Cumulative Benchmark Time: ${cumulative_time}s" | tee -a "${query_prefix}.txt"
                    echo "Iostat monitoring stopped and saved to ${iteration_iostat_file}" >&2
                    echo "Mpstat monitoring stopped and saved to ${iteration_mpstat_file}" >&2 
                    echo "" | tee -a "${query_prefix}.txt"

                    ((benchmark_run++))
                done

                echo "Reached target cumulative time of $TARGET_TIME seconds for Query $query_number."

                # Capture the end time total
                end_time_total=$(date +%s.%N)
                finish_time_total_formatted=$(date -u -d "@${end_time_total}" +"%Y-%m-%dT%H:%M:%SZ")

                # Calculate the total time in seconds with two decimal places
                total_time=$(echo "scale=2; $end_time_total - $start_time_total" | bc)
                formatted_total_time=$(printf "%.2f" "$total_time")

                # Stop perf monitoring processes with SIGINT to ensure output is written
                echo "Stopping perf monitoring..."
                set +e  # Disable 'exit on error' to ensure cleanup completes even on error

                kill -SIGINT "$PERF_PID" 2>/dev/null || true
                wait "$PERF_PID" 2>/dev/null || true
                echo "Perf stats saved to $PERF_BASE_DIR/perf_stats_process_f${formatted_freq}_sf${sf}_t${thread}_q${query_number}.txt."

                # Stop system-wide perf monitoring
                kill -SIGINT "$PERF_SYSTEM_PID" 2>/dev/null || true
                wait "$PERF_SYSTEM_PID" 2>/dev/null || true
                echo "System-wide perf stats saved to $PERF_BASE_DIR/perf_stats_system_f${formatted_freq}_sf${sf}_t${thread}_q${query_number}.txt."

                set -e  # Re-enable 'exit on error'

                echo "Perf monitoring completed for Q$query_number."

                # Stop pcm-memory
                stop_pcm_memory
                echo "pcm-memory output saved to $PCM_BASE_DIR/pcm_f${formatted_freq}_sf${sf}_t${thread}_q${query_number}.txt."

                echo "Finish Time: $finish_time_total_formatted" >&2

                echo "Total Time: ${formatted_total_time}s" | tee -a "${query_prefix}.txt" >&2
                echo "Completed Query $query_number with $((benchmark_run - 1)) benchmark runs." | tee -a "${query_prefix}.txt"

                # Save start and finish times to a file
                timestamp_file="${ILO_POWER_DIR}/query_timestamps_f${formatted_freq}_sf${sf}_t${thread}.txt"
                echo "Query $query_number: Start Time = $start_time_total_formatted, Finish Time = $finish_time_total_formatted" >> "$timestamp_file"

                # Clean up DuckDB process
                cleanup "$sf" "$formatted_freq"
      
                echo "Running iLO power script for query number: $query_number"
                python3 iLO_power.py username password URL >> "${ilo_prefix}.txt"
                echo "------------------------------------------------------------------------------------" >> "${ilo_prefix}.txt"            
            
            done

        done
    done

done

echo "All scale factors processed successfully."
echo "Zipping the Files directory into Files.zip..."
zip -r "Files.zip" Files
echo "Zipping completed: Files.zip"
