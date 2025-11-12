from dotenv import load_dotenv

from aas_metrics.read_aas_metrics import read_metrics_to_simple_json
from knowledge_base.aas_sparksql_examples import AASSparkSQLExamples
from prompts.ass_sparksql_conversion import DAXSparkSQLPromptTemplate

from langchain_core.prompts import PromptTemplate
from databricks_langchain import ChatDatabricks
from langchain_core.output_parsers import StrOutputParser
import json
import os
from datetime import datetime
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

# Thread lock for file operations
file_lock = threading.Lock()

def convert_dax_to_sparksql_uc_metric_view(dax_expressions: str) -> str:
    """
    Convert a DAX expression to UC Metric View SparkSQL expression.
    """
    
    knowledge_base = AASSparkSQLExamples()
    conversion_examples = knowledge_base.get_content()
    
    prompt = DAXSparkSQLPromptTemplate()
    prompt_template = prompt.get_template_text()
    
    response_format = """
    - name: [NAME OF THE MEASURE]
          # [ORIGINAL DAX EXPRESSION]
          expr: [SPARKSQL EXPRESSION]
    """

    converter_prompt = PromptTemplate(
        template = prompt_template,
        input_variables = prompt.get_input_variables(),
        partial_variables = {
            "conversion_examples": conversion_examples,
            "response_format": response_format
        }
    )

    llm = ChatDatabricks(
        endpoint="databricks-claude-sonnet-4",
        temperature=0
    )

    chain = converter_prompt | llm | StrOutputParser()

    res = chain.invoke(input={"input_dax_expressions": dax_expressions})

    return res


def save_conversion_results(results: str, output_folder: str = "uc_converted_metrics") -> str:
    """
    Save the conversion results to a text file in the specified folder.
    
    Args:
        results (str): The conversion results to save
        output_folder (str): The folder where to save the file
        
    Returns:
        str: The path to the saved file
    """
    # Generate filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"converted_metrics_{timestamp}.txt"
    filepath = os.path.join(output_folder, filename)
    
    # Save the results
    with open(filepath, 'w', encoding='utf-8') as file:
        file.write(f"# DAX to SparkSQL UC Metric View Conversion Results\n")
        file.write(f"# Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        total_conversions = sum(1 for line in results.splitlines() if 'name:' in line)
        file.write(f"# Total conversions: {total_conversions}\n")
        file.write("\n" + "="*80 + "\n\n")
        file.write(results)
    
    return filepath


def create_batches(metrics_list: list, batch_size: int = 100) -> list:
    """
    Break down a list of metrics into smaller batches.
    
    Args:
        metrics_list (list): List of all metrics
        batch_size (int): Size of each batch (default: 100)
        
    Returns:
        list: List of batches, where each batch is a list of metrics
    """
    batches = []
    for i in range(0, len(metrics_list), batch_size):
        batch = metrics_list[i:i + batch_size]
        batches.append(batch)
    return batches


def process_batches_parallel(metric_batches: list, max_workers: int = 4) -> list:
    """
    Process multiple batches of metrics in parallel using ThreadPoolExecutor.
    
    Args:
        metric_batches (list): List of batches to process
        max_workers (int): Maximum number of concurrent threads (default: 4)
        
    Returns:
        list: List of batch results with timing information
    """
    all_results = []
    total_start_time = time.time()
    
    print(f"\n{'='*60}")
    print(f"STARTING PARALLEL PROCESSING")
    print(f"{'='*60}")
    print(f"Total batches: {len(metric_batches)}")
    print(f"Max concurrent threads: {max_workers}")
    print(f"{'='*60}")
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all batch processing tasks
        future_to_batch = {}
        for i, batch_data in enumerate(metric_batches, 1):
            batch_name = f"Batch {i}"
            future = executor.submit(process_metrics_batch, batch_name, batch_data, i)
            future_to_batch[future] = (i, batch_name, len(batch_data))
        
        # Collect results as they complete
        completed_batches = 0
        for future in as_completed(future_to_batch):
            batch_num, batch_name, batch_size = future_to_batch[future]
            completed_batches += 1
            
            try:
                results, saved_file, count = future.result()
                all_results.append({
                    'batch': batch_name,
                    'batch_num': batch_num,
                    'file': saved_file,
                    'count': count
                })
                
                print(f"\n✅ {batch_name} completed ({completed_batches}/{len(metric_batches)})")
                print(f"   📁 File: {os.path.basename(saved_file)}")
                print(f"   📊 Metrics: {count}")
                
            except Exception as exc:
                print(f"\n❌ {batch_name} generated an exception: {exc}")
                # Still add to results for tracking, but mark as failed
                all_results.append({
                    'batch': batch_name,
                    'batch_num': batch_num,
                    'file': None,
                    'count': 0,
                    'error': str(exc)
                })
    
    total_time = time.time() - total_start_time
    
    print(f"\n{'='*60}")
    print(f"PARALLEL PROCESSING COMPLETED")
    print(f"{'='*60}")
    print(f"Total processing time: {total_time:.2f} seconds")
    print(f"Average time per batch: {total_time/len(metric_batches):.2f} seconds")
    
    # Sort results by batch number for consistent ordering
    all_results.sort(key=lambda x: x['batch_num'])
    
    return all_results


def process_metrics_batch(batch_name: str, metrics_data: list, thread_id: int = None) -> tuple:
    """
    Process a batch of metrics data to SparkSQL conversion.
    Now thread-safe for concurrent execution.
    
    Args:
        batch_name (str): Name of the batch for display purposes
        metrics_data (list): List of metric dictionaries
        thread_id (int): Optional thread identifier for logging
        
    Returns:
        tuple: (results, saved_file_path, metrics_count)
    """
    thread_info = f"[Thread {thread_id}] " if thread_id else ""
    
    print(f"\n{thread_info}{'='*50}")
    print(f"{thread_info}PROCESSING {batch_name.upper()}")
    print(f"{thread_info}{'='*50}")
    
    # Convert metrics data to JSON string
    dax_expressions = json.dumps(metrics_data)
    
    print(f"{thread_info}Converting {len(metrics_data)} DAX expressions...")
    
    start_time = time.time()
    # Convert DAX to SparkSQL
    results = convert_dax_to_sparksql_uc_metric_view(dax_expressions)
    conversion_time = time.time() - start_time
    
    # Save results with thread safety
    with file_lock:
        saved_file = save_conversion_results(results)
    
    print(f"\n{thread_info}{batch_name} conversion completed!")
    print(f"{thread_info}Conversion time: {conversion_time:.2f} seconds")
    print(f"{thread_info}Results saved to: {saved_file}")
    print(f"{thread_info}File size: {os.path.getsize(saved_file)} bytes")
    
    return results, saved_file, len(metrics_data)


def combine_all_results(output_folder: str = "uc_converted_metrics") -> str:
    """
    Combine all text files in the output folder into a single consolidated file.
    
    Args:
        output_folder (str): The folder containing the batch result files
        
    Returns:
        str: Path to the combined file
    """
    import glob
    
    # Get all .txt files in the folder
    pattern = os.path.join(output_folder, "converted_metrics_*.txt")
    result_files = sorted(glob.glob(pattern))
    
    if not result_files:
        print("No result files found to combine.")
        return None
    
    # Create combined filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    combined_filename = f"combined_all_metrics_{timestamp}.txt"
    combined_filepath = os.path.join(output_folder, combined_filename)
    
    print(f"\nCombining {len(result_files)} result files...")
    
    with open(combined_filepath, 'w', encoding='utf-8') as combined_file:
        # Write header
        combined_file.write("# COMBINED DAX to SparkSQL UC Metric View Conversion Results\n")
        combined_file.write(f"# Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        combined_file.write(f"# Combined from {len(result_files)} batch files\n")
        combined_file.write("\n" + "="*100 + "\n\n")
        
        total_conversions = 0
        
        for i, file_path in enumerate(result_files, 1):
            # Add batch separator
            combined_file.write(f"\n{'#'*60}\n")
            combined_file.write(f"# BATCH {i} - Source: {os.path.basename(file_path)}\n")
            combined_file.write(f"{'#'*60}\n\n")
            
            # Read and append file content (skip the header lines)
            with open(file_path, 'r', encoding='utf-8') as batch_file:
                lines = batch_file.readlines()
                
                # Skip header lines (lines starting with # or empty lines at the beginning)
                content_start = 0
                for j, line in enumerate(lines):
                    if line.strip() and not line.startswith('#') and not line.startswith('='):
                        content_start = j
                        break
                
                # Write the actual conversion content
                content = ''.join(lines[content_start:])
                combined_file.write(content)
                
                # Count conversions in this batch
                batch_conversions = len([line for line in lines if 'name:' in line])
                total_conversions += batch_conversions
                
                combined_file.write("\n")
        
        # Write footer
        combined_file.write(f"\n{'='*100}\n")
        combined_file.write(f"# SUMMARY: Total {total_conversions} metrics converted from {len(result_files)} batches\n")
        combined_file.write(f"# Combined file generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        combined_file.write(f"{'='*100}\n")
    
    print(f"Combined file created: {combined_filepath}")
    print(f"Total conversions: {total_conversions}")
    print(f"File size: {os.path.getsize(combined_filepath)} bytes")
    
    return combined_filepath


if __name__ == '__main__':

    load_dotenv()

    # Configuration
    BATCH_SIZE = 5
    MAX_WORKERS = 1  # Number of concurrent threads
    
    print("="*80)
    print("DAX TO SPARKSQL UC METRIC VIEW CONVERTER - MULTI-THREADED")
    print("="*80)
    print(f"Configuration:")
    print(f"  - Batch size: {BATCH_SIZE}")
    print(f"  - Max concurrent threads: {MAX_WORKERS}")
    print("="*80)

    # Load all metrics from the single JSON file
    print("Loading metrics from JSON file...")
    all_metrics = read_metrics_to_simple_json("aas_metrics/metrics.json")
    
    # Break into batches
    metric_batches = create_batches(all_metrics, BATCH_SIZE)
    
    print(f"Loaded {len(all_metrics)} total metrics")
    print(f"Created {len(metric_batches)} batches of {BATCH_SIZE} metrics each")
    print(f"Last batch contains {len(metric_batches[-1])} metrics" if metric_batches else "No batches created")
    
    # Process all batches in parallel
    overall_start = time.time()
    all_results = process_batches_parallel(metric_batches, max_workers=MAX_WORKERS)
    overall_time = time.time() - overall_start
    
    # Print summary with performance metrics
    print(f"\n{'='*60}")
    print("CONVERSION SUMMARY")
    print(f"{'='*60}")
    
    successful_batches = [r for r in all_results if 'error' not in r]
    failed_batches = [r for r in all_results if 'error' in r]
    total_metrics = sum(batch['count'] for batch in successful_batches)
    
    for batch in all_results:
        if 'error' in batch:
            print(f"❌ {batch['batch']}: FAILED - {batch['error']}")
        else:
            print(f"✅ {batch['batch']}: {batch['count']} metrics -> {os.path.basename(batch['file'])}")
    
    print(f"\n📊 PERFORMANCE METRICS:")
    print(f"   Total metrics processed: {total_metrics}")
    print(f"   Successful batches: {len(successful_batches)}/{len(all_results)}")
    print(f"   Total processing time: {overall_time:.2f} seconds")
    if total_metrics > 0:
        print(f"   Average time per metric: {overall_time/total_metrics:.3f} seconds")
    print(f"   Speedup from parallelization: ~{MAX_WORKERS}x (theoretical)")
    
    if failed_batches:
        print(f"\n⚠️  WARNING: {len(failed_batches)} batches failed. Check the errors above.")
    else:
        print(f"\n🎉 All conversions completed successfully!")
    
    # Combine all result files into a single file (only successful ones)
    if successful_batches:
        print(f"\n{'='*60}")
        print("COMBINING RESULTS")
        print(f"{'='*60}")
        combined_file = combine_all_results()
        
        if combined_file:
            print(f"\n📁 All batch files have been combined into: {combined_file}")
            print(f"   Final file size: {os.path.getsize(combined_file)} bytes")
        else:
            print("\n❌ No files were found to combine.")
    else:
        print(f"\n❌ No successful batches to combine.")