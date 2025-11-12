#!/usr/bin/env python3
"""
Metrics JSON Reader

This module provides functions to read the metrics.json file and convert it to
different formats: string format "Name = Expression" and simplified JSON format
"""

import json
from typing import List, Union, Dict


def read_metrics_to_simple_json(file_path) -> List[Dict[str, str]]:
    """
    Reads the metrics.json file and returns a simplified JSON format with only name and expression.
    
    Args:
        file_path (str): Path to the metrics.json file
        
    Returns:
        List[Dict[str, str]]: List of dictionaries with 'name' and 'expression' keys
        
    Example:
        >>> metrics = read_metrics_to_simple_json()
        >>> print(json.dumps(metrics[:2], indent=2))
        [
          {
            "name": "Total Orders",
            "expression": "SUM('FactSales'[OrderCount])"
          },
          {
            "name": "Total Amount",
            "expression": "SUM('FactSales'[Amount])"
          }
        ]
    """
    try:
        # Read the JSON file
        with open(file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
        
        # Extract measures from the JSON
        measures = data.get('measures', [])
        
        # Create simplified metrics list
        simplified_metrics = []
        for measure in measures:
            name = measure.get('name', '')
            expression = measure.get('expression', '')
            
            # Handle expression formatting
            formatted_expression = _format_expression(expression)
            
            if name and formatted_expression:
                simplified_metrics.append({
                    "name": name,
                    "expression": formatted_expression
                })
        
        return simplified_metrics
    
    except FileNotFoundError:
        raise FileNotFoundError(f"Metrics file not found: {file_path}")
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON format in {file_path}: {e}")
    except Exception as e:
        raise Exception(f"Error reading metrics file: {e}")

def _format_expression(expression: Union[str, List[str]]) -> str:
    """
    Formats the expression field, handling both string and array formats.
    
    Args:
        expression: The expression field from the JSON (can be string or list)
        
    Returns:
        str: Formatted expression string
    """
    if isinstance(expression, str):
        # Simple string expression, just clean it up
        return expression.strip()
    elif isinstance(expression, list):
        # Array of strings, join them together
        # Remove empty strings and clean up whitespace
        clean_parts = [part.strip() for part in expression if part.strip()]
        return ' '.join(clean_parts)
    else:
        return str(expression) if expression is not None else ""

# Example usage and testing
if __name__ == "__main__":
    try:
        
        # Example 2: Read metrics as simplified JSON
        print("=== Simplified JSON Format (First 3 metrics) ===")
        metrics_json = read_metrics_to_simple_json()
        print(json.dumps(metrics_json[:3], indent=2, ensure_ascii=False))
        print(f"... and {len(metrics_json) - 3} more metrics\n")

    
    except Exception as e:
        print(f"Error: {e}")