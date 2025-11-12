#!/usr/bin/env python3
"""
DAX to SparkSQL Conversion Prompt Template for LangChain

This module provides a class to access the prompt template used for converting
DAX expressions to SparkSQL expressions for Unity Catalog Metric Views.
"""

class DAXSparkSQLPromptTemplate:
    """
    A class that provides access to the DAX to SparkSQL conversion prompt template.
    
    This class is designed to work with LangChain and provides methods to:
    - Get the raw prompt template text
    - Create a LangChain PromptTemplate object
    - Get formatted prompts with filled placeholders
    """
    
    def __init__(self):
        """Initialize the DAX to SparkSQL prompt template."""
        self._template = """
        
        You are an expert in both DAX (Data Analysis Expressions) and SparkSQL. Your task is to assist in converting DAX expressions into Databricks Unity Catalog Metric View measures, which are defined using SparkSQL syntax.

        ### Objective:

        Convert DAX measures into equivalent SparkSQL expressions that are compatible with Unity Catalog Metric Views. These expressions will be used in Databricks Genie AI/BI (Genie Rooms) as well as in Dashboards, and must strictly follow SparkSQL syntax conventions.

        ### Context:

        Below is a set of reference examples showing DAX expressions alongside their properly converted SparkSQL-based UC Metric View versions. Use these examples to guide your conversions and maintain consistency with established transformation patterns.

        {conversion_examples}

        ### Output Format:

        Please format your response using the structure provided below:

        {response_format}

        This ensures consistent and structured outputs for downstream processing.

        ### Guidelines:

        1. Use the provided examples and your expert knowledge of DAX and SparkSQL for the conversion.
        2. If you encounter an expression you cannot confidently convert, do not guess. Instead:
            2.1. Explain clearly what is ambiguous, unsupported, or missing.
            2.2. Optionally suggest what additional context or assumptions would be needed to proceed.
        3. You may use SparkSQL idioms such as CASE WHEN, FILTER, AGGREGATE, SUM, MAX, DATE_TRUNC, and others where appropriate.
        4. Ensure that the converted expression faithfully reproduces the logic and intent of the original DAX expression.
        5. If a referenced base measure is missing, define it as: measure("BASE MEASURE"). For example, DAX: DIVIDE([Current Sales], [Prior Year Sales], BLANK()) - 1  → SparkSQL: CASE WHEN measure('Prior Year Sales') IS NULL OR measure('Prior Year Sales') = 0 THEN NULL ELSE measure('Current Sales') / measure('Prior Year Sales') - 1 END

        ### Input:

        Below is the list of DAX expressions to be converted:
 
        {input_dax_expressions}
        
        """

        self._input_variables = ["conversion_examples", "response_format", "input_dax_expressions"]
    
    def get_template_text(self) -> str:
        """
        Returns the raw prompt template text.
        
        Returns:
            str: The complete prompt template with placeholders
        """
        return self._template

    def get_input_variables(self) -> list[str]:
        """
        Returns the list of input variables for the prompt template.
        
        Returns:
            list[str]: The list of input variables
        """
        return self._input_variables
    