class AASSparkSQLExamples:
    """
    A class containing DAX to SparkSQL conversion examples for AAS (Azure Analysis Services) migration.
    
    This class provides access to a knowledge base of DAX expressions and their corresponding
    SparkSQL equivalents for Unity Catalog Metric Views.
    """
    
    def __init__(self):
        """Initialize the AAS SparkSQL Examples class."""
        self._content = """
        
        DAX expressions 
        
        [
          {
            "name": "Total Revenue",
            "expression": "SUM('FactSales'[Revenue])"
          },
          {
            "name": "Total Quantity",
            "expression": "SUM('FactSales'[Quantity])"
          },
          {
            "name": "Average Price",
            "expression": "DIVIDE([Total Revenue], [Total Quantity])"
          }
        ]

        
        Unity Catalog Metric View Expressions (SparkSQL)
        
        - name: Total Revenue
          # SUM('FactSales'[Revenue])
          expr: SUM(Revenue)
        - name: Total Quantity
          # SUM('FactSales'[Quantity])
          expr: SUM(Quantity)
        - name: Average Price
          # DIVIDE([Total Revenue], [Total Quantity])
          expr: SUM(Revenue) / NULLIF(SUM(Quantity), 0)
          """

    def get_content(self) -> str:
        """
        Returns the complete content of the AAS SparkSQL examples as text.
        
        Returns:
            str: The full content containing DAX expressions and their SparkSQL equivalents
        """
        return self._content