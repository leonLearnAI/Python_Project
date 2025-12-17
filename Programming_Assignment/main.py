"""
Main entry point for Traffic Safety Analysis Pipeline
"""

import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).resolve().parent
sys.path.append(str(project_root))

from pymongo import MongoClient
from settings import (
    MONGODB_CONFIG, 
    SCHEMA_FILES,
    COLLECTIONS,
    DATA_FILES,
    PROCESSING_CONFIG,
    ANALYSIS_CONFIG
)
from schema_manager import SchemaManager
from data_analysis import TrafficAnalysisEngine
from data_visualization import TrafficVisualizationEngine
import json
from datetime import datetime
from data_processor import TrafficDataProcessor


def main():
    """Main execution function"""
    
    print("=" * 60)
    print("TRAFFIC SAFETY ANALYSIS PIPELINE")
    print("=" * 60)
    
    # 1. Initialize MongoDB connection
    print("\n1. Initializing MongoDB connection...")
    client = MongoClient(
        MONGODB_CONFIG["uri"],
        serverSelectionTimeoutMS=MONGODB_CONFIG["timeout"],
        maxPoolSize=MONGODB_CONFIG["max_pool_size"]
    )
    
    # Test connection
    client.admin.command('ping')
    print("✓ MongoDB connection established")
    
    # 2. Setup schemas
    print("\n2. Setting up database schemas...")
    schema_manager = SchemaManager(client, MONGODB_CONFIG["database"])
    
    # Setup all schemas
    schema_results = schema_manager.setup_all_schemas(SCHEMA_FILES)
    
    for schema_name, success in schema_results.items():
        status = "✓" if success else "✗"
        print(f"  {status} {schema_name}")
    
    # 3. Initialize data processor
    print("\n3. Initializing data processor...")
    processor = TrafficDataProcessor(
        mongo_uri=MONGODB_CONFIG["uri"],
        db_name=MONGODB_CONFIG["database"]
    )
    
    # 4. Load and process data
    print("\n4. Loading and processing data...")
    processor.load_all_data(
        sample_size=PROCESSING_CONFIG["sample_size"],
        batch_size=PROCESSING_CONFIG["batch_size"]
    )
    
    # 5. Run sample queries
    print("\n5. Running sample queries...")
    processor.run_sample_queries()
        
    # 2. Initialize analysis engine
    print("\n2. Initializing analysis engine...")
    analysis_engine = TrafficAnalysisEngine(client)
    
    # 3. Initialize visualization engine
    print("3. Initializing visualization engine...")
    visualization_engine = TrafficVisualizationEngine()
    
    # 4. Perform analyses
    print("\n4. Performing data analyses...")
    
    # Option 1: Individual analyses
    print("\n   Running individual analyses:")
    
    # Temporal analysis
    print("   - Temporal analysis...")
    temporal_results = analysis_engine.perform_temporal_analysis(days_back=180)
    
    # Weather analysis
    print("   - Weather analysis...")
    weather_results = analysis_engine.perform_weather_analysis(days_back=180)
    
    # Option 2: Comprehensive analysis
    print("\n   Running comprehensive analysis...")
    comprehensive_results = analysis_engine.perform_comprehensive_analysis(days_back=180)
    
    # 5. Save analysis results
    print("\n5. Saving analysis results...")
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Save individual results
    with open(f"reports/analysis_results_temporal_{timestamp}.json", "w") as f:
        json.dump(temporal_results, f, indent=2, default=str)
    
    with open(f"reports/analysis_results_weather_{timestamp}.json", "w") as f:
        json.dump(weather_results, f, indent=2, default=str)
    
    # Save comprehensive results
    with open(f"reports/analysis_results_comprehensive_{timestamp}.json", "w") as f:
        json.dump(comprehensive_results, f, indent=2, default=str)
    
    print("✓ Analysis results saved to JSON files")
    
    # 6. Generate visualizations
    print("\n6. Generating visualizations...")
    
    # Generate visualizations for individual analyses
    print("\n   Generating temporal visualizations...")
    temporal_charts = visualization_engine.generate_temporal_visualizations(temporal_results)
    
    print("   Generating weather visualizations...")
    weather_charts = visualization_engine.generate_weather_visualizations(weather_results)
   
    # Generate comprehensive visualizations
    print("\n   Generating comprehensive visualizations...")
    comprehensive_charts = visualization_engine.generate_comprehensive_visualizations(comprehensive_results)
    
    # 7. Summary
    print("\n" + "="*60)
    print("ANALYSIS AND VISUALIZATION COMPLETE")
    print("="*60)
    
    print(f"\nGenerated Charts:")
    print(f"  • Temporal: {len(temporal_charts)} charts")
    print(f"  • Weather: {len(weather_charts)} charts")
    print(f"  • Comprehensive: {len(comprehensive_charts)} charts")
    print(f"  • TOTAL: {len(temporal_charts) + len(weather_charts) + len(comprehensive_charts)} charts")
    
    print(f"\nFiles saved in:")
    print(f"  • Analysis results: reports/")
    print(f"  • Visualizations: reports/visualizations/")
    
    print("\n" + "="*60)


if __name__ == "__main__":
    main()
