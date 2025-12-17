"""
Traffic Data Processor with schema validation
"""

import pandas as pd
import numpy as np
import json
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from pathlib import Path
import logging
from pymongo import MongoClient

from settings import DATA_FILES, PROCESSING_CONFIG, SCHEMA_FILES
from schema_manager import SchemaManager

logger = logging.getLogger(__name__)

class TrafficDataProcessor:
    """Processes traffic and crash data with schema validation"""
    
    def __init__(self, mongo_uri: str, db_name: str):
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]
        self.schema_manager = SchemaManager(self.client, db_name)
        
        # Load schemas
        self.schemas = {}
        for name, file_path in SCHEMA_FILES.items():
            try:
                self.schemas[name] = self.schema_manager.load_schema(file_path)
            except Exception as e:
                logger.warning(f"Could not load schema {name}: {e}")

    def clear_collections(self):
        """Clear all documents from a collection"""
        try:
            # Clear existing collections
            for coll in ['crash_reports', 'traffic_flow', 'incidents', 'analytics']:
                if coll in self.db.list_collection_names():
                    result = self.db[coll].delete_many({})
                    print(f"✓ Cleared {result.deleted_count} documents from {coll}")
        except Exception as e:
            logger.error(f"Failed to clear collection {collection_name}: {e}")

    def load_all_data(self, sample_size: Optional[int] = None, batch_size: int = 1000):
        """Load all data sources with schema validation"""
        return 
    
        # 0. Clear existing data
        self.clear_collections()

        # 1. Load crash data
        print("\n" + "="*50)
        print("PROCESSING CRASH REPORTING DATA")
        print("="*50)

        self._load_crash_data(sample_size)
        
        # 2. Load traffic flow data
        print("\n" + "="*50)
        print("PROCESSING TRAFFIC FLOW DATA")
        print("="*50)
        
        self._load_traffic_flow_data(sample_size)
        
        # 3. Load incidents data
        print("\n" + "="*50)
        print("PROCESSING INCIDENTS DATA")
        print("="*50)
        
        self._load_incidents_data(sample_size)
    
    def _load_crash_data(self, sample_size: Optional[int] = None):
        """Load and process crash reporting data"""
        try:
            # Read CSV
            df = pd.read_csv(DATA_FILES["crash_reports"], low_memory=False)
            
            # Sample if specified
            if sample_size and sample_size < len(df):
                df = df.head(sample_size)
            
            # Clean and transform
            cleaned_df = self.clean_crash_data(df)
            documents = self.transform_crash_data(cleaned_df)
            
            # Insert to db
            self.db.crash_reports.insert_many(documents)            
            print(f"✓ Crash reports: {len(documents)} documents inserted")
        except Exception as e:
            logger.error(f"Failed to load crash data: {e}")
    
    def clean_crash_data(self, df):
        """Clean and transform crash reporting data"""
        print(f"Original crash data shape: {df.shape}")
        
        # Make a copy
        df = df.copy()
        
        # Handle missing values
        df = df.replace(['', ' ', 'Unknown', 'UNKNOWN', 'unknown', None, 'N/A', 'null', 'NULL'], np.nan)
        
        # Parse date time
        df['Crash Date/Time'] = pd.to_datetime(
            df['Crash Date/Time'], 
            format='%m/%d/%Y %I:%M:%S %p',
            errors='coerce'
        )
        
        # Extract date components
        df['crash_year'] = df['Crash Date/Time'].dt.year
        df['crash_month'] = df['Crash Date/Time'].dt.month
        df['crash_hour'] = df['Crash Date/Time'].dt.hour
        df['crash_weekday'] = df['Crash Date/Time'].dt.day_name()
        
        # Parse coordinates - handle different formats
        def extract_coordinates(row):
            """Extract coordinates from various possible sources in the row"""
            sources = []
            
            # Source 1: Latitude and Longitude columns
            try:
                lat = row.get('Latitude')
                lon = row.get('Longitude')
                if pd.notna(lat) and pd.notna(lon):
                    sources.append([float(lon), float(lat)])
            except:
                pass
            
            # Source 2: Location column (string format)
            loc = row.get('Location')
            if pd.notna(loc):
                loc_str = str(loc)
                # Try to extract coordinates from string like "(39.10533874, -76.98984545)"
                try:
                    # Remove parentheses and split
                    clean_str = loc_str.strip('() ')
                    parts = clean_str.split(',')
                    if len(parts) >= 2:
                        lat = float(parts[0].strip())
                        lon = float(parts[1].strip())
                        sources.append([lon, lat])
                except:
                    # Try regex
                    try:
                        coords = re.findall(r"[-+]?\d+\.\d+", loc_str)
                        if len(coords) >= 2:
                            sources.append([float(coords[1]), float(coords[0])])
                    except:
                        pass
            
            # Return first valid source or None
            return sources[0] if sources else None
        
        # Apply coordinate extraction
        coordinates = []
        for idx, row in df.iterrows():
            coords = extract_coordinates(row)
            coordinates.append(coords)
        
        df['coordinates'] = coordinates
        
        # Clean numeric columns
        if 'Speed Limit' in df.columns:
            df['Speed Limit'] = pd.to_numeric(df['Speed Limit'], errors='coerce')
        
        if 'Vehicle Year' in df.columns:
            df['Vehicle Year'] = pd.to_numeric(df['Vehicle Year'], errors='coerce')
        
        # Clean categorical columns
        categorical_cols = ['Weather', 'Surface Condition', 'Light', 'Traffic Control']
        for col in categorical_cols:
            if col in df.columns:
                df[col] = df[col].fillna('UNKNOWN').astype(str).str.upper()
        
        print(f"Cleaned crash data shape: {df.shape}")
        print(f"Rows with coordinates: {sum(1 for x in df['coordinates'] if x is not None)}")
        
        return df

    def transform_crash_data(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Transform crash data to match schema"""
        documents = []
        
        for _, row in df.iterrows():
            # Build document according to schema
            doc = {
                'report_number': row.get('Report Number'),
                'local_case_number': row.get('Local Case Number'),
                'agency_name': row.get('Agency Name'),
                'acrs_report_type': row.get('ACRS Report Type'),
                'crash_datetime': row.get('Crash Date/Time'),
                'route_info': {
                    'route_type': row.get('Route Type'),
                    'road_name': row.get('Road Name'),
                    'cross_street': row.get('Cross-Street Name'),
                    'municipality': row.get('Municipality')
                },
                'environmental_conditions': {
                    'weather': row.get('Weather'),
                    'surface_condition': row.get('Surface Condition'),
                    'light_condition': row.get('Light'),
                    'traffic_control': row.get('Traffic Control')
                },
                'collision_details': {
                    'collision_type': row.get('Collision Type'),
                    'driver_at_fault': row.get('Driver At Fault'),
                    'injury_severity': row.get('Injury Severity')
                },
                'vehicle_info': [{
                    'vehicle_id': row.get('Vehicle ID'),
                    'damage_extent': row.get('Vehicle Damage Extent'),
                    'body_type': row.get('Vehicle Body Type'),
                    'vehicle_make': row.get('Vehicle Make'),
                    'vehicle_model': row.get('Vehicle Model'),
                    'vehicle_year': row.get('Vehicle Year'),
                    'speed_limit': row.get('Speed Limit')
                }] if pd.notna(row.get('Vehicle ID')) else [],
                'substance_abuse': {
                    'driver_substance_abuse': row.get('Driver Substance Abuse'),
                    'non_motorist_substance_abuse': row.get('Non-Motorist Substance Abuse')
                },
                'location': {
                    'type': 'Point',
                    'coordinates': row.get('coordinates')
                } if row.get('coordinates') else None,
                'metadata': {
                    'created_date': datetime.now(),
                    'last_updated': datetime.now(),
                    'data_source': 'Crash_Reporting_Drivers_Data.csv'
                }
            }
            documents.append(doc)
        
        return documents

    def _load_traffic_flow_data(self, sample_size: Optional[int] = None):
        """Load and process traffic_flow reporting data"""
        try:
            # Read CSV
            df = pd.read_csv(DATA_FILES["traffic_flow"], low_memory=False)
            
            # Sample if specified
            if sample_size and sample_size < len(df):
                df = df.head(sample_size)
            
            # Clean and transform
            cleaned_df = self.clean_traffic_flow_data(df)
            documents = self.transform_traffic_flow_data(cleaned_df)
            # Insert to db
            self.db.traffic_flow_reports.insert_many(documents)            
            print(f"✓ Traffic_flow reports: {len(documents)} documents inserted")
        except Exception as e:
            logger.error(f"Failed to load traffic_flow data: {e}")
                        
    def clean_traffic_flow_data(self, df):
        """Clean and transform traffic flow data"""
        print(f"Original traffic flow shape: {df.shape}")
        
        # Clean column names
        df.columns = [col.strip().lower() for col in df.columns]
        
        # Parse date
        def parse_date(date_val):
            if pd.isna(date_val):
                return pd.NaT
            date_str = str(date_val)
            for fmt in ('%m/%d/%Y', '%d/%m/%Y', '%Y-%m-%d', '%Y/%m/%d'):
                try:
                    return pd.to_datetime(date_str, format=fmt)
                except:
                    continue
            return pd.NaT
        
        df['date'] = df['date'].apply(parse_date)
        
        # Parse start_time
        df['start_time'] = pd.to_datetime(df['start_time'], errors='coerce')
        
        # Parse end_time (might be just time like "09:45")
        def parse_end_time(row):
            start = row.get('start_time')
            end_str = row.get('end_time')
            
            if pd.isna(start) or pd.isna(end_str):
                return pd.NaT
            
            try:
                end_str = str(end_str)
                # If it's just time (HH:MM)
                if ':' in end_str and len(end_str.split(':')) == 2:
                    time_parts = end_str.split(':')
                    if len(time_parts) == 2:
                        hour = int(time_parts[0])
                        minute = int(time_parts[1])
                        return datetime.combine(
                            start.date(), 
                            datetime.min.time().replace(hour=hour, minute=minute)
                        )
            except:
                pass
            
            # Try default parsing
            return pd.to_datetime(end_str, errors='coerce')
        
        df['end_time'] = df.apply(parse_end_time, axis=1)
        
        # Fill missing end times
        mask = df['end_time'].isna() & df['start_time'].notna()
        df.loc[mask, 'end_time'] = df.loc[mask, 'start_time'] + timedelta(minutes=15)
        
        # Clean numeric columns
        numeric_cols = ['flow', 'flow_pc', 'cong', 'cong_pc', 'dsat', 'dsat_pc']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Extract hour
        df['hour'] = df['start_time'].dt.hour
        
        print(f"Cleaned traffic flow shape: {df.shape}")
        return df

    def transform_traffic_flow_data(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Transform crash data to match schema"""
        documents = []

        for _, row in df.iterrows():
            doc = {
                'site': row.get('site'),
                'date': row.get('date'),
                'day_of_week': row.get('day'),
                'time_slot': {
                    'start_time': row.get('start_time'),
                    'end_time': row.get('end_time'),
                    'duration_minutes': row.get('duration_minutes')
                },
                'flow_metrics': {
                    'flow': row.get('flow'),
                    'flow_percent': row.get('flow_pc'),
                    'congestion': row.get('cong'),
                    'congestion_percent': row.get('cong_pc'),
                    'dsat': row.get('dsat'),
                    'dsat_percent': row.get('dsat_pc')
                },
                'metadata': {
                    'data_source': 'Traffic_Flow_Data_Jan_to_June_2023_SDCC.csv',
                    'processing_date': datetime.now()
                }
            }
            documents.append(doc)        

        return documents

    def _load_incidents_data(self, sample_size: Optional[int] = None):
        """Load and process incidents reporting data"""
        try:
            # Read CSV
            with open(DATA_FILES["incidents"], 'r') as f:
                df = json.load(f)
            
            # Sample if specified
            if sample_size and sample_size < len(df):
                df = df.head(sample_size)
            
            # Clean and transform
            cleaned_df = self.clean_incidents_data(df)
            documents = self.transform_incidents_data(cleaned_df)
            # Insert to db
            self.db.incidents_reports.insert_many(documents)            
            print(f"✓ Incidents reports: {len(documents)} documents inserted")
        except Exception as e:
            logger.error(f"Failed to load incidents data: {e}")
                    
    def clean_incidents_data(self, data_list):
        """Clean and transform incidents JSON data"""
        df = pd.DataFrame(data_list)
        print(f"Original incidents shape: {df.shape}")
        
        # Clean coordinates
        if 'lon2' in df.columns:
            df['longitude'] = pd.to_numeric(df['lon2'], errors='coerce')
        if 'lat2' in df.columns:
            df['latitude'] = pd.to_numeric(df['lat2'], errors='coerce')
        
        # Parse dates
        date_cols = ['crash_date', 'ta_date']
        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        
        # Clean categorical columns
        cat_cols = ['weather', 'lightcond', 'trafcontrl']
        for col in cat_cols:
            if col in df.columns:
                df[col] = df[col].fillna('UNKNOWN').astype(str).str.upper()
        
        print(f"Cleaned incidents shape: {df.shape}")
        return df
    
    def transform_incidents_data(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Transform crash data to match schema"""
        documents = []     

        for _, row in df.iterrows():
            doc = {
                'tamainid': row.get('tamainid'),
                'location': {
                    'description': row.get('location_description'),
                    'coordinates': {
                        'longitude': row.get('lon2'),
                        'latitude': row.get('lat2')
                    } if pd.notna(row.get('lon2')) and pd.notna(row.get('lat2')) else None
                },
                'road_characteristics': {
                    'features': row.get('rdfeature'),
                    'characteristics': row.get('rdcharacter', []),
                    'class': row.get('rdclass', []),
                    'configuration': row.get('rdconfigur', []),
                    'surface': row.get('rdsurface'),
                    'condition': row.get('rdcondition')
                },
                'environmental_factors': {
                    'light_condition': row.get('lightcond'),
                    'weather': row.get('weather'),
                    'traffic_control': row.get('trafcontrl')
                },
                'crash_details': {
                    'crash_date': row.get('crash_date'),
                    'ta_date': row.get('ta_date'),
                    'ta_time': row.get('ta_time'),
                    'fatality': row.get('fatality'),
                    'possible_injuries': row.get('possblinj'),
                    'fatalities_flag': row.get('fatalities'),
                    'injuries_flag': row.get('injuries')
                },
                'vehicles': row.get('vehicles_consolidated', []),
                'year': row.get('year'),
                'month': row.get('month')
            }
            documents.append(doc)
                       
        return documents
            
    def _print_loading_summary(self, results: Dict[str, Dict[str, Any]]):
        """Print loading summary"""
        print("\n" + "="*60)
        print("DATA LOADING SUMMARY")
        print("="*60)
        
        for data_source, result in results.items():
            if "inserted" in result:
                print(f"{data_source}: {result['inserted']:,} documents")
            elif "error" in result:
                print(f"{data_source}: ERROR - {result['error'][:100]}")
        
        # Get final counts from database
        print("\nFinal database counts:")
        for coll_name in ["crash_reports", "traffic_flow", "incidents"]:
            try:
                count = self.db[coll_name].count_documents({})
                print(f"  {coll_name}: {count:,}")
            except:
                print(f"  {coll_name}: Collection not found")
                
    def run_sample_queries(self):
        """Run sample queries to demonstrate data access"""
        print("\n" + "="*60)
        print("SAMPLE QUERIES")
        print("="*60)
        
        # Query 1: Count crashes by injury severity
        print("\n1. Crashes by Injury Severity:")
        pipeline1 = [
            {"$group": {
                "_id": "$collision_details.injury_severity",
                "count": {"$sum": 1}
            }},
            {"$sort": {"count": -1}}
        ]
        
        results1 = list(self.db.crash_reports.aggregate(pipeline1))
        for result in results1[:5]:
            print(f"  {result['_id']}: {result['count']}")
        
        # Query 2: Crashes by hour of day
        print("\n2. Crashes by Hour of Day:")
        pipeline2 = [
            {"$match": {"temporal_features.hour": {"$ne": None}}},
            {"$group": {
                "_id": "$temporal_features.hour",
                "count": {"$sum": 1}
            }},
            {"$sort": {"_id": 1}}
        ]
        
        results2 = list(self.db.crash_reports.aggregate(pipeline2))
        for result in results2:
            print(f"  {result['_id']:02d}:00 - {result['count']}")
        
        # Query 3: Average traffic flow by hour
        print("\n3. Average Traffic Flow by Hour:")
        pipeline3 = [
            {"$match": {"flow_metrics.flow": {"$gt": 0}}},
            {"$group": {
                "_id": "$hour",
                "avg_flow": {"$avg": "$flow_metrics.flow"},
                "records": {"$sum": 1}
            }},
            {"$sort": {"_id": 1}}
        ]
        
        results3 = list(self.db.traffic_flow.aggregate(pipeline3))
        for result in results3[:6]:
            print(f"  Hour {result['_id']}: {result['avg_flow']:.1f} vehicles")
        
        # Query 4: Find crashes on specific date
        print("\n4. Crashes on 2024-04-21:")
        crashes = list(self.db.crash_reports.find({
            "crash_datetime": {
                "$gte": datetime(2024, 4, 21),
                "$lt": datetime(2024, 4, 22)
            }
        }).limit(3))
        
        for crash in crashes:
            time = crash.get('crash_datetime', '')
            severity = crash.get('collision_details', {}).get('injury_severity', 'Unknown')
            print(f"  {crash.get('report_number')} at {time} - {severity}")
        
        # Query 5: Crashes by weather condition
        print("\n5. Crashes by Weather Condition:")
        pipeline5 = [
            {"$group": {
                "_id": "$environmental_conditions.weather",
                "count": {"$sum": 1}
            }},
            {"$sort": {"count": -1}},
            {"$limit": 5}
        ]
        
        results5 = list(self.db.crash_reports.aggregate(pipeline5))
        for result in results5:
            print(f"  {result['_id']}: {result['count']}")
 