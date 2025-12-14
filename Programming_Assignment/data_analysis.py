"""
Traffic Data Analysis Engine
Independent class for performing various analyses on MongoDB data
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
import logging
from scipy import stats

logger = logging.getLogger(__name__)

class TrafficAnalysisEngine:
    """Engine for performing traffic data analysis"""
    
    def __init__(self, mongo_client, db_name: str = "traffic_safety_db"):
        self.client = mongo_client
        self.db = self.client[db_name]
    
    def perform_temporal_analysis(self, days_back: int = 365) -> Dict[str, Any]:
        """
        Analyze temporal patterns in crash data
        
        Args:
            days_back: Number of days to look back
            
        Returns:
            Dictionary with temporal analysis results
        """
        print("Performing temporal analysis...")
        
        try:
            # Calculate date range
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days_back)
            
            # Query crash data
            pipeline = [
                {
                    "$match": {
                        "crash_datetime": {
                            "$gte": start_date,
                            "$lte": end_date
                        }
                    }
                },
                {
                    "$project": {
                        "hour": {"$hour": "$crash_datetime"},
                        "day_of_week": {"$dayOfWeek": "$crash_datetime"},
                        "month": {"$month": "$crash_datetime"},
                        "year": {"$year": "$crash_datetime"},
                        "injury_severity": "$collision_details.injury_severity",
                        "weather": "$environmental_conditions.weather"
                    }
                }
            ]
            
            cursor = self.db.crash_reports.aggregate(pipeline)
            data = list(cursor)
            
            if not data:
                return {"error": "No data found for temporal analysis"}
            
            df = pd.DataFrame(data)
            
            # Calculate metrics
            results = {
                "analysis_type": "temporal_analysis",
                "time_period": {
                    "start_date": start_date.isoformat(),
                    "end_date": end_date.isoformat(),
                    "days": days_back
                },
                "total_crashes": len(df),
                "hourly_distribution": self._calculate_hourly_distribution(df),
                "daily_distribution": self._calculate_daily_distribution(df),
                "monthly_distribution": self._calculate_monthly_distribution(df),
                "peak_hours": self._identify_peak_hours(df),
                "weekend_vs_weekday": self._calculate_weekend_weekday_ratio(df),
                "seasonal_patterns": self._identify_seasonal_patterns(df),
                "temporal_correlations": self._calculate_temporal_correlations(df)
            }
            
            print(f"✓ Temporal analysis completed: {len(df)} crashes analyzed")
            return results
            
        except Exception as e:
            logger.error(f"Error in temporal analysis: {e}")
            return {"error": str(e)}
    
    def _calculate_hourly_distribution(self, df: pd.DataFrame) -> Dict[int, int]:
        """Calculate crashes by hour"""
        if 'hour' not in df.columns:
            return {}
        
        hourly_counts = df['hour'].value_counts().sort_index()
        return {int(hour): int(count) for hour, count in hourly_counts.items()}
    
    def _calculate_daily_distribution(self, df: pd.DataFrame) -> Dict[str, int]:
        """Calculate crashes by day of week"""
        if 'day_of_week' not in df.columns:
            return {}
        
        # MongoDB dayOfWeek: 1=Sunday, 7=Saturday
        day_map = {
            1: "Sunday", 2: "Monday", 3: "Tuesday", 4: "Wednesday",
            5: "Thursday", 6: "Friday", 7: "Saturday"
        }
        
        daily_counts = df['day_of_week'].value_counts().sort_index()
        return {day_map.get(day, f"Day{day}"): int(count) 
                for day, count in daily_counts.items()}
    
    def _calculate_monthly_distribution(self, df: pd.DataFrame) -> Dict[str, int]:
        """Calculate crashes by month"""
        if 'month' not in df.columns:
            return {}
        
        month_map = {
            1: "January", 2: "February", 3: "March", 4: "April",
            5: "May", 6: "June", 7: "July", 8: "August",
            9: "September", 10: "October", 11: "November", 12: "December"
        }
        
        monthly_counts = df['month'].value_counts().sort_index()
        return {month_map.get(month, f"Month{month}"): int(count) 
                for month, count in monthly_counts.items()}
    
    def _identify_peak_hours(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Identify peak crash hours"""
        if 'hour' not in df.columns:
            return {}
        
        hourly_counts = df['hour'].value_counts()
        peak_hour = hourly_counts.idxmax()
        peak_count = hourly_counts.max()
        
        # Identify rush hours (7-9 AM, 4-6 PM)
        morning_rush = df[(df['hour'] >= 7) & (df['hour'] <= 9)].shape[0]
        evening_rush = df[(df['hour'] >= 16) & (df['hour'] <= 18)].shape[0]
        
        return {
            "peak_hour": int(peak_hour),
            "peak_count": int(peak_count),
            "morning_rush_crashes": int(morning_rush),
            "evening_rush_crashes": int(evening_rush),
            "rush_hour_percentage": float((morning_rush + evening_rush) / len(df) * 100)
        }
    
    def _calculate_weekend_weekday_ratio(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Calculate weekend vs weekday crash ratio"""
        if 'day_of_week' not in df.columns:
            return {}
        
        # Weekend days: 1 (Sunday) and 7 (Saturday)
        weekend_mask = df['day_of_week'].isin([1, 7])
        weekend_crashes = df[weekend_mask].shape[0]
        weekday_crashes = df[~weekend_mask].shape[0]
        
        total = len(df)
        if total > 0:
            weekend_ratio = weekend_crashes / total
            weekday_ratio = weekday_crashes / total
        else:
            weekend_ratio = weekday_ratio = 0
        
        return {
            "weekend_crashes": int(weekend_crashes),
            "weekday_crashes": int(weekday_crashes),
            "weekend_ratio": float(weekend_ratio),
            "weekday_ratio": float(weekday_ratio),
            "weekend_to_weekday_ratio": float(weekend_crashes / weekday_crashes if weekday_crashes > 0 else 0)
        }
    
    def _identify_seasonal_patterns(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Identify seasonal patterns in crashes"""
        if 'month' not in df.columns:
            return {}
        
        # Define seasons
        season_map = {
            12: "Winter", 1: "Winter", 2: "Winter",
            3: "Spring", 4: "Spring", 5: "Spring",
            6: "Summer", 7: "Summer", 8: "Summer",
            9: "Fall", 10: "Fall", 11: "Fall"
        }
        
        df['season'] = df['month'].map(season_map)
        seasonal_counts = df['season'].value_counts()
        
        return {
            "seasonal_distribution": {season: int(count) 
                                     for season, count in seasonal_counts.items()},
            "peak_season": seasonal_counts.idxmax() if not seasonal_counts.empty else None,
            "seasonal_variation": float(seasonal_counts.std() / seasonal_counts.mean() 
                                       if seasonal_counts.mean() > 0 else 0)
        }
    
    def _calculate_temporal_correlations(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Calculate temporal correlations"""
        correlations = {}
        
        try:
            # Hour vs severity (if severity data available)
            if 'hour' in df.columns and 'injury_severity' in df.columns:
                # Create severity score (higher = more severe)
                severity_map = {
                    'No Apparent Injury': 1,
                    'Possible Injury': 2,
                    'Suspected Minor Injury': 3,
                    'Suspected Serious Injury': 4,
                    'Fatal Injury': 5
                }
                
                df['severity_score'] = df['injury_severity'].map(
                    lambda x: severity_map.get(x, 1) if pd.notna(x) else 1
                )
                
                hour_severity_corr = df[['hour', 'severity_score']].corr().iloc[0, 1]
                correlations['hour_severity_correlation'] = float(hour_severity_corr)
        except:
            pass
        
        return correlations
    
    def perform_weather_analysis(self, days_back: int = 365) -> Dict[str, Any]:
        """
        Analyze weather impact on crashes
        
        Args:
            days_back: Number of days to look back
            
        Returns:
            Dictionary with weather analysis results
        """
        print("Performing weather analysis...")
        
        try:
            # Calculate date range
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days_back)
            
            # Query crash data with weather
            pipeline = [
                {
                    "$match": {
                        "crash_datetime": {
                            "$gte": start_date,
                            "$lte": end_date
                        },
                        "environmental_conditions.weather": {"$ne": None}
                    }
                },
                {
                    "$project": {
                        "weather": "$environmental_conditions.weather",
                        "injury_severity": "$collision_details.injury_severity",
                        "collision_type": "$collision_details.collision_type",
                        "hour": {"$hour": "$crash_datetime"}
                    }
                }
            ]
            
            cursor = self.db.crash_reports.aggregate(pipeline)
            data = list(cursor)
            
            if not data:
                return {"error": "No data found for weather analysis"}
            
            df = pd.DataFrame(data)
            
            # Clean weather categories
            df['weather_clean'] = df['weather'].str.upper().str.strip()
            
            # Group similar weather conditions
            weather_groups = {
                'CLEAR': ['CLEAR', 'SUNNY', 'FAIR'],
                'CLOUDY': ['CLOUDY', 'OVERCAST', 'PARTLY CLOUDY'],
                'RAIN': ['RAIN', 'RAINING', 'DRIZZLE', 'SHOWER'],
                'SNOW': ['SNOW', 'SNOWING', 'SLEET', 'ICE'],
                'FOG': ['FOG', 'FOGGY', 'MIST', 'HAZE'],
                'WIND': ['WIND', 'WINDY', 'GUSTY']
            }
            
            def map_weather(weather_str):
                if pd.isna(weather_str):
                    return 'UNKNOWN'
                for group, keywords in weather_groups.items():
                    if any(keyword in weather_str for keyword in keywords):
                        return group
                return weather_str
            
            df['weather_group'] = df['weather_clean'].apply(map_weather)
            
            # Calculate metrics
            results = {
                "analysis_type": "weather_analysis",
                "time_period": {
                    "start_date": start_date.isoformat(),
                    "end_date": end_date.isoformat()
                },
                "total_crashes_analyzed": len(df),
                "weather_distribution": self._calculate_weather_distribution(df),
                "weather_severity_analysis": self._analyze_weather_severity(df),
                "weather_temporal_patterns": self._analyze_weather_temporal(df),
                "clear_vs_adverse": self._compare_clear_adverse_weather(df),
                "weather_risk_factors": self._calculate_weather_risk_factors(df)
            }
            
            print(f"✓ Weather analysis completed: {len(df)} crashes analyzed")
            return results
            
        except Exception as e:
            logger.error(f"Error in weather analysis: {e}")
            return {"error": str(e)}
    
    def _calculate_weather_distribution(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Calculate crash distribution by weather"""
        if 'weather_group' not in df.columns:
            return {}
        
        weather_counts = df['weather_group'].value_counts()
        total = len(df)
        
        distribution = {}
        for weather, count in weather_counts.items():
            percentage = count / total * 100 if total > 0 else 0
            distribution[weather] = {
                "count": int(count),
                "percentage": float(percentage)
            }
        
        return distribution
    
    def _analyze_weather_severity(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Analyze injury severity by weather"""
        if 'weather_group' not in df.columns or 'injury_severity' not in df.columns:
            return {}
        
        # Create severity score
        severity_map = {
            'No Apparent Injury': 1,
            'Possible Injury': 2,
            'Suspected Minor Injury': 3,
            'Suspected Serious Injury': 4,
            'Fatal Injury': 5
        }
        
        df['severity_score'] = df['injury_severity'].map(
            lambda x: severity_map.get(x, 1) if pd.notna(x) else 1
        )
        
        # Calculate average severity by weather
        severity_by_weather = df.groupby('weather_group')['severity_score'].agg(['mean', 'std', 'count'])
        
        results = {}
        for weather, row in severity_by_weather.iterrows():
            results[weather] = {
                "average_severity": float(row['mean']),
                "severity_std": float(row['std']),
                "crash_count": int(row['count'])
            }
        
        return results
    
    def _analyze_weather_temporal(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Analyze temporal patterns by weather"""
        if 'weather_group' not in df.columns or 'hour' not in df.columns:
            return {}
        
        results = {}
        weather_groups = df['weather_group'].unique()
        
        for weather in weather_groups:
            weather_df = df[df['weather_group'] == weather]
            hourly_counts = weather_df['hour'].value_counts().sort_index()
            
            if not hourly_counts.empty:
                results[weather] = {
                    "peak_hour": int(hourly_counts.idxmax()),
                    "hourly_distribution": {int(hour): int(count) 
                                          for hour, count in hourly_counts.items()}
                }
        
        return results
    
    def _compare_clear_adverse_weather(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Compare clear vs adverse weather conditions"""
        if 'weather_group' not in df.columns:
            return {}
        
        clear_conditions = ['CLEAR']
        adverse_conditions = ['RAIN', 'SNOW', 'FOG', 'WIND']
        
        clear_df = df[df['weather_group'].isin(clear_conditions)]
        adverse_df = df[df['weather_group'].isin(adverse_conditions)]
        
        total = len(df)
        
        return {
            "clear_weather_crashes": len(clear_df),
            "adverse_weather_crashes": len(adverse_df),
            "clear_percentage": len(clear_df) / total * 100 if total > 0 else 0,
            "adverse_percentage": len(adverse_df) / total * 100 if total > 0 else 0,
            "clear_adverse_ratio": len(clear_df) / len(adverse_df) if len(adverse_df) > 0 else 0
        }
    
    def _calculate_weather_risk_factors(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Calculate risk factors for different weather conditions"""
        if 'weather_group' not in df.columns:
            return {}
        
        # Calculate relative risk compared to clear weather
        weather_counts = df['weather_group'].value_counts()
        clear_count = weather_counts.get('CLEAR', 0)
        
        risk_factors = {}
        for weather, count in weather_counts.items():
            if weather == 'CLEAR' or clear_count == 0:
                risk_factor = 1.0
            else:
                # Adjust for total crashes in each weather type
                risk_factor = (count / clear_count) * (len(df) / count)
            
            risk_factors[weather] = {
                "risk_factor": float(risk_factor),
                "crash_count": int(count),
                "risk_level": "High" if risk_factor > 2.0 else "Medium" if risk_factor > 1.2 else "Low"
            }
        
        return risk_factors
    
    def perform_comprehensive_analysis(self, days_back: int = 365) -> Dict[str, Any]:
        """
        Perform comprehensive analysis combining all analyses
        
        Args:
            days_back: Number of days to look back
            
        Returns:
            Dictionary with comprehensive analysis results
        """
        print("Performing comprehensive analysis...")
        
        try:
            # Run all analyses
            temporal_results = self.perform_temporal_analysis(days_back)
            weather_results = self.perform_weather_analysis(days_back)
            
            # Combine results
            comprehensive_results = {
                "analysis_type": "comprehensive_analysis",
                "analysis_timestamp": datetime.now().isoformat(),
                "time_period": {
                    "days_back": days_back,
                    "analysis_date": datetime.now().strftime("%Y-%m-%d")
                },
                "temporal_analysis": temporal_results,
                "weather_analysis": weather_results,
                "summary_insights": self._generate_summary_insights(
                    temporal_results, weather_results
                )
            }
            
            print("✓ Comprehensive analysis completed")
            return comprehensive_results
            
        except Exception as e:
            logger.error(f"Error in comprehensive analysis: {e}")
            return {"error": str(e)}
    
    def _generate_summary_insights(self, temporal, weather) -> Dict[str, Any]:
        """Generate summary insights from all analyses"""
        insights = {
            "key_findings": [],
            "risk_factors": [],
            "recommendations": [],
            "data_quality_notes": []
        }
        
        try:
            # Extract key findings from temporal analysis
            if "error" not in temporal:
                if "peak_hours" in temporal:
                    peak_hour = temporal["peak_hours"].get("peak_hour", "N/A")
                    insights["key_findings"].append(f"Peak crash hour: {peak_hour}:00")
                
                if "weekend_vs_weekday" in temporal:
                    weekend_ratio = temporal["weekend_vs_weekday"].get("weekend_ratio", 0)
                    insights["key_findings"].append(
                        f"Weekend crash ratio: {weekend_ratio:.1%}"
                    )
            
            # Extract from weather analysis
            if "error" not in weather:
                if "clear_vs_adverse" in weather:
                    clear_pct = weather["clear_vs_adverse"].get("clear_percentage", 0)
                    adverse_pct = weather["clear_vs_adverse"].get("adverse_percentage", 0)
                    insights["key_findings"].append(
                        f"Clear weather crashes: {clear_pct:.1f}%, Adverse: {adverse_pct:.1f}%"
                    )

            # Generate recommendations
            insights["recommendations"].extend([
                "Increase police patrols during peak crash hours"
            ])            
        except Exception as e:
            logger.warning(f"Error generating summary insights: {e}")
            insights["data_quality_notes"].append("Some insights may be incomplete due to data limitations")
        
        return insights
