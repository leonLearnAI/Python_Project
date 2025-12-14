"""
Traffic Data Visualization Engine
Independent class for generating visualizations from analysis results
"""

import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from datetime import datetime
from typing import Dict, Any, List, Optional
import os
import pandas as pd
from matplotlib import gridspec

class TrafficVisualizationEngine:
    """Engine for generating traffic data visualizations"""
    
    def __init__(self, output_dir: str = "reports/visualizations"):
        self.output_dir = output_dir
        self._setup_directories()
        
    def _setup_directories(self):
        """Create necessary directories"""
        directories = [
            self.output_dir,
            f"{self.output_dir}/temporal",
            f"{self.output_dir}/weather",
            f"{self.output_dir}/comprehensive"
        ]
        
        for directory in directories:
            os.makedirs(directory, exist_ok=True)
    
    def generate_temporal_visualizations(self, analysis_results: Dict[str, Any]) -> List[str]:
        """
        Generate visualizations for temporal analysis
        
        Args:
            analysis_results: Results from temporal analysis
            
        Returns:
            List of file paths for generated charts
        """
        print("Generating temporal visualizations...")
        
        if "error" in analysis_results:
            print(f"Error in analysis results: {analysis_results['error']}")
            return []
        
        generated_files = []
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # 1. Hourly Distribution Chart
        if "hourly_distribution" in analysis_results:
            filepath = self._create_hourly_distribution_chart(
                analysis_results["hourly_distribution"],
                analysis_results.get("peak_hours", {}),
                timestamp
            )
            generated_files.append(filepath)
        
        # 2. Daily Distribution Chart
        if "daily_distribution" in analysis_results:
            filepath = self._create_daily_distribution_chart(
                analysis_results["daily_distribution"],
                timestamp
            )
            generated_files.append(filepath)
        
        # 3. Monthly Distribution Chart
        if "monthly_distribution" in analysis_results:
            filepath = self._create_monthly_distribution_chart(
                analysis_results["monthly_distribution"],
                timestamp
            )
            generated_files.append(filepath)
        
        # 4. Peak Hours Analysis Chart
        if "peak_hours" in analysis_results:
            filepath = self._create_peak_hours_chart(
                analysis_results["peak_hours"],
                timestamp
            )
            generated_files.append(filepath)
        
        # 5. Weekend vs Weekday Chart
        if "weekend_vs_weekday" in analysis_results:
            filepath = self._create_weekend_weekday_chart(
                analysis_results["weekend_vs_weekday"],
                timestamp
            )
            generated_files.append(filepath)
        
        # 6. Seasonal Patterns Chart
        if "seasonal_patterns" in analysis_results:
            filepath = self._create_seasonal_patterns_chart(
                analysis_results["seasonal_patterns"],
                timestamp
            )
            generated_files.append(filepath)
        
        # 7. Comprehensive Temporal Dashboard
        if all(key in analysis_results for key in ["hourly_distribution", "daily_distribution", 
                                                  "monthly_distribution", "peak_hours"]):
            filepath = self._create_temporal_dashboard(
                analysis_results,
                timestamp
            )
            generated_files.append(filepath)
        
        print(f"✓ Generated {len(generated_files)} temporal visualizations")
        return generated_files
    
    def _create_hourly_distribution_chart(self, hourly_data: Dict[int, int], 
                                         peak_hours: Dict[str, Any], 
                                         timestamp: str) -> str:
        """Create chart for hourly crash distribution"""
        plt.figure(figsize=(12, 6))
        
        hours = list(hourly_data.keys())
        counts = list(hourly_data.values())
        
        # Create bar chart
        bars = plt.bar(hours, counts, color='skyblue', alpha=0.7, edgecolor='navy', linewidth=1)
        
        # Highlight peak hour
        if peak_hours and "peak_hour" in peak_hours:
            peak_hour = peak_hours["peak_hour"]
            if peak_hour in hours:
                idx = hours.index(peak_hour)
                bars[idx].set_color('red')
                bars[idx].set_alpha(0.9)
                
                # Add annotation
                plt.annotate(f'Peak: {counts[idx]} crashes',
                            xy=(peak_hour, counts[idx]),
                            xytext=(peak_hour, counts[idx] + max(counts)*0.05),
                            ha='center',
                            arrowprops=dict(arrowstyle='->', color='red'),
                            fontsize=10, fontweight='bold')
        
        # Highlight rush hours
        rush_hours = [(7, 9), (16, 18)]  # Morning and evening rush
        for start, end in rush_hours:
            plt.axvspan(start-0.5, end+0.5, alpha=0.1, color='orange', label='Rush Hours')
        
        plt.title('Hourly Crash Distribution', fontsize=14, fontweight='bold', pad=20)
        plt.xlabel('Hour of Day', fontsize=12)
        plt.ylabel('Number of Crashes', fontsize=12)
        plt.xticks(range(0, 24, 2))
        plt.grid(True, alpha=0.3, axis='y')
        
        # Add total crashes
        total_crashes = sum(counts)
        plt.text(0.02, 0.98, f'Total Crashes: {total_crashes:,}',
                transform=plt.gca().transAxes, fontsize=10,
                verticalalignment='top', bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))
        
        plt.tight_layout()
        
        filename = f"{self.output_dir}/temporal/hourly_distribution_{timestamp}.png"
        plt.savefig(filename, dpi=300, bbox_inches='tight')
        plt.close()
        
        return filename
    
    def _create_daily_distribution_chart(self, daily_data: Dict[str, int], 
                                        timestamp: str) -> str:
        """Create chart for daily crash distribution"""
        plt.figure(figsize=(10, 6))
        
        # Ensure correct order of days
        day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        days = [day for day in day_order if day in daily_data]
        counts = [daily_data[day] for day in days]
        
        # Color weekends differently
        colors = ['skyblue' if day not in ['Saturday', 'Sunday'] else 'lightcoral' for day in days]
        
        bars = plt.bar(days, counts, color=colors, alpha=0.7, edgecolor='navy', linewidth=1)
        
        plt.title('Daily Crash Distribution', fontsize=14, fontweight='bold', pad=20)
        plt.xlabel('Day of Week', fontsize=12)
        plt.ylabel('Number of Crashes', fontsize=12)
        plt.xticks(rotation=45, ha='right')
        plt.grid(True, alpha=0.3, axis='y')
        
        # Add average line
        avg_count = np.mean(counts) if counts else 0
        plt.axhline(y=avg_count, color='red', linestyle='--', alpha=0.7, label=f'Average: {avg_count:.1f}')
        plt.legend()
        
        # Add value labels on bars
        for bar in bars:
            height = bar.get_height()
            plt.text(bar.get_x() + bar.get_width()/2., height + 0.5,
                    f'{int(height)}', ha='center', va='bottom', fontsize=9)
        
        plt.tight_layout()
        
        filename = f"{self.output_dir}/temporal/daily_distribution_{timestamp}.png"
        plt.savefig(filename, dpi=300, bbox_inches='tight')
        plt.close()
        
        return filename
    
    def _create_monthly_distribution_chart(self, monthly_data: Dict[str, int], 
                                          timestamp: str) -> str:
        """Create chart for monthly crash distribution"""
        plt.figure(figsize=(12, 6))
        
        # Ensure correct order of months
        month_order = ['January', 'February', 'March', 'April', 'May', 'June',
                      'July', 'August', 'September', 'October', 'November', 'December']
        months = [month for month in month_order if month in monthly_data]
        counts = [monthly_data[month] for month in months]
        
        # Color by season
        season_colors = {
            'Winter': 'lightblue',
            'Spring': 'lightgreen',
            'Summer': 'gold',
            'Fall': 'orange'
        }
        
        colors = []
        for month in months:
            if month in ['December', 'January', 'February']:
                colors.append(season_colors['Winter'])
            elif month in ['March', 'April', 'May']:
                colors.append(season_colors['Spring'])
            elif month in ['June', 'July', 'August']:
                colors.append(season_colors['Summer'])
            else:
                colors.append(season_colors['Fall'])
        
        bars = plt.bar(months, counts, color=colors, alpha=0.7, edgecolor='navy', linewidth=1)
        
        plt.title('Monthly Crash Distribution', fontsize=14, fontweight='bold', pad=20)
        plt.xlabel('Month', fontsize=12)
        plt.ylabel('Number of Crashes', fontsize=12)
        plt.xticks(rotation=45, ha='right')
        plt.grid(True, alpha=0.3, axis='y')
        
        # Add trend line
        if len(counts) >= 3:
            x = range(len(counts))
            z = np.polyfit(x, counts, 1)
            p = np.poly1d(z)
            plt.plot(months, p(x), "r--", alpha=0.8, linewidth=2, label='Trend')
            plt.legend()
        
        plt.tight_layout()
        
        filename = f"{self.output_dir}/temporal/monthly_distribution_{timestamp}.png"
        plt.savefig(filename, dpi=300, bbox_inches='tight')
        plt.close()
        
        return filename
    
    def _create_peak_hours_chart(self, peak_hours_data: Dict[str, Any], 
                                timestamp: str) -> str:
        """Create chart for peak hours analysis"""
        fig, axes = plt.subplots(1, 2, figsize=(14, 6))
        fig.suptitle('Peak Hours Analysis', fontsize=16, fontweight='bold')
        
        # Subplot 1: Peak Hour Details
        ax1 = axes[0]
        
        labels = ['Peak Hour', 'Morning Rush\n(7-9 AM)', 'Evening Rush\n(4-6 PM)']
        values = [
            peak_hours_data.get("peak_count", 0),
            peak_hours_data.get("morning_rush_crashes", 0),
            peak_hours_data.get("evening_rush_crashes", 0)
        ]
        
        colors = ['red', 'orange', 'purple']
        bars = ax1.bar(labels, values, color=colors, alpha=0.7)
        
        ax1.set_ylabel('Number of Crashes', fontsize=12)
        ax1.grid(True, alpha=0.3, axis='y')
        
        # Add value labels
        for bar in bars:
            height = bar.get_height()
            ax1.text(bar.get_x() + bar.get_width()/2., height + 0.5,
                    f'{int(height)}', ha='center', va='bottom', fontsize=10)
        
        # Subplot 2: Rush Hour Percentage
        ax2 = axes[1]
        
        rush_percentage = peak_hours_data.get("rush_hour_percentage", 0)
        non_rush_percentage = 100 - rush_percentage
        
        sizes = [rush_percentage, non_rush_percentage]
        labels = ['Rush Hours', 'Non-Rush Hours']
        colors = ['orange', 'lightgray']
        explode = (0.1, 0)
        
        wedges, texts, autotexts = ax2.pie(sizes, explode=explode, labels=labels, colors=colors,
                                          autopct='%1.1f%%', shadow=True, startangle=90)
        
        ax2.set_title('Rush Hour vs Non-Rush Hour Crashes', fontsize=12)
        
        # Make autotexts white and bold
        for autotext in autotexts:
            autotext.set_color('white')
            autotext.set_fontweight('bold')
        
        plt.tight_layout()
        
        filename = f"{self.output_dir}/temporal/peak_hours_analysis_{timestamp}.png"
        plt.savefig(filename, dpi=300, bbox_inches='tight')
        plt.close()
        
        return filename
    
    def _create_weekend_weekday_chart(self, weekend_data: Dict[str, Any], 
                                     timestamp: str) -> str:
        """Create chart for weekend vs weekday analysis"""
        fig, axes = plt.subplots(1, 2, figsize=(14, 6))
        fig.suptitle('Weekend vs Weekday Analysis', fontsize=16, fontweight='bold')
        
        # Subplot 1: Crash Counts
        ax1 = axes[0]
        
        labels = ['Weekend', 'Weekday']
        values = [
            weekend_data.get("weekend_crashes", 0),
            weekend_data.get("weekday_crashes", 0)
        ]
        
        colors = ['lightcoral', 'skyblue']
        bars = ax1.bar(labels, values, color=colors, alpha=0.7, edgecolor='navy', linewidth=1)
        
        ax1.set_ylabel('Number of Crashes', fontsize=12)
        ax1.grid(True, alpha=0.3, axis='y')
        
        # Add value labels
        for bar in bars:
            height = bar.get_height()
            ax1.text(bar.get_x() + bar.get_width()/2., height + 0.5,
                    f'{int(height)}', ha='center', va='bottom', fontsize=12, fontweight='bold')
        
        # Subplot 2: Ratios
        ax2 = axes[1]
        
        weekend_ratio = weekend_data.get("weekend_ratio", 0)
        weekday_ratio = weekend_data.get("weekday_ratio", 0)
        weekend_to_weekday = weekend_data.get("weekend_to_weekday_ratio", 0)
        
        # Create grouped bar chart
        x = np.arange(3)
        width = 0.6
        
        metrics = [weekend_ratio, weekday_ratio, weekend_to_weekday]
        metric_labels = ['Weekend\nRatio', 'Weekday\nRatio', 'Weekend/Weekday\nRatio']
        colors = ['lightcoral', 'skyblue', 'gold']
        
        bars = ax2.bar(x, metrics, width, color=colors, alpha=0.7)
        
        ax2.set_ylabel('Ratio', fontsize=12)
        ax2.set_xticks(x)
        ax2.set_xticklabels(metric_labels)
        ax2.grid(True, alpha=0.3, axis='y')
        
        # Add ratio values
        for bar, metric in zip(bars, metrics):
            height = bar.get_height()
            ax2.text(bar.get_x() + bar.get_width()/2., height + 0.01,
                    f'{metric:.3f}', ha='center', va='bottom', fontsize=10)
        
        plt.tight_layout()
        
        filename = f"{self.output_dir}/temporal/weekend_weekday_analysis_{timestamp}.png"
        plt.savefig(filename, dpi=300, bbox_inches='tight')
        plt.close()
        
        return filename
    
    def _create_seasonal_patterns_chart(self, seasonal_data: Dict[str, Any], 
                                    timestamp: str) -> str:
        """Create chart for seasonal patterns"""
        plt.figure(figsize=(10, 6))
        
        if "seasonal_distribution" not in seasonal_data:
            return ""
        
        seasonal_dist = seasonal_data["seasonal_distribution"]
        
        # Ensure correct order of seasons
        season_order = ['Winter', 'Spring', 'Summer', 'Fall']
        seasons = [season for season in season_order if season in seasonal_dist]
        counts = [seasonal_dist[season] for season in seasons]
        
        colors = ['lightblue', 'lightgreen', 'gold', 'orange']
        explode = [0.05 if season == seasonal_data.get("peak_season", "") else 0 
                for season in seasons]
        
        wedges, texts, autotexts = plt.pie(counts, labels=seasons, colors=colors,
                                        explode=explode, autopct='%1.1f%%',
                                        shadow=True, startangle=90)
        
        plt.title('Seasonal Crash Distribution', fontsize=14, fontweight='bold', pad=20)
        
        # Highlight peak season
        peak_season = seasonal_data.get("peak_season")
        if peak_season and peak_season in seasons:
            idx = seasons.index(peak_season)
            wedges[idx].set_edgecolor('red')
            wedges[idx].set_linewidth(2)
            
            # FIXED: Get the center of the wedge for annotation
            # Calculate the angle for the center of the wedge
            total = sum(counts)
            start_angle = 90  # Because startangle=90
            
            # Calculate the cumulative angles
            cum_counts = 0
            for i in range(idx):
                cum_counts += counts[i]
            
            # Calculate the angle for the center of this wedge
            wedge_center_angle = start_angle + (cum_counts + counts[idx]/2) / total * 360
            
            # Convert to radians and calculate x, y coordinates
            angle_rad = np.deg2rad(wedge_center_angle)
            x = 0.8 * np.cos(angle_rad)  # 0.8 is the radius offset
            y = 0.8 * np.sin(angle_rad)
            
            # Add annotation with correct coordinates
            plt.annotate(f'Peak Season\n{counts[idx]} crashes',
                        xy=(x, y),
                        xytext=(1.2, 0.5),
                        arrowprops=dict(arrowstyle='->', color='red'),
                        fontsize=10, fontweight='bold')
        
        # Make autotexts white and bold
        for autotext in autotexts:
            autotext.set_color('white')
            autotext.set_fontweight('bold')
        
        # Add seasonal variation metric
        variation = seasonal_data.get("seasonal_variation", 0)
        plt.text(0.5, -0.1, f'Seasonal Variation: {variation:.3f}',
                transform=plt.gca().transAxes, ha='center',
                fontsize=10, bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))
        
        plt.tight_layout()
        
        filename = f"{self.output_dir}/temporal/seasonal_patterns_{timestamp}.png"
        plt.savefig(filename, dpi=300, bbox_inches='tight')
        plt.close()
        
        return filename
    
    def _create_temporal_dashboard(self, analysis_results: Dict[str, Any], 
                                  timestamp: str) -> str:
        """Create comprehensive temporal analysis dashboard"""
        fig = plt.figure(figsize=(20, 16))
        fig.suptitle('Temporal Analysis Dashboard', fontsize=18, fontweight='bold', y=0.98)
        
        # Create grid layout
        gs = gridspec.GridSpec(3, 3, figure=fig, hspace=0.3, wspace=0.3)
        
        # 1. Hourly Distribution (top left)
        ax1 = fig.add_subplot(gs[0, 0])
        hourly_data = analysis_results["hourly_distribution"]
        hours = list(hourly_data.keys())
        counts = list(hourly_data.values())
        ax1.bar(hours, counts, color='skyblue', alpha=0.7)
        ax1.set_title('Hourly Distribution', fontsize=12, fontweight='bold')
        ax1.set_xlabel('Hour')
        ax1.set_ylabel('Crashes')
        ax1.grid(True, alpha=0.3, axis='y')
        ax1.set_xticks(range(0, 24, 3))
        
        # 2. Daily Distribution (top middle)
        ax2 = fig.add_subplot(gs[0, 1])
        daily_data = analysis_results["daily_distribution"]
        day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        days = [day for day in day_order if day in daily_data]
        daily_counts = [daily_data[day] for day in days]
        colors = ['skyblue' if day not in ['Saturday', 'Sunday'] else 'lightcoral' for day in days]
        ax2.bar(days, daily_counts, color=colors, alpha=0.7)
        ax2.set_title('Daily Distribution', fontsize=12, fontweight='bold')
        ax2.set_xlabel('Day')
        ax2.set_ylabel('Crashes')
        ax2.grid(True, alpha=0.3, axis='y')
        plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45, ha='right')
        
        # 3. Monthly Distribution (top right)
        ax3 = fig.add_subplot(gs[0, 2])
        monthly_data = analysis_results["monthly_distribution"]
        month_order = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                      'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
        months = [month[:3] for month in month_order if month in monthly_data or 
                 any(m.startswith(month) for m in monthly_data.keys())]
        monthly_counts = []
        for month in month_order:
            for key in monthly_data.keys():
                if key.startswith(month):
                    monthly_counts.append(monthly_data[key])
                    break
            else:
                monthly_counts.append(0)
        monthly_counts = monthly_counts[:len(months)]
        ax3.plot(months, monthly_counts, marker='o', linewidth=2, color='green')
        ax3.fill_between(months, 0, monthly_counts, alpha=0.3, color='green')
        ax3.set_title('Monthly Trend', fontsize=12, fontweight='bold')
        ax3.set_xlabel('Month')
        ax3.set_ylabel('Crashes')
        ax3.grid(True, alpha=0.3)
        plt.setp(ax3.xaxis.get_majorticklabels(), rotation=45)
        
        # 4. Peak Hours Analysis (middle left)
        ax4 = fig.add_subplot(gs[1, 0])
        peak_hours = analysis_results["peak_hours"]
        labels = ['Peak', 'Morning\nRush', 'Evening\nRush']
        values = [peak_hours.get("peak_count", 0),
                 peak_hours.get("morning_rush_crashes", 0),
                 peak_hours.get("evening_rush_crashes", 0)]
        colors = ['red', 'orange', 'purple']
        ax4.bar(labels, values, color=colors, alpha=0.7)
        ax4.set_title('Peak Hours Analysis', fontsize=12, fontweight='bold')
        ax4.set_ylabel('Crashes')
        ax4.grid(True, alpha=0.3, axis='y')
        
        # 5. Weekend vs Weekday (middle middle)
        ax5 = fig.add_subplot(gs[1, 1])
        weekend_data = analysis_results["weekend_vs_weekday"]
        labels = ['Weekend', 'Weekday']
        values = [weekend_data.get("weekend_crashes", 0),
                 weekend_data.get("weekday_crashes", 0)]
        colors = ['lightcoral', 'skyblue']
        wedges, texts, autotexts = ax5.pie(values, labels=labels, colors=colors,
                                          autopct='%1.1f%%', startangle=90)
        ax5.set_title('Weekend vs Weekday', fontsize=12, fontweight='bold')
        for autotext in autotexts:
            autotext.set_color('white')
            autotext.set_fontweight('bold')
        
        # 6. Seasonal Patterns (middle right)
        ax6 = fig.add_subplot(gs[1, 2])
        seasonal_data = analysis_results["seasonal_patterns"]
        if "seasonal_distribution" in seasonal_data:
            seasonal_dist = seasonal_data["seasonal_distribution"]
            season_order = ['Winter', 'Spring', 'Summer', 'Fall']
            seasons = [s for s in season_order if s in seasonal_dist]
            season_counts = [seasonal_dist[s] for s in seasons]
            colors = ['lightblue', 'lightgreen', 'gold', 'orange']
            ax6.bar(seasons, season_counts, color=colors, alpha=0.7)
            ax6.set_title('Seasonal Distribution', fontsize=12, fontweight='bold')
            ax6.set_ylabel('Crashes')
            ax6.grid(True, alpha=0.3, axis='y')
        
        # 7. Key Metrics Summary (bottom row, full width)
        ax7 = fig.add_subplot(gs[2, :])
        ax7.axis('off')
        
        # Prepare summary text
        total_crashes = analysis_results.get("total_crashes", 0)
        peak_hour = peak_hours.get("peak_hour", "N/A")
        rush_percentage = peak_hours.get("rush_hour_percentage", 0)
        weekend_ratio = weekend_data.get("weekend_ratio", 0)
        peak_season = seasonal_data.get("peak_season", "N/A")
        
        summary_text = f"""
        TEMPORAL ANALYSIS SUMMARY
        {'='*40}
        
        • Total Crashes Analyzed: {total_crashes:,}
        • Peak Crash Hour: {peak_hour}:00
        • Rush Hour Crashes: {rush_percentage:.1f}%
        • Weekend Crash Ratio: {weekend_ratio:.1%}
        • Peak Season: {peak_season}
        • Analysis Period: {analysis_results.get('time_period', {}).get('days', 'N/A')} days
        
        KEY INSIGHTS:
        1. Highest crash frequency occurs during {peak_hour}:00
        2. Rush hours account for {rush_percentage:.1f}% of all crashes
        3. Weekend driving shows {weekend_ratio:.1%} crash probability
        4. Seasonal patterns indicate {peak_season} as highest risk period
        """
        
        ax7.text(0.02, 0.98, summary_text, transform=ax7.transAxes,
                fontsize=11, family='monospace', verticalalignment='top',
                bbox=dict(boxstyle='round', facecolor='lightyellow', alpha=0.8))
        
        plt.tight_layout()
        
        filename = f"{self.output_dir}/temporal/temporal_dashboard_{timestamp}.png"
        plt.savefig(filename, dpi=300, bbox_inches='tight')
        plt.close()
        
        return filename
    
    def generate_weather_visualizations(self, analysis_results: Dict[str, Any]) -> List[str]:
        """
        Generate visualizations for weather analysis
        
        Args:
            analysis_results: Results from weather analysis
            
        Returns:
            List of file paths for generated charts
        """
        print("Generating weather visualizations...")
        
        if "error" in analysis_results:
            print(f"Error in analysis results: {analysis_results['error']}")
            return []
        
        generated_files = []
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # 1. Weather Distribution Chart
        if "weather_distribution" in analysis_results:
            filepath = self._create_weather_distribution_chart(
                analysis_results["weather_distribution"],
                timestamp
            )
            generated_files.append(filepath)
        
        # 2. Weather Severity Analysis Chart
        if "weather_severity_analysis" in analysis_results:
            filepath = self._create_weather_severity_chart(
                analysis_results["weather_severity_analysis"],
                timestamp
            )
            generated_files.append(filepath)
        
        # 3. Clear vs Adverse Weather Chart
        if "clear_vs_adverse" in analysis_results:
            filepath = self._create_clear_adverse_chart(
                analysis_results["clear_vs_adverse"],
                timestamp
            )
            generated_files.append(filepath)
        
        # 4. Weather Risk Factors Chart
        if "weather_risk_factors" in analysis_results:
            filepath = self._create_weather_risk_factors_chart(
                analysis_results["weather_risk_factors"],
                timestamp
            )
            generated_files.append(filepath)
        
        # 5. Weather Temporal Patterns Chart
        if "weather_temporal_patterns" in analysis_results:
            filepath = self._create_weather_temporal_chart(
                analysis_results["weather_temporal_patterns"],
                timestamp
            )
            generated_files.append(filepath)
        
        # 6. Comprehensive Weather Dashboard
        if all(key in analysis_results for key in ["weather_distribution", "weather_severity_analysis",
                                                  "clear_vs_adverse", "weather_risk_factors"]):
            filepath = self._create_weather_dashboard(
                analysis_results,
                timestamp
            )
            generated_files.append(filepath)
        
        print(f"✓ Generated {len(generated_files)} weather visualizations")
        return generated_files
    
    def _create_weather_distribution_chart(self, weather_data: Dict[str, Dict[str, Any]], 
                                          timestamp: str) -> str:
        """Create chart for weather distribution"""
        plt.figure(figsize=(12, 6))
        
        # Extract data
        weather_types = list(weather_data.keys())
        counts = [data["count"] for data in weather_data.values()]
        percentages = [data["percentage"] for data in weather_data.values()]
        
        # Sort by count (descending)
        sorted_indices = np.argsort(counts)[::-1]
        weather_types = [weather_types[i] for i in sorted_indices]
        counts = [counts[i] for i in sorted_indices]
        percentages = [percentages[i] for i in sorted_indices]
        
        # Create horizontal bar chart
        y_pos = range(len(weather_types))
        colors = plt.cm.Blues(np.linspace(0.3, 0.9, len(weather_types)))
        
        bars = plt.barh(y_pos, counts, color=colors, alpha=0.7, edgecolor='navy', linewidth=1)
        
        plt.title('Crash Distribution by Weather Condition', fontsize=14, fontweight='bold', pad=20)
        plt.xlabel('Number of Crashes', fontsize=12)
        plt.yticks(y_pos, weather_types)
        plt.gca().invert_yaxis()
        plt.grid(True, alpha=0.3, axis='x')
        
        # Add percentage labels
        for i, (bar, percentage) in enumerate(zip(bars, percentages)):
            width = bar.get_width()
            plt.text(width + max(counts)*0.01, bar.get_y() + bar.get_height()/2.,
                    f'{percentage:.1f}%', ha='left', va='center', fontsize=9)
        
        # Add total count
        total = sum(counts)
        plt.text(0.02, 0.98, f'Total Crashes: {total:,}',
                transform=plt.gca().transAxes, fontsize=10,
                verticalalignment='top', bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))
        
        plt.tight_layout()
        
        filename = f"{self.output_dir}/weather/weather_distribution_{timestamp}.png"
        plt.savefig(filename, dpi=300, bbox_inches='tight')
        plt.close()
        
        return filename
    
    def _create_weather_severity_chart(self, severity_data: Dict[str, Dict[str, Any]], 
                                      timestamp: str) -> str:
        """Create chart for weather severity analysis"""
        plt.figure(figsize=(12, 6))
        
        # Extract data
        weather_types = list(severity_data.keys())
        avg_severities = [data["average_severity"] for data in severity_data.values()]
        crash_counts = [data["crash_count"] for data in severity_data.values()]
        
        # Sort by average severity (descending)
        sorted_indices = np.argsort(avg_severities)[::-1]
        weather_types = [weather_types[i] for i in sorted_indices]
        avg_severities = [avg_severities[i] for i in sorted_indices]
        crash_counts = [crash_counts[i] for i in sorted_indices]
        
        # Create figure with two y-axes
        fig, ax1 = plt.subplots(figsize=(12, 6))
        
        # Bar chart for average severity
        colors = plt.cm.RdYlGn_r(np.linspace(0.2, 0.8, len(weather_types)))
        bars = ax1.bar(weather_types, avg_severities, color=colors, alpha=0.7)
        
        ax1.set_xlabel('Weather Condition', fontsize=12)
        ax1.set_ylabel('Average Severity Score', fontsize=12, color='darkred')
        ax1.tick_params(axis='y', labelcolor='darkred')
        plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45, ha='right')
        ax1.grid(True, alpha=0.3, axis='y')
        
        # Add value labels on bars
        for bar, severity in zip(bars, avg_severities):
            height = bar.get_height()
            ax1.text(bar.get_x() + bar.get_width()/2., height + 0.05,
                    f'{severity:.2f}', ha='center', va='bottom', fontsize=9, fontweight='bold')
        
        # Create second y-axis for crash counts
        ax2 = ax1.twinx()
        ax2.plot(weather_types, crash_counts, 'o-', color='darkblue', linewidth=2, markersize=8)
        ax2.set_ylabel('Number of Crashes', fontsize=12, color='darkblue')
        ax2.tick_params(axis='y', labelcolor='darkblue')
        
        plt.title('Weather Conditions vs Injury Severity', fontsize=14, fontweight='bold', pad=20)
        
        # Add legend
        ax1.legend(['Average Severity'], loc='upper left')
        ax2.legend(['Crash Count'], loc='upper right')
        
        plt.tight_layout()
        
        filename = f"{self.output_dir}/weather/weather_severity_{timestamp}.png"
        plt.savefig(filename, dpi=300, bbox_inches='tight')
        plt.close()
        
        return filename
    
    def _create_clear_adverse_chart(self, clear_adverse_data: Dict[str, Any], 
                                   timestamp: str) -> str:
        """Create chart for clear vs adverse weather comparison"""
        fig, axes = plt.subplots(1, 2, figsize=(14, 6))
        fig.suptitle('Clear vs Adverse Weather Analysis', fontsize=16, fontweight='bold')
        
        # Subplot 1: Crash Counts
        ax1 = axes[0]
        
        labels = ['Clear Weather', 'Adverse Weather']
        clear_count = clear_adverse_data.get("clear_weather_crashes", 0)
        adverse_count = clear_adverse_data.get("adverse_weather_crashes", 0)
        values = [clear_count, adverse_count]
        
        colors = ['lightgreen', 'lightcoral']
        bars = ax1.bar(labels, values, color=colors, alpha=0.7, edgecolor='navy', linewidth=1)
        
        ax1.set_ylabel('Number of Crashes', fontsize=12)
        ax1.grid(True, alpha=0.3, axis='y')
        
        # Add value labels
        for bar in bars:
            height = bar.get_height()
            ax1.text(bar.get_x() + bar.get_width()/2., height + 0.5,
                    f'{int(height)}', ha='center', va='bottom', fontsize=12, fontweight='bold')
        
        # Subplot 2: Percentages
        ax2 = axes[1]
        
        clear_pct = clear_adverse_data.get("clear_percentage", 0)
        adverse_pct = clear_adverse_data.get("adverse_percentage", 0)
        ratio = clear_adverse_data.get("clear_adverse_ratio", 0)
        
        # Create pie chart
        sizes = [clear_pct, adverse_pct]
        labels_pie = [f'Clear\n{clear_pct:.1f}%', f'Adverse\n{adverse_pct:.1f}%']
        colors_pie = ['lightgreen', 'lightcoral']
        explode = (0.05, 0)
        
        wedges, texts, autotexts = ax2.pie(sizes, explode=explode, labels=labels_pie, 
                                          colors=colors_pie, autopct='', shadow=True, 
                                          startangle=90)
        
        # Add ratio text
        ax2.text(0, 0, f'Ratio: {ratio:.2f}', ha='center', va='center',
                fontsize=11, fontweight='bold', bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.8))
        
        plt.tight_layout()
        
        filename = f"{self.output_dir}/weather/clear_adverse_weather_{timestamp}.png"
        plt.savefig(filename, dpi=300, bbox_inches='tight')
        plt.close()
        
        return filename
    
    def _create_weather_risk_factors_chart(self, risk_data: Dict[str, Dict[str, Any]], 
                                          timestamp: str) -> str:
        """Create chart for weather risk factors"""
        plt.figure(figsize=(12, 6))
        
        # Extract and sort data by risk factor
        weather_types = list(risk_data.keys())
        risk_factors = [data["risk_factor"] for data in risk_data.values()]
        risk_levels = [data["risk_level"] for data in risk_data.values()]
        
        # Sort by risk factor (descending)
        sorted_indices = np.argsort(risk_factors)[::-1]
        weather_types = [weather_types[i] for i in sorted_indices]
        risk_factors = [risk_factors[i] for i in sorted_indices]
        risk_levels = [risk_levels[i] for i in sorted_indices]
        
        # Color by risk level
        colors = []
        for level in risk_levels:
            if level == 'High':
                colors.append('red')
            elif level == 'Medium':
                colors.append('orange')
            else:
                colors.append('green')
        
        # Create bar chart
        y_pos = range(len(weather_types))
        bars = plt.barh(y_pos, risk_factors, color=colors, alpha=0.7, edgecolor='black', linewidth=1)
        
        plt.title('Weather Condition Risk Factors', fontsize=14, fontweight='bold', pad=20)
        plt.xlabel('Risk Factor (Relative to Clear Weather)', fontsize=12)
        plt.yticks(y_pos, weather_types)
        plt.gca().invert_yaxis()
        plt.grid(True, alpha=0.3, axis='x')
        
        # Add baseline (clear weather = 1.0)
        plt.axvline(x=1.0, color='blue', linestyle='--', alpha=0.5, label='Baseline (Clear Weather)')
        plt.legend()
        
        # Add value labels
        for bar, risk, level in zip(bars, risk_factors, risk_levels):
            width = bar.get_width()
            plt.text(width + 0.05, bar.get_y() + bar.get_height()/2.,
                    f'{risk:.2f} ({level})', ha='left', va='center', fontsize=9)
        
        plt.tight_layout()
        
        filename = f"{self.output_dir}/weather/weather_risk_factors_{timestamp}.png"
        plt.savefig(filename, dpi=300, bbox_inches='tight')
        plt.close()
        
        return filename
    
    def _create_weather_temporal_chart(self, temporal_data: Dict[str, Dict[str, Any]], 
                                      timestamp: str) -> str:
        """Create chart for weather temporal patterns"""
        plt.figure(figsize=(14, 8))
        
        # Get unique weather types with peak hour data
        weather_types = []
        peak_hours = []
        
        for weather, data in temporal_data.items():
            if "peak_hour" in data:
                weather_types.append(weather)
                peak_hours.append(data["peak_hour"])
        
        if not weather_types:
            return ""
        
        # Create polar plot
        ax = plt.subplot(111, projection='polar')
        
        # Convert hours to radians
        theta = [hour * 2 * np.pi / 24 for hour in peak_hours]
        
        # Create colors based on weather type
        colors = plt.cm.Set2(np.linspace(0, 1, len(weather_types)))
        
        # Plot points
        scatter = ax.scatter(theta, [1] * len(theta), c=colors, s=200, alpha=0.7, edgecolors='black')
        
        # Customize polar plot
        ax.set_theta_zero_location('N')
        ax.set_theta_direction(-1)
        ax.set_xticks(np.linspace(0, 2*np.pi, 24, endpoint=False))
        ax.set_xticklabels([f'{h}:00' for h in range(24)])
        ax.set_ylim(0, 1.2)
        ax.grid(True, alpha=0.3)
        
        plt.title('Peak Crash Hours by Weather Condition', fontsize=16, fontweight='bold', pad=20)
        
        # Add legend
        for i, weather in enumerate(weather_types):
            ax.plot([], [], 'o', color=colors[i], label=f'{weather}: {peak_hours[i]}:00', markersize=8)
        
        ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
        
        plt.tight_layout()
        
        filename = f"{self.output_dir}/weather/weather_temporal_{timestamp}.png"
        plt.savefig(filename, dpi=300, bbox_inches='tight')
        plt.close()
        
        return filename
    
    def _create_weather_dashboard(self, analysis_results: Dict[str, Any], 
                                 timestamp: str) -> str:
        """Create comprehensive weather analysis dashboard"""
        fig = plt.figure(figsize=(20, 16))
        fig.suptitle('Weather Analysis Dashboard', fontsize=18, fontweight='bold', y=0.98)
        
        # Create grid layout
        gs = gridspec.GridSpec(3, 3, figure=fig, hspace=0.3, wspace=0.3)
        
        # 1. Weather Distribution (top left)
        ax1 = fig.add_subplot(gs[0, 0])
        weather_data = analysis_results["weather_distribution"]
        weather_types = list(weather_data.keys())[:6]  # Top 6
        counts = [weather_data[w]["count"] for w in weather_types]
        colors = plt.cm.Blues(np.linspace(0.3, 0.9, len(weather_types)))
        ax1.barh(weather_types, counts, color=colors)
        ax1.set_title('Top Weather Conditions', fontsize=12, fontweight='bold')
        ax1.set_xlabel('Crashes')
        ax1.invert_yaxis()
        ax1.grid(True, alpha=0.3, axis='x')
        
        # 2. Weather Severity (top middle)
        ax2 = fig.add_subplot(gs[0, 1])
        severity_data = analysis_results["weather_severity_analysis"]
        weather_types = list(severity_data.keys())[:5]  # Top 5
        severities = [severity_data[w]["average_severity"] for w in weather_types]
        colors = plt.cm.RdYlGn_r(np.linspace(0.2, 0.8, len(weather_types)))
        ax2.bar(weather_types, severities, color=colors)
        ax2.set_title('Average Severity by Weather', fontsize=12, fontweight='bold')
        ax2.set_ylabel('Severity Score')
        plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45, ha='right')
        ax2.grid(True, alpha=0.3, axis='y')
        
        # 3. Clear vs Adverse (top right)
        ax3 = fig.add_subplot(gs[0, 2])
        clear_data = analysis_results["clear_vs_adverse"]
        labels = ['Clear', 'Adverse']
        values = [clear_data.get("clear_weather_crashes", 0),
                 clear_data.get("adverse_weather_crashes", 0)]
        colors = ['lightgreen', 'lightcoral']
        wedges, texts, autotexts = ax3.pie(values, labels=labels, colors=colors,
                                          autopct='%1.1f%%', startangle=90)
        ax3.set_title('Clear vs Adverse Weather', fontsize=12, fontweight='bold')
        for autotext in autotexts:
            autotext.set_color('white')
            autotext.set_fontweight('bold')
        
        # 4. Risk Factors (middle left)
        ax4 = fig.add_subplot(gs[1, 0])
        risk_data = analysis_results["weather_risk_factors"]
        weather_types = list(risk_data.keys())[:5]
        risks = [risk_data[w]["risk_factor"] for w in weather_types]
        colors = ['red' if r > 1.5 else 'orange' if r > 1.0 else 'green' for r in risks]
        ax4.barh(weather_types, risks, color=colors)
        ax4.set_title('Weather Risk Factors', fontsize=12, fontweight='bold')
        ax4.set_xlabel('Risk Factor')
        ax4.invert_yaxis()
        ax4.axvline(x=1.0, color='blue', linestyle='--', alpha=0.5)
        ax4.grid(True, alpha=0.3, axis='x')
        
        # 5. Temporal Patterns (middle right, span 2 columns)
        ax5 = fig.add_subplot(gs[1, 1:])
        temporal_data = analysis_results.get("weather_temporal_patterns", {})
        
        # Prepare heatmap data
        weather_types = list(temporal_data.keys())[:4]
        heatmap_data = []
        
        for weather in weather_types:
            if "hourly_distribution" in temporal_data[weather]:
                hourly_data = temporal_data[weather]["hourly_distribution"]
                hours = list(range(24))
                counts = [hourly_data.get(h, 0) for h in hours]
                heatmap_data.append(counts)
        
        if heatmap_data:
            im = ax5.imshow(heatmap_data, cmap='YlOrRd', aspect='auto', interpolation='nearest')
            ax5.set_title('Hourly Patterns by Weather', fontsize=12, fontweight='bold')
            ax5.set_xlabel('Hour of Day')
            ax5.set_ylabel('Weather Condition')
            ax5.set_yticks(range(len(weather_types)))
            ax5.set_yticklabels(weather_types)
            ax5.set_xticks(range(0, 24, 3))
            ax5.set_xticklabels([f'{h}:00' for h in range(0, 24, 3)])
            plt.colorbar(im, ax=ax5, label='Crash Count')
        
        # 6. Key Metrics Summary (bottom row, full width)
        ax6 = fig.add_subplot(gs[2, :])
        ax6.axis('off')
        
        # Prepare summary text
        total_crashes = analysis_results.get("total_crashes_analyzed", 0)
        clear_pct = analysis_results.get("clear_vs_adverse", {}).get("clear_percentage", 0)
        adverse_pct = analysis_results.get("clear_vs_adverse", {}).get("adverse_percentage", 0)
        
        # Get highest risk weather
        risk_data = analysis_results.get("weather_risk_factors", {})
        if risk_data:
            highest_risk = max(risk_data.items(), key=lambda x: x[1]["risk_factor"])
            highest_risk_name = highest_risk[0]
            highest_risk_value = highest_risk[1]["risk_factor"]
        else:
            highest_risk_name = "N/A"
            highest_risk_value = 0
        
        # Get highest severity weather
        severity_data = analysis_results.get("weather_severity_analysis", {})
        if severity_data:
            highest_severity = max(severity_data.items(), key=lambda x: x[1]["average_severity"])
            highest_severity_name = highest_severity[0]
            highest_severity_value = highest_severity[1]["average_severity"]
        else:
            highest_severity_name = "N/A"
            highest_severity_value = 0
        
        summary_text = f"""
        WEATHER ANALYSIS SUMMARY
        {'='*40}
        
        • Total Crashes Analyzed: {total_crashes:,}
        • Clear Weather Crashes: {clear_pct:.1f}%
        • Adverse Weather Crashes: {adverse_pct:.1f}%
        • Highest Risk Weather: {highest_risk_name} (Risk Factor: {highest_risk_value:.2f})
        • Highest Severity Weather: {highest_severity_name} (Severity: {highest_severity_value:.2f})
        
        KEY INSIGHTS:
        1. {clear_pct:.1f}% of crashes occur in clear weather conditions
        2. {highest_risk_name} poses the highest relative risk ({highest_risk_value:.2f}x baseline)
        3. {highest_severity_name} conditions result in most severe crashes
        4. Weather impacts crash frequency and severity differently
        """
        
        ax6.text(0.02, 0.98, summary_text, transform=ax6.transAxes,
                fontsize=11, family='monospace', verticalalignment='top',
                bbox=dict(boxstyle='round', facecolor='lightyellow', alpha=0.8))
        
        plt.tight_layout()
        
        filename = f"{self.output_dir}/weather/weather_dashboard_{timestamp}.png"
        plt.savefig(filename, dpi=300, bbox_inches='tight')
        plt.close()
        
        return filename
    
    def generate_comprehensive_visualizations(self, analysis_results: Dict[str, Any]) -> List[str]:
        """
        Generate comprehensive visualizations from all analyses
        
        Args:
            analysis_results: Results from comprehensive analysis
            
        Returns:
            List of file paths for generated charts
        """
        print("Generating comprehensive visualizations...")
        
        if "error" in analysis_results:
            print(f"Error in analysis results: {analysis_results['error']}")
            return []
        
        generated_files = []
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Generate executive summary dashboard
        if all(key in analysis_results for key in ["temporal_analysis", "weather_analysis"]):
            filepath = self._create_executive_summary_dashboard(analysis_results, timestamp)
            generated_files.append(filepath)
        
        print(f"✓ Generated {len(generated_files)} comprehensive visualizations")
        return generated_files
    
    def _create_executive_summary_dashboard(self, analysis_results: Dict[str, Any], 
                                           timestamp: str) -> str:
        """Create executive summary dashboard"""
        fig = plt.figure(figsize=(20, 16))
        fig.suptitle('Traffic Safety Analysis - Executive Summary', fontsize=20, fontweight='bold', y=0.98)
        
        # Create grid layout
        gs = gridspec.GridSpec(4, 4, figure=fig, hspace=0.4, wspace=0.4)
        
        # Get data from each analysis
        temporal = analysis_results.get("temporal_analysis", {})
        weather = analysis_results.get("weather_analysis", {})
        summary = analysis_results.get("summary_insights", {})
        
        # 1. Temporal Highlights (top left)
        ax1 = fig.add_subplot(gs[0, 0])
        ax1.axis('off')
        
        temporal_text = "TEMPORAL HIGHLIGHTS\n" + "="*20 + "\n\n"
        
        if "peak_hours" in temporal:
            peak_hour = temporal["peak_hours"].get("peak_hour", "N/A")
            rush_pct = temporal["peak_hours"].get("rush_hour_percentage", 0)
            temporal_text += f"• Peak Hour: {peak_hour}:00\n"
            temporal_text += f"• Rush Hours: {rush_pct:.1f}%\n"
        
        if "weekend_vs_weekday" in temporal:
            weekend_ratio = temporal["weekend_vs_weekday"].get("weekend_ratio", 0)
            temporal_text += f"• Weekend Ratio: {weekend_ratio:.1%}\n"
        
        ax1.text(0.05, 0.95, temporal_text, transform=ax1.transAxes,
                fontsize=10, family='monospace', verticalalignment='top',
                bbox=dict(boxstyle='round', facecolor='lightblue', alpha=0.2))
        
        # 2. Weather Highlights (top middle)
        ax2 = fig.add_subplot(gs[0, 1])
        ax2.axis('off')
        
        weather_text = "WEATHER HIGHLIGHTS\n" + "="*20 + "\n\n"
        
        if "clear_vs_adverse" in weather:
            clear_pct = weather["clear_vs_adverse"].get("clear_percentage", 0)
            adverse_pct = weather["clear_vs_adverse"].get("adverse_percentage", 0)
            weather_text += f"• Clear Weather: {clear_pct:.1f}%\n"
            weather_text += f"• Adverse Weather: {adverse_pct:.1f}%\n"
        
        if "weather_risk_factors" in weather:
            risk_items = list(weather["weather_risk_factors"].items())
            if risk_items:
                top_risk = max(risk_items, key=lambda x: x[1]["risk_factor"])
                weather_text += f"• Highest Risk: {top_risk[0]}\n"
                weather_text += f"• Risk Factor: {top_risk[1]['risk_factor']:.2f}\n"
        
        ax2.text(0.05, 0.95, weather_text, transform=ax2.transAxes,
                fontsize=10, family='monospace', verticalalignment='top',
                bbox=dict(boxstyle='round', facecolor='lightgreen', alpha=0.2))
     
        # 5. Key Findings (middle left, span 2 rows)
        ax5 = fig.add_subplot(gs[1:3, 0])
        ax5.axis('off')
        
        findings_text = "KEY FINDINGS\n" + "="*15 + "\n\n"
        
        if summary and "key_findings" in summary:
            for i, finding in enumerate(summary["key_findings"][:6], 1):
                findings_text += f"{i}. {finding}\n"
        else:
            findings_text += "No key findings available\n"
        
        ax5.text(0.05, 0.95, findings_text, transform=ax5.transAxes,
                fontsize=10, family='monospace', verticalalignment='top',
                bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))
        
        # 6. Risk Factors (middle, span 2 rows)
        ax6 = fig.add_subplot(gs[1:3, 1])
        ax6.axis('off')
        
        risks_text = "IDENTIFIED RISK FACTORS\n" + "="*25 + "\n\n"
        
        if summary and "risk_factors" in summary:
            for i, risk in enumerate(summary["risk_factors"][:6], 1):
                risks_text += f"{i}. {risk}\n"
        else:
            risks_text += "No specific risk factors identified\n"
        
        ax6.text(0.05, 0.95, risks_text, transform=ax6.transAxes,
                fontsize=10, family='monospace', verticalalignment='top',
                bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))
        
        # 7. Recommendations (middle right, span 2 rows)
        ax7 = fig.add_subplot(gs[1:3, 2:])
        ax7.axis('off')
        
        recommendations_text = "SAFETY RECOMMENDATIONS\n" + "="*25 + "\n\n"
        
        if summary and "recommendations" in summary:
            for i, rec in enumerate(summary["recommendations"][:8], 1):
                recommendations_text += f"{i}. {rec}\n"
        else:
            recommendations_text += "No specific recommendations available\n"
        
        ax7.text(0.05, 0.95, recommendations_text, transform=ax7.transAxes,
                fontsize=10, family='monospace', verticalalignment='top',
                bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))
        
        # 8. Executive Summary (bottom row, full width)
        ax8 = fig.add_subplot(gs[3, :])
        ax8.axis('off')
        
        # Calculate overall metrics
        total_temporal = temporal.get("total_crashes", 0)
        total_weather = weather.get("total_crashes_analyzed", 0)
        
        # Get analysis period
        time_period = analysis_results.get("time_period", {})
        days_back = time_period.get("days_back", "N/A")
        analysis_date = time_period.get("analysis_date", "N/A")
        
        summary_text = f"""
        EXECUTIVE SUMMARY
        {'='*20}
        
        ANALYSIS OVERVIEW:
        • Analysis Date: {analysis_date}
        • Analysis Period: {days_back} days
        • Temporal Analysis: {total_temporal:,} crashes analyzed
        • Weather Analysis: {total_weather:,} crashes analyzed  
        
        KEY TAKEAWAYS:
        1. Temporal patterns reveal specific high-risk periods
        2. Weather conditions significantly impact crash frequency and severity
        
        ACTIONABLE INSIGHTS:
        • Target safety measures during identified peak periods
        • Implement weather-adaptive safety protocols
        """
        
        ax8.text(0.02, 0.98, summary_text, transform=ax8.transAxes,
                fontsize=11, family='monospace', verticalalignment='top',
                bbox=dict(boxstyle='round', facecolor='lightyellow', alpha=0.8))
        
        plt.tight_layout()
        
        filename = f"{self.output_dir}/comprehensive/executive_summary_{timestamp}.png"
        plt.savefig(filename, dpi=300, bbox_inches='tight')
        plt.close()
        
        return filename
