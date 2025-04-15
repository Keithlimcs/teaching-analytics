import pandas as pd
import numpy as np
import sqlite3
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st
from datetime import datetime, timedelta

class OpportunityAnalyzer:
    """
    A class to analyze opportunity pipeline and sales forecasting for the Teaching Organization Analytics application.
    Provides comprehensive analysis of sales pipeline, conversion rates, and revenue forecasts.
    """
    
    def __init__(self, db_path='data/teaching_analytics.db'):
        """Initialize the analyzer with database connection"""
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
    
    def get_pipeline_overview(self):
        """
        Get an overview of the sales pipeline
        
        Returns:
            dict: Overview statistics
        """
        try:
            # Get total opportunities
            query = "SELECT COUNT(*) FROM opportunities"
            total_opportunities = pd.read_sql(query, self.conn).iloc[0, 0]
            
            # Get stage distribution
            query = """
            SELECT stage, COUNT(*) as count, SUM(potential_revenue) as potential_revenue
            FROM opportunities 
            GROUP BY stage 
            ORDER BY 
                CASE stage
                    WHEN 'Lead' THEN 1
                    WHEN 'Prospect' THEN 2
                    WHEN 'Proposal' THEN 3
                    WHEN 'Negotiation' THEN 4
                    WHEN 'Closed Won' THEN 5
                    WHEN 'Closed Lost' THEN 6
                    ELSE 7
                END
            """
            stage_distribution = pd.read_sql(query, self.conn)
            
            # Get open vs closed opportunities
            query = """
            SELECT 
                CASE 
                    WHEN stage IN ('Closed Won', 'Closed Lost') THEN 'Closed'
                    ELSE 'Open'
                END as status,
                COUNT(*) as count,
                SUM(potential_revenue) as potential_revenue
            FROM opportunities
            GROUP BY status
            """
            open_vs_closed = pd.read_sql(query, self.conn)
            
            # Get win rate
            query = """
            SELECT 
                COUNT(CASE WHEN stage = 'Closed Won' THEN 1 END) as won_count,
                COUNT(CASE WHEN stage = 'Closed Lost' THEN 1 END) as lost_count,
                COUNT(CASE WHEN stage IN ('Closed Won', 'Closed Lost') THEN 1 END) as total_closed
            FROM opportunities
            """
            win_rate_data = pd.read_sql(query, self.conn)
            
            if win_rate_data['total_closed'].iloc[0] > 0:
                win_rate = (win_rate_data['won_count'].iloc[0] / win_rate_data['total_closed'].iloc[0]) * 100
            else:
                win_rate = 0
            
            # Get weighted pipeline value
            query = """
            SELECT 
                SUM(potential_revenue * (probability / 100)) as weighted_value,
                SUM(potential_revenue) as total_value
            FROM opportunities
            WHERE stage NOT IN ('Closed Won', 'Closed Lost')
            """
            pipeline_value = pd.read_sql(query, self.conn)
            
            # Get top opportunities
            query = """
            SELECT 
                o.opportunity_id,
                c.name as client_name,
                p.name as program_name,
                o.potential_revenue,
                o.stage,
                o.probability,
                o.expected_close_date
            FROM opportunities o
            JOIN clients c ON o.client_id = c.client_id
            JOIN programs p ON o.program_id = p.program_id
            WHERE o.stage NOT IN ('Closed Won', 'Closed Lost')
            ORDER BY o.potential_revenue DESC
            LIMIT 10
            """
            top_opportunities = pd.read_sql(query, self.conn)
            
            return {
                'total_opportunities': total_opportunities,
                'stage_distribution': stage_distribution,
                'open_vs_closed': open_vs_closed,
                'win_rate': win_rate,
                'pipeline_value': pipeline_value,
                'top_opportunities': top_opportunities
            }
        except Exception as e:
            return {'error': str(e)}
    
    def analyze_conversion_rates(self):
        """
        Analyze pipeline conversion rates
        
        Returns:
            dict: Conversion rate analysis
        """
        try:
            # Get stage counts
            query = """
            SELECT stage, COUNT(*) as count
            FROM opportunities 
            GROUP BY stage 
            ORDER BY 
                CASE stage
                    WHEN 'Lead' THEN 1
                    WHEN 'Prospect' THEN 2
                    WHEN 'Proposal' THEN 3
                    WHEN 'Negotiation' THEN 4
                    WHEN 'Closed Won' THEN 5
                    WHEN 'Closed Lost' THEN 6
                    ELSE 7
                END
            """
            stage_counts = pd.read_sql(query, self.conn)
            
            # Calculate conversion rates between stages
            if not stage_counts.empty:
                stage_order = ['Lead', 'Prospect', 'Proposal', 'Negotiation', 'Closed Won']
                conversion_rates = []
                
                for i in range(len(stage_order) - 1):
                    current_stage = stage_order[i]
                    next_stage = stage_order[i + 1]
                    
                    current_count = stage_counts[stage_counts['stage'] == current_stage]['count'].iloc[0] if current_stage in stage_counts['stage'].values else 0
                    next_count = stage_counts[stage_counts['stage'] == next_stage]['count'].iloc[0] if next_stage in stage_counts['stage'].values else 0
                    
                    if current_count > 0:
                        conversion_rate = (next_count / current_count) * 100
                    else:
                        conversion_rate = 0
                    
                    conversion_rates.append({
                        'from_stage': current_stage,
                        'to_stage': next_stage,
                        'conversion_rate': conversion_rate
                    })
                
                conversion_rates_df = pd.DataFrame(conversion_rates)
            else:
                conversion_rates_df = pd.DataFrame(columns=['from_stage', 'to_stage', 'conversion_rate'])
            
            # Get overall lead-to-win rate
            lead_count = stage_counts[stage_counts['stage'] == 'Lead']['count'].iloc[0] if 'Lead' in stage_counts['stage'].values else 0
            won_count = stage_counts[stage_counts['stage'] == 'Closed Won']['count'].iloc[0] if 'Closed Won' in stage_counts['stage'].values else 0
            
            if lead_count > 0:
                lead_to_win_rate = (won_count / lead_count) * 100
            else:
                lead_to_win_rate = 0
            
            # Get conversion by client industry
            query = """
            SELECT 
                c.industry,
                COUNT(CASE WHEN o.stage = 'Closed Won' THEN 1 END) as won_count,
                COUNT(CASE WHEN o.stage = 'Closed Lost' THEN 1 END) as lost_count,
                COUNT(*) as total_count
            FROM opportunities o
            JOIN clients c ON o.client_id = c.client_id
            WHERE c.industry IS NOT NULL
            GROUP BY c.industry
            """
            conversion_by_industry = pd.read_sql(query, self.conn)
            
            if not conversion_by_industry.empty:
                conversion_by_industry['win_rate'] = (conversion_by_industry['won_count'] / conversion_by_industry['total_count']) * 100
            
            # Get conversion by program category
            query = """
            SELECT 
                p.category,
                COUNT(CASE WHEN o.stage = 'Closed Won' THEN 1 END) as won_count,
                COUNT(CASE WHEN o.stage = 'Closed Lost' THEN 1 END) as lost_count,
                COUNT(*) as total_count
            FROM opportunities o
            JOIN programs p ON o.program_id = p.program_id
            WHERE p.category IS NOT NULL
            GROUP BY p.category
            """
            conversion_by_category = pd.read_sql(query, self.conn)
            
            if not conversion_by_category.empty:
                conversion_by_category['win_rate'] = (conversion_by_category['won_count'] / conversion_by_category['total_count']) * 100
            
            return {
                'stage_counts': stage_counts,
                'conversion_rates': conversion_rates_df,
                'lead_to_win_rate': lead_to_win_rate,
                'conversion_by_industry': conversion_by_industry,
                'conversion_by_category': conversion_by_category
            }
        except Exception as e:
            return {'error': str(e)}
    
    def analyze_sales_velocity(self):
        """
        Analyze sales velocity (how quickly opportunities move through the pipeline)
        
        Returns:
            dict: Sales velocity analysis
        """
        try:
            # Get average days in each stage
            query = """
            WITH stage_transitions AS (
                SELECT 
                    opportunity_id,
                    stage,
                    created_date as start_date,
                    CASE 
                        WHEN stage IN ('Closed Won', 'Closed Lost') THEN actual_close_date
                        ELSE last_updated
                    END as end_date
                FROM opportunities
            )
            SELECT 
                stage,
                AVG(julianday(end_date) - julianday(start_date)) as avg_days_in_stage,
                COUNT(*) as opportunity_count
            FROM stage_transitions
            WHERE start_date IS NOT NULL AND end_date IS NOT NULL
            GROUP BY stage
            ORDER BY 
                CASE stage
                    WHEN 'Lead' THEN 1
                    WHEN 'Prospect' THEN 2
                    WHEN 'Proposal' THEN 3
                    WHEN 'Negotiation' THEN 4
                    WHEN 'Closed Won' THEN 5
                    WHEN 'Closed Lost' THEN 6
                    ELSE 7
                END
            """
            days_in_stage = pd.read_sql(query, self.conn)
            
            # Get average days to close
            query = """
            SELECT 
                AVG(julianday(actual_close_date) - julianday(created_date)) as avg_days_to_close,
                COUNT(*) as opportunity_count
            FROM opportunities
            WHERE stage IN ('Closed Won', 'Closed Lost') 
                AND created_date IS NOT NULL 
                AND actual_close_date IS NOT NULL
            """
            days_to_close = pd.read_sql(query, self.conn)
            
            avg_days_to_close = days_to_close['avg_days_to_close'].iloc[0] if not days_to_close.empty and not pd.isna(days_to_close['avg_days_to_close'].iloc[0]) else 0
            
            # Get average days to close by outcome
            query = """
            SELECT 
                stage,
                AVG(julianday(actual_close_date) - julianday(created_date)) as avg_days_to_close,
                COUNT(*) as opportunity_count
            FROM opportunities
            WHERE stage IN ('Closed Won', 'Closed Lost') 
                AND created_date IS NOT NULL 
                AND actual_close_date IS NOT NULL
            GROUP BY stage
            """
            days_by_outcome = pd.read_sql(query, self.conn)
            
            # Get sales velocity by client industry
            query = """
            SELECT 
                c.industry,
                AVG(julianday(o.actual_close_date) - julianday(o.created_date)) as avg_days_to_close,
                COUNT(*) as opportunity_count
            FROM opportunities o
            JOIN clients c ON o.client_id = c.client_id
            WHERE o.stage IN ('Closed Won', 'Closed Lost') 
                AND o.created_date IS NOT NULL 
                AND o.actual_close_date IS NOT NULL
                AND c.industry IS NOT NULL
            GROUP BY c.industry
            """
            velocity_by_industry = pd.read_sql(query, self.conn)
            
            # Get sales velocity by program category
            query = """
            SELECT 
                p.category,
                AVG(julianday(o.actual_close_date) - julianday(o.created_date)) as avg_days_to_close,
                COUNT(*) as opportunity_count
            FROM opportunities o
            JOIN programs p ON o.program_id = p.program_id
            WHERE o.stage IN ('Closed Won', 'Closed Lost') 
                AND o.created_date IS NOT NULL 
                AND o.actual_close_date IS NOT NULL
                AND p.category IS NOT NULL
            GROUP BY p.category
            """
            velocity_by_category = pd.read_sql(query, self.conn)
            
            return {
                'days_in_stage': days_in_stage,
                'avg_days_to_close': avg_days_to_close,
                'days_by_outcome': days_by_outcome,
                'velocity_by_industry': velocity_by_industry,
                'velocity_by_category': velocity_by_category
            }
        except Exception as e:
            return {'error': str(e)}
    
    def generate_sales_forecast(self, forecast_periods=3, period_type='month'):
        """
        Generate sales forecast based on pipeline and historical data
        
        Args:
            forecast_periods: Number of periods to forecast
            period_type: Type of period ('month', 'quarter', 'year')
            
        Returns:
            dict: Sales forecast
        """
        try:
            # Get current date
            current_date = datetime.now()
            
            # Define period start and end dates
            period_dates = []
            for i in range(forecast_periods):
                if period_type == 'month':
                    start_date = current_date + timedelta(days=30 * i)
                    end_date = current_date + timedelta(days=30 * (i + 1))
                elif period_type == 'quarter':
                    start_date = current_date + timedelta(days=90 * i)
                    end_date = current_date + timedelta(days=90 * (i + 1))
                elif period_type == 'year':
                    start_date = current_date + timedelta(days=365 * i)
                    end_date = current_date + timedelta(days=365 * (i + 1))
                
                period_dates.append({
                    'period': i + 1,
                    'start_date': start_date.strftime('%Y-%m-%d'),
                    'end_date': end_date.strftime('%Y-%m-%d')
                })
            
            # Get weighted pipeline value for each period
            pipeline_forecast = []
            for period in period_dates:
                query = f"""
                SELECT 
                    SUM(potential_revenue * (probability / 100)) as weighted_value,
                    SUM(potential_revenue) as total_value,
                    COUNT(*) as opportunity_count
                FROM opportunities
                WHERE stage NOT IN ('Closed Won', 'Closed Lost')
                    AND expected_close_date >= '{period['start_date']}'
                    AND expected_close_date < '{period['end_date']}'
                """
                period_data = pd.read_sql(query, self.conn)
                
                pipeline_forecast.append({
                    'period': period['period'],
                    'period_name': f"Period {period['period']}",
                    'start_date': period['start_date'],
                    'end_date': period['end_date'],
                    'weighted_value': period_data['weighted_value'].iloc[0] if not pd.isna(period_data['weighted_value'].iloc[0]) else 0,
                    'total_value': period_data['total_value'].iloc[0] if not pd.isna(period_data['total_value'].iloc[0]) else 0,
                    'opportunity_count': period_data['opportunity_count'].iloc[0]
                })
            
            pipeline_forecast_df = pd.DataFrame(pipeline_forecast)
            
            # Get historical win rate
            query = """
            SELECT 
                COUNT(CASE WHEN stage = 'Closed Won' THEN 1 END) as won_count,
                COUNT(CASE WHEN stage IN ('Closed Won', 'Closed Lost') THEN 1 END) as total_closed
            FROM opportunities
            WHERE actual_close_date IS NOT NULL
            """
            win_rate_data = pd.read_sql(query, self.conn)
            
            if win_rate_data['total_closed'].iloc[0] > 0:
                historical_win_rate = (win_rate_data['won_count'].iloc[0] / win_rate_data['total_closed'].iloc[0])
            else:
                historical_win_rate = 0
            
            # Get historical average deal size
            query = """
            SELECT AVG(potential_revenue) as avg_deal_size
            FROM opportunities
            WHERE stage = 'Closed Won'
            """
            deal_size_data = pd.read_sql(query, self.conn)
            
            historical_avg_deal_size = deal_size_data['avg_deal_size'].iloc[0] if not deal_size_data.empty and not pd.isna(deal_size_data['avg_deal_size'].iloc[0]) else 0
            
            # Get historical sales by period
            if period_type == 'month':
                date_format = '%Y-%m'
                group_by = "strftime('%Y-%m', actual_close_date)"
            elif period_type == 'quarter':
                date_format = '%Y-Q%Q'
                group_by = "strftime('%Y-Q', actual_close_date) || ((strftime('%m', actual_close_date) + 2) / 3)"
            elif period_type == 'year':
                date_format = '%Y'
                group_by = "strftime('%Y', actual_close_date)"
            
            query = f"""
            SELECT 
                {group_by} as period,
                SUM(potential_revenue) as total_revenue,
                COUNT(*) as deal_count
            FROM opportunities
            WHERE stage = 'Closed Won' AND actual_close_date IS NOT NULL
            GROUP BY period
            ORDER BY period
            """
            historical_sales = pd.read_sql(query, self.conn)
            
            # Calculate forecast based on historical data and pipeline
            forecast = []
            for period in pipeline_forecast:
                # Pipeline-based forecast
                pipeline_forecast_value = period['weighted_value']
                
                # Historical-based forecast (simple average of past periods)
                if not historical_sales.empty:
                    historical_avg = historical_sales['total_revenue'].mean()
                else:
                    historical_avg = 0
                
                # Combined forecast (weighted average)
                if pipeline_forecast_value > 0 and historical_avg > 0:
                    combined_forecast = (pipeline_forecast_value * 0.7) + (historical_avg * 0.3)
                elif pipeline_forecast_value > 0:
                    combined_forecast = pipeline_forecast_value
                else:
                    combined_forecast = historical_avg
                
                forecast.append({
                    'period': period['period'],
                    'period_name': f"Period {period['period']}",
                    'start_date': period['start_date'],
                    'end_date': period['end_date'],
                    'pipeline_forecast': pipeline_forecast_value,
                    'historical_forecast': historical_avg,
                    'combined_forecast': combined_forecast
                })
            
            forecast_df = pd.DataFrame(forecast)
            
            return {
                'pipeline_forecast': pipeline_forecast_df,
                'historical_win_rate': historical_win_rate,
                'historical_avg_deal_size': historical_avg_deal_size,
                'historical_sales': historical_sales,
                'forecast': forecast_df
            }
        except Exception as e:
            return {'error': str(e)}
    
    def analyze_pipeline_trends(self):
        """
        Analyze pipeline trends over time
        
        Returns:
            dict: Pipeline trends analysis
        """
        try:
            # Get pipeline value over time
            query = """
            SELECT 
                strftime('%Y-%m', created_date) as month,
                SUM(CASE WHEN stage NOT IN ('Closed Won', 'Closed Lost') THEN potential_revenue * (probability / 100) ELSE 0 END) as weighted_pipeline,
                SUM(CASE WHEN stage NOT IN ('Closed Won', 'Closed Lost') THEN potential_revenue ELSE 0 END) as total_pipeline,
                COUNT(CASE WHEN stage NOT IN ('Closed Won', 'Closed Lost') THEN 1 END) as open_opportunities,
                SUM(CASE WHEN stage = 'Closed Won' THEN potential_revenue ELSE 0 END) as closed_won_value,
                COUNT(CASE WHEN stage = 'Closed Won' THEN 1 END) as closed_won_count
            FROM opportunities
            WHERE created_date IS NOT NULL
            GROUP BY month
            ORDER BY month
            """
            pipeline_over_time = pd.read_sql(query, self.conn)
            
            # Get win rate over time
            query = """
            SELECT 
                strftime('%Y-%m', actual_close_date) as month,
                COUNT(CASE WHEN stage = 'Closed Won' THEN 1 END) as won_count,
                COUNT(CASE WHEN stage = 'Closed Lost' THEN 1 END) as lost_count,
                COUNT(CASE WHEN stage IN ('Closed Won', 'Closed Lost') THEN 1 END) as total_closed
            FROM opportunities
            WHERE actual_close_date IS NOT NULL
            GROUP BY month
            ORDER BY month
            """
            win_rate_over_time = pd.read_sql(query, self.conn)
            
            if not win_rate_over_time.empty:
                win_rate_over_time['win_rate'] = (win_rate_over_time['won_count'] / win_rate_over_time['total_closed']) * 100
            
            # Get average deal size over time
            query = """
            SELECT 
                strftime('%Y-%m', actual_close_date) as month,
                AVG(potential_revenue) as avg_deal_size,
                COUNT(*) as deal_count
            FROM opportunities
            WHERE stage = 'Closed Won' AND actual_close_date IS NOT NULL
            GROUP BY month
            ORDER BY month
            """
            deal_size_over_time = pd.read_sql(query, self.conn)
            
            # Get stage distribution over time
            query = """
            SELECT 
                strftime('%Y-%m', created_date) as month,
                stage,
                COUNT(*) as opportunity_count
            FROM opportunities
            WHERE created_date IS NOT NULL
            GROUP BY month, stage
            ORDER BY month, 
                CASE stage
                    WHEN 'Lead' THEN 1
                    WHEN 'Prospect' THEN 2
                    WHEN 'Proposal' THEN 3
                    WHEN 'Negotiation' THEN 4
                    WHEN 'Closed Won' THEN 5
                    WHEN 'Closed Lost' THEN 6
                    ELSE 7
                END
            """
            stage_distribution_over_time = pd.read_sql(query, self.conn)
            
            return {
                'pipeline_over_time': pipeline_over_time,
                'win_rate_over_time': win_rate_over_time,
                'deal_size_over_time': deal_size_over_time,
                'stage_distribution_over_time': stage_distribution_over_time
            }
        except Exception as e:
            return {'error': str(e)}
    
    def create_pipeline_funnel_chart(self, data=None):
        """
        Create a pipeline funnel chart
        
        Args:
            data: Optional pre-loaded data dictionary
            
        Returns:
            plotly.graph_objects.Figure: Pipeline funnel chart
        """
        if data is None:
            overview = self.get_pipeline_overview()
        else:
            overview = data
        
        if 'error' in overview:
            return None
        
        # Create pipeline funnel chart
        if 'stage_distribution' in overview and not overview['stage_distribution'].empty:
            # Define stage order
            stage_order = ['Lead', 'Prospect', 'Proposal', 'Negotiation', 'Closed Won', 'Closed Lost']
            
            # Filter and sort data
            df = overview['stage_distribution'].copy()
            df = df[df['stage'].isin(stage_order)]
            
            # Create custom order for display (excluding Closed Lost at the bottom)
            display_order = ['Lead', 'Prospect', 'Proposal', 'Negotiation', 'Closed Won']
            display_df = df[df['stage'].isin(display_order)].copy()
            display_df['stage'] = pd.Categorical(display_df['stage'], categories=display_order, ordered=True)
            display_df = display_df.sort_values('stage', ascending=False)
            
            fig = go.Figure(go.Funnel(
                y=display_df['stage'],
                x=display_df['count'],
                textposition="inside",
                textinfo="value+percent initial",
                opacity=0.8,
                marker={"color": ["#4169E1", "#3CB371", "#FFD700", "#FF8C00", "#32CD32"]},
                connector={"line": {"color": "royalblue", "width": 1}}
            ))
            
            fig.update_layout(
                title="Sales Pipeline Funnel",
                font=dict(size=14)
            )
            
            return fig
        
        return None
    
    def create_pipeline_value_chart(self, data=None):
        """
        Create a pipeline value chart
        
        Args:
            data: Optional pre-loaded data dictionary
            
        Returns:
            plotly.graph_objects.Figure: Pipeline value chart
        """
        if data is None:
            overview = self.get_pipeline_overview()
        else:
            overview = data
        
        if 'error' in overview:
            return None
        
        # Create pipeline value chart
        if 'stage_distribution' in overview and not overview['stage_distribution'].empty:
            # Define stage order
            stage_order = ['Lead', 'Prospect', 'Proposal', 'Negotiation', 'Closed Won']
            
            # Filter and sort data
            df = overview['stage_distribution'].copy()
            df = df[df['stage'].isin(stage_order)]
            df['stage'] = pd.Categorical(df['stage'], categories=stage_order, ordered=True)
            df = df.sort_values('stage')
            
            fig = px.bar(
                df,
                x='stage',
                y='potential_revenue',
                title='Pipeline Value by Stage',
                color='stage',
                text_auto=True
            )
            fig.update_traces(texttemplate='$%{y:,.0f}', textposition='outside')
            fig.update_layout(xaxis_title='Stage', yaxis_title='Potential Revenue ($)')
            return fig
        
        return None
    
    def create_win_rate_chart(self, data=None):
        """
        Create a win rate chart
        
        Args:
            data: Optional pre-loaded data dictionary
            
        Returns:
            plotly.graph_objects.Figure: Win rate chart
        """
        if data is None:
            conversion_data = self.analyze_conversion_rates()
        else:
            conversion_data = data
        
        if 'error' in conversion_data:
            return None
        
        # Create win rate by industry chart
        if 'conversion_by_industry' in conversion_data and not conversion_data['conversion_by_industry'].empty:
            df = conversion_data['conversion_by_industry'].sort_values('win_rate', ascending=False)
            
            fig = px.bar(
                df,
                x='industry',
                y='win_rate',
                title='Win Rate by Industry',
                color='win_rate',
                color_continuous_scale='RdYlGn',
                text_auto=True
            )
            fig.update_traces(texttemplate='%{y:.1f}%', textposition='outside')
            fig.update_layout(xaxis_title='Industry', yaxis_title='Win Rate (%)')
            return fig
        
        return None
    
    def create_conversion_rates_chart(self, data=None):
        """
        Create a conversion rates chart
        
        Args:
            data: Optional pre-loaded data dictionary
            
        Returns:
            plotly.graph_objects.Figure: Conversion rates chart
        """
        if data is None:
            conversion_data = self.analyze_conversion_rates()
        else:
            conversion_data = data
        
        if 'error' in conversion_data:
            return None
        
        # Create conversion rates chart
        if 'conversion_rates' in conversion_data and not conversion_data['conversion_rates'].empty:
            df = conversion_data['conversion_rates']
            
            fig = px.bar(
                df,
                x='from_stage',
                y='conversion_rate',
                title='Stage-to-Stage Conversion Rates',
                color='conversion_rate',
                color_continuous_scale='RdYlGn',
                text_auto=True,
                hover_data=['to_stage']
            )
            fig.update_traces(texttemplate='%{y:.1f}%', textposition='outside')
            fig.update_layout(
                xaxis_title='From Stage',
                yaxis_title='Conversion Rate (%)',
                xaxis={'categoryorder': 'array', 'categoryarray': ['Lead', 'Prospect', 'Proposal', 'Negotiation']}
            )
            return fig
        
        return None
    
    def create_sales_velocity_chart(self, data=None):
        """
        Create a sales velocity chart
        
        Args:
            data: Optional pre-loaded data dictionary
            
        Returns:
            plotly.graph_objects.Figure: Sales velocity chart
        """
        if data is None:
            velocity_data = self.analyze_sales_velocity()
        else:
            velocity_data = data
        
        if 'error' in velocity_data:
            return None
        
        # Create days in stage chart
        if 'days_in_stage' in velocity_data and not velocity_data['days_in_stage'].empty:
            df = velocity_data['days_in_stage']
            
            fig = px.bar(
                df,
                x='stage',
                y='avg_days_in_stage',
                title='Average Days in Each Pipeline Stage',
                color='stage',
                text_auto=True
            )
            fig.update_traces(texttemplate='%{y:.1f} days', textposition='outside')
            fig.update_layout(
                xaxis_title='Stage',
                yaxis_title='Average Days',
                xaxis={'categoryorder': 'array', 'categoryarray': ['Lead', 'Prospect', 'Proposal', 'Negotiation', 'Closed Won', 'Closed Lost']}
            )
            return fig
        
        return None
    
    def create_forecast_chart(self, data=None, forecast_periods=3):
        """
        Create a sales forecast chart
        
        Args:
            data: Optional pre-loaded data dictionary
            forecast_periods: Number of periods to forecast
            
        Returns:
            plotly.graph_objects.Figure: Forecast chart
        """
        if data is None:
            forecast_data = self.generate_sales_forecast(forecast_periods=forecast_periods)
        else:
            forecast_data = data
        
        if 'error' in forecast_data:
            return None
        
        # Create forecast chart
        if 'forecast' in forecast_data and not forecast_data['forecast'].empty:
            df = forecast_data['forecast']
            
            fig = go.Figure()
            
            # Add pipeline forecast bar
            fig.add_trace(go.Bar(
                x=df['period_name'],
                y=df['pipeline_forecast'],
                name='Pipeline Forecast',
                marker_color='rgb(55, 83, 109)'
            ))
            
            # Add historical forecast bar
            fig.add_trace(go.Bar(
                x=df['period_name'],
                y=df['historical_forecast'],
                name='Historical Forecast',
                marker_color='rgb(26, 118, 255)'
            ))
            
            # Add combined forecast line
            fig.add_trace(go.Scatter(
                x=df['period_name'],
                y=df['combined_forecast'],
                name='Combined Forecast',
                mode='lines+markers',
                line=dict(color='rgb(219, 64, 82)', width=3),
                marker=dict(size=8)
            ))
            
            fig.update_layout(
                title='Sales Forecast by Period',
                xaxis_title='Period',
                yaxis_title='Forecast Revenue ($)',
                barmode='group',
                legend=dict(
                    orientation="h",
                    yanchor="bottom",
                    y=1.02,
                    xanchor="right",
                    x=1
                )
            )
            
            return fig
        
        return None
    
    def create_pipeline_trends_chart(self, data=None):
        """
        Create a pipeline trends chart
        
        Args:
            data: Optional pre-loaded data dictionary
            
        Returns:
            plotly.graph_objects.Figure: Pipeline trends chart
        """
        if data is None:
            trends_data = self.analyze_pipeline_trends()
        else:
            trends_data = data
        
        if 'error' in trends_data:
            return None
        
        # Create pipeline trends chart
        if 'pipeline_over_time' in trends_data and not trends_data['pipeline_over_time'].empty:
            df = trends_data['pipeline_over_time']
            
            # Create figure with secondary y-axis
            fig = make_subplots(specs=[[{"secondary_y": True}]])
            
            # Add weighted pipeline bar chart
            fig.add_trace(
                go.Bar(
                    x=df['month'],
                    y=df['weighted_pipeline'],
                    name='Weighted Pipeline',
                    marker_color='rgb(55, 83, 109)'
                ),
                secondary_y=False
            )
            
            # Add closed won value line chart
            fig.add_trace(
                go.Scatter(
                    x=df['month'],
                    y=df['closed_won_value'],
                    name='Closed Won Value',
                    marker_color='rgb(26, 118, 255)',
                    mode='lines+markers'
                ),
                secondary_y=False
            )
            
            # Add open opportunities line chart
            fig.add_trace(
                go.Scatter(
                    x=df['month'],
                    y=df['open_opportunities'],
                    name='Open Opportunities',
                    marker_color='rgb(219, 64, 82)',
                    mode='lines+markers'
                ),
                secondary_y=True
            )
            
            # Update layout
            fig.update_layout(
                title='Pipeline Trends Over Time',
                xaxis_title='Month',
                legend=dict(
                    orientation="h",
                    yanchor="bottom",
                    y=1.02,
                    xanchor="right",
                    x=1
                )
            )
            
            # Set y-axes titles
            fig.update_yaxes(title_text="Revenue ($)", secondary_y=False)
            fig.update_yaxes(title_text="Number of Opportunities", secondary_y=True)
            
            return fig
        
        return None
    
    def close(self):
        """Close the database connection"""
        if hasattr(self, 'conn') and self.conn:
            self.conn.close()


# Example usage:
if __name__ == "__main__":
    analyzer = OpportunityAnalyzer()
    
    # Example: Get pipeline overview
    overview = analyzer.get_pipeline_overview()
    print(f"Total opportunities: {overview.get('total_opportunities', 0)}")
    
    # Example: Create a chart
    fig = analyzer.create_pipeline_funnel_chart()
    if fig:
        fig.show()
    
    analyzer.close()
