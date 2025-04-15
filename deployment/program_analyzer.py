import pandas as pd
import numpy as np
import sqlite3
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

class ProgramAnalyzer:
    """
    A class to analyze program popularity for the Teaching Organization Analytics application.
    Provides comprehensive analysis of program performance, popularity, and trends.
    """
    
    def __init__(self, db_path='data/teaching_analytics.db'):
        """Initialize the analyzer with database connection"""
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
    
    def get_program_overview(self):
        """
        Get an overview of program data
        
        Returns:
            dict: Overview statistics
        """
        try:
            # Get total programs
            query = "SELECT COUNT(*) FROM programs"
            total_programs = pd.read_sql(query, self.conn).iloc[0, 0]
            
            # Get category distribution
            query = """
            SELECT category, COUNT(*) as count 
            FROM programs 
            WHERE category IS NOT NULL 
            GROUP BY category 
            ORDER BY count DESC
            """
            category_distribution = pd.read_sql(query, self.conn)
            
            # Get delivery mode distribution
            query = """
            SELECT delivery_mode, COUNT(*) as count 
            FROM programs 
            WHERE delivery_mode IS NOT NULL 
            GROUP BY delivery_mode 
            ORDER BY count DESC
            """
            delivery_mode_distribution = pd.read_sql(query, self.conn)
            
            # Get duration distribution
            query = """
            SELECT 
                CASE
                    WHEN duration <= 8 THEN '1 Day or Less'
                    WHEN duration <= 16 THEN '2 Days'
                    WHEN duration <= 24 THEN '3 Days'
                    WHEN duration <= 40 THEN '1 Week'
                    ELSE 'More than 1 Week'
                END as duration_category,
                COUNT(*) as count
            FROM programs
            WHERE duration IS NOT NULL
            GROUP BY duration_category
            ORDER BY 
                CASE duration_category
                    WHEN '1 Day or Less' THEN 1
                    WHEN '2 Days' THEN 2
                    WHEN '3 Days' THEN 3
                    WHEN '1 Week' THEN 4
                    ELSE 5
                END
            """
            duration_distribution = pd.read_sql(query, self.conn)
            
            # Get price distribution
            query = """
            SELECT 
                CASE
                    WHEN base_price < 500 THEN 'Under $500'
                    WHEN base_price < 1000 THEN '$500-$999'
                    WHEN base_price < 1500 THEN '$1000-$1499'
                    WHEN base_price < 2000 THEN '$1500-$1999'
                    ELSE '$2000+'
                END as price_category,
                COUNT(*) as count
            FROM programs
            WHERE base_price IS NOT NULL
            GROUP BY price_category
            ORDER BY 
                CASE price_category
                    WHEN 'Under $500' THEN 1
                    WHEN '$500-$999' THEN 2
                    WHEN '$1000-$1499' THEN 3
                    WHEN '$1500-$1999' THEN 4
                    ELSE 5
                END
            """
            price_distribution = pd.read_sql(query, self.conn)
            
            # Get top programs by enrollment
            query = """
            SELECT 
                p.program_id,
                p.name,
                p.category,
                p.delivery_mode,
                COUNT(e.enrollment_id) as enrollment_count,
                SUM(e.revenue) as total_revenue,
                SUM(e.num_participants) as total_participants
            FROM programs p
            LEFT JOIN enrollments e ON p.program_id = e.program_id
            GROUP BY p.program_id
            ORDER BY enrollment_count DESC
            LIMIT 10
            """
            top_programs = pd.read_sql(query, self.conn)
            
            return {
                'total_programs': total_programs,
                'category_distribution': category_distribution,
                'delivery_mode_distribution': delivery_mode_distribution,
                'duration_distribution': duration_distribution,
                'price_distribution': price_distribution,
                'top_programs': top_programs
            }
        except Exception as e:
            return {'error': str(e)}
    
    def analyze_program_popularity(self):
        """
        Analyze program popularity
        
        Returns:
            dict: Program popularity analysis
        """
        try:
            # Get program popularity by enrollment count
            query = """
            SELECT 
                p.program_id,
                p.name,
                p.category,
                p.delivery_mode,
                COUNT(e.enrollment_id) as enrollment_count,
                SUM(e.revenue) as total_revenue,
                SUM(e.num_participants) as total_participants,
                AVG(e.feedback_score) as avg_feedback
            FROM programs p
            LEFT JOIN enrollments e ON p.program_id = e.program_id
            GROUP BY p.program_id
            ORDER BY enrollment_count DESC
            """
            popularity_by_enrollment = pd.read_sql(query, self.conn)
            
            # Get program popularity by revenue
            query = """
            SELECT 
                p.program_id,
                p.name,
                p.category,
                p.delivery_mode,
                COUNT(e.enrollment_id) as enrollment_count,
                SUM(e.revenue) as total_revenue,
                SUM(e.num_participants) as total_participants,
                AVG(e.feedback_score) as avg_feedback
            FROM programs p
            LEFT JOIN enrollments e ON p.program_id = e.program_id
            GROUP BY p.program_id
            ORDER BY total_revenue DESC
            """
            popularity_by_revenue = pd.read_sql(query, self.conn)
            
            # Get program popularity by feedback score
            query = """
            SELECT 
                p.program_id,
                p.name,
                p.category,
                p.delivery_mode,
                COUNT(e.enrollment_id) as enrollment_count,
                SUM(e.revenue) as total_revenue,
                SUM(e.num_participants) as total_participants,
                AVG(e.feedback_score) as avg_feedback
            FROM programs p
            JOIN enrollments e ON p.program_id = e.program_id
            WHERE e.feedback_score IS NOT NULL
            GROUP BY p.program_id
            HAVING COUNT(e.enrollment_id) >= 3
            ORDER BY avg_feedback DESC
            """
            popularity_by_feedback = pd.read_sql(query, self.conn)
            
            # Get category popularity
            query = """
            SELECT 
                p.category,
                COUNT(e.enrollment_id) as enrollment_count,
                SUM(e.revenue) as total_revenue,
                SUM(e.num_participants) as total_participants,
                AVG(e.feedback_score) as avg_feedback
            FROM programs p
            JOIN enrollments e ON p.program_id = e.program_id
            WHERE p.category IS NOT NULL
            GROUP BY p.category
            ORDER BY enrollment_count DESC
            """
            category_popularity = pd.read_sql(query, self.conn)
            
            # Get delivery mode popularity
            query = """
            SELECT 
                p.delivery_mode,
                COUNT(e.enrollment_id) as enrollment_count,
                SUM(e.revenue) as total_revenue,
                SUM(e.num_participants) as total_participants,
                AVG(e.feedback_score) as avg_feedback
            FROM programs p
            JOIN enrollments e ON p.program_id = e.program_id
            WHERE p.delivery_mode IS NOT NULL
            GROUP BY p.delivery_mode
            ORDER BY enrollment_count DESC
            """
            delivery_mode_popularity = pd.read_sql(query, self.conn)
            
            return {
                'popularity_by_enrollment': popularity_by_enrollment,
                'popularity_by_revenue': popularity_by_revenue,
                'popularity_by_feedback': popularity_by_feedback,
                'category_popularity': category_popularity,
                'delivery_mode_popularity': delivery_mode_popularity
            }
        except Exception as e:
            return {'error': str(e)}
    
    def analyze_program_trends(self):
        """
        Analyze program trends over time
        
        Returns:
            dict: Program trends analysis
        """
        try:
            # Get enrollment trends over time
            query = """
            SELECT 
                strftime('%Y-%m', e.start_date) as month,
                COUNT(e.enrollment_id) as enrollment_count,
                SUM(e.revenue) as total_revenue,
                SUM(e.num_participants) as total_participants
            FROM enrollments e
            WHERE e.start_date IS NOT NULL
            GROUP BY month
            ORDER BY month
            """
            enrollment_trends = pd.read_sql(query, self.conn)
            
            # Get category trends over time
            query = """
            SELECT 
                strftime('%Y-%m', e.start_date) as month,
                p.category,
                COUNT(e.enrollment_id) as enrollment_count,
                SUM(e.revenue) as total_revenue
            FROM enrollments e
            JOIN programs p ON e.program_id = p.program_id
            WHERE e.start_date IS NOT NULL AND p.category IS NOT NULL
            GROUP BY month, p.category
            ORDER BY month, p.category
            """
            category_trends = pd.read_sql(query, self.conn)
            
            # Get delivery mode trends over time
            query = """
            SELECT 
                strftime('%Y-%m', e.start_date) as month,
                e.delivery_mode,
                COUNT(e.enrollment_id) as enrollment_count,
                SUM(e.revenue) as total_revenue
            FROM enrollments e
            WHERE e.start_date IS NOT NULL AND e.delivery_mode IS NOT NULL
            GROUP BY month, e.delivery_mode
            ORDER BY month, e.delivery_mode
            """
            delivery_mode_trends = pd.read_sql(query, self.conn)
            
            # Get feedback trends over time
            query = """
            SELECT 
                strftime('%Y-%m', e.start_date) as month,
                AVG(e.feedback_score) as avg_feedback,
                COUNT(e.enrollment_id) as enrollment_count
            FROM enrollments e
            WHERE e.start_date IS NOT NULL AND e.feedback_score IS NOT NULL
            GROUP BY month
            ORDER BY month
            """
            feedback_trends = pd.read_sql(query, self.conn)
            
            return {
                'enrollment_trends': enrollment_trends,
                'category_trends': category_trends,
                'delivery_mode_trends': delivery_mode_trends,
                'feedback_trends': feedback_trends
            }
        except Exception as e:
            return {'error': str(e)}
    
    def analyze_program_profitability(self):
        """
        Analyze program profitability
        
        Returns:
            dict: Program profitability analysis
        """
        try:
            # Get program profitability
            query = """
            SELECT 
                p.program_id,
                p.name,
                p.category,
                p.delivery_mode,
                COUNT(e.enrollment_id) as enrollment_count,
                SUM(e.revenue) as total_revenue,
                SUM(e.trainer_cost + e.logistics_cost + e.venue_cost + e.utilities_cost + e.materials_cost) as total_cost,
                SUM(e.revenue - (e.trainer_cost + e.logistics_cost + e.venue_cost + e.utilities_cost + e.materials_cost)) as total_profit,
                CASE 
                    WHEN SUM(e.revenue) > 0 
                    THEN (SUM(e.revenue - (e.trainer_cost + e.logistics_cost + e.venue_cost + e.utilities_cost + e.materials_cost)) / SUM(e.revenue)) * 100 
                    ELSE 0 
                END as profit_margin
            FROM programs p
            JOIN enrollments e ON p.program_id = e.program_id
            GROUP BY p.program_id
            ORDER BY profit_margin DESC
            """
            program_profitability = pd.read_sql(query, self.conn)
            
            # Get category profitability
            query = """
            SELECT 
                p.category,
                COUNT(e.enrollment_id) as enrollment_count,
                SUM(e.revenue) as total_revenue,
                SUM(e.trainer_cost + e.logistics_cost + e.venue_cost + e.utilities_cost + e.materials_cost) as total_cost,
                SUM(e.revenue - (e.trainer_cost + e.logistics_cost + e.venue_cost + e.utilities_cost + e.materials_cost)) as total_profit,
                CASE 
                    WHEN SUM(e.revenue) > 0 
                    THEN (SUM(e.revenue - (e.trainer_cost + e.logistics_cost + e.venue_cost + e.utilities_cost + e.materials_cost)) / SUM(e.revenue)) * 100 
                    ELSE 0 
                END as profit_margin
            FROM programs p
            JOIN enrollments e ON p.program_id = e.program_id
            WHERE p.category IS NOT NULL
            GROUP BY p.category
            ORDER BY profit_margin DESC
            """
            category_profitability = pd.read_sql(query, self.conn)
            
            # Get delivery mode profitability
            query = """
            SELECT 
                e.delivery_mode,
                COUNT(e.enrollment_id) as enrollment_count,
                SUM(e.revenue) as total_revenue,
                SUM(e.trainer_cost + e.logistics_cost + e.venue_cost + e.utilities_cost + e.materials_cost) as total_cost,
                SUM(e.revenue - (e.trainer_cost + e.logistics_cost + e.venue_cost + e.utilities_cost + e.materials_cost)) as total_profit,
                CASE 
                    WHEN SUM(e.revenue) > 0 
                    THEN (SUM(e.revenue - (e.trainer_cost + e.logistics_cost + e.venue_cost + e.utilities_cost + e.materials_cost)) / SUM(e.revenue)) * 100 
                    ELSE 0 
                END as profit_margin
            FROM enrollments e
            WHERE e.delivery_mode IS NOT NULL
            GROUP BY e.delivery_mode
            ORDER BY profit_margin DESC
            """
            delivery_mode_profitability = pd.read_sql(query, self.conn)
            
            # Get cost breakdown
            query = """
            SELECT 
                'Trainer Cost' as cost_type,
                SUM(e.trainer_cost) as total_cost,
                (SUM(e.trainer_cost) / SUM(e.trainer_cost + e.logistics_cost + e.venue_cost + e.utilities_cost + e.materials_cost)) * 100 as percentage
            FROM enrollments e
            UNION ALL
            SELECT 
                'Logistics Cost' as cost_type,
                SUM(e.logistics_cost) as total_cost,
                (SUM(e.logistics_cost) / SUM(e.trainer_cost + e.logistics_cost + e.venue_cost + e.utilities_cost + e.materials_cost)) * 100 as percentage
            FROM enrollments e
            UNION ALL
            SELECT 
                'Venue Cost' as cost_type,
                SUM(e.venue_cost) as total_cost,
                (SUM(e.venue_cost) / SUM(e.trainer_cost + e.logistics_cost + e.venue_cost + e.utilities_cost + e.materials_cost)) * 100 as percentage
            FROM enrollments e
            UNION ALL
            SELECT 
                'Utilities Cost' as cost_type,
                SUM(e.utilities_cost) as total_cost,
                (SUM(e.utilities_cost) / SUM(e.trainer_cost + e.logistics_cost + e.venue_cost + e.utilities_cost + e.materials_cost)) * 100 as percentage
            FROM enrollments e
            UNION ALL
            SELECT 
                'Materials Cost' as cost_type,
                SUM(e.materials_cost) as total_cost,
                (SUM(e.materials_cost) / SUM(e.trainer_cost + e.logistics_cost + e.venue_cost + e.utilities_cost + e.materials_cost)) * 100 as percentage
            FROM enrollments e
            ORDER BY total_cost DESC
            """
            cost_breakdown = pd.read_sql(query, self.conn)
            
            return {
                'program_profitability': program_profitability,
                'category_profitability': category_profitability,
                'delivery_mode_profitability': delivery_mode_profitability,
                'cost_breakdown': cost_breakdown
            }
        except Exception as e:
            return {'error': str(e)}
    
    def get_program_details(self, program_id):
        """
        Get detailed information for a specific program
        
        Args:
            program_id: ID of the program to analyze
            
        Returns:
            dict: Program details
        """
        try:
            # Get program information
            query = f"SELECT * FROM programs WHERE program_id = {program_id}"
            program_info = pd.read_sql(query, self.conn)
            
            if program_info.empty:
                return {'error': f"Program with ID {program_id} not found"}
            
            # Get enrollment history
            query = f"""
            SELECT 
                e.enrollment_id,
                c.name as client_name,
                c.industry as client_industry,
                e.start_date,
                e.end_date,
                e.delivery_mode,
                e.num_participants,
                e.revenue,
                e.trainer_cost + e.logistics_cost + e.venue_cost + e.utilities_cost + e.materials_cost as total_cost,
                e.revenue - (e.trainer_cost + e.logistics_cost + e.venue_cost + e.utilities_cost + e.materials_cost) as profit,
                e.status,
                e.feedback_score
            FROM enrollments e
            JOIN clients c ON e.client_id = c.client_id
            WHERE e.program_id = {program_id}
            ORDER BY e.start_date DESC
            """
            enrollment_history = pd.read_sql(query, self.conn)
            
            # Get opportunity history
            query = f"""
            SELECT 
                o.opportunity_id,
                c.name as client_name,
                c.industry as client_industry,
                o.potential_revenue,
                o.estimated_participants,
                o.stage,
                o.probability,
                o.expected_close_date,
                o.actual_close_date,
                o.created_date,
                o.owner
            FROM opportunities o
            JOIN clients c ON o.client_id = c.client_id
            WHERE o.program_id = {program_id}
            ORDER BY o.created_date DESC
            """
            opportunity_history = pd.read_sql(query, self.conn)
            
            # Calculate enrollments over time
            if not enrollment_history.empty and 'start_date' in enrollment_history.columns:
                enrollment_history['start_date'] = pd.to_datetime(enrollment_history['start_date'])
                enrollments_over_time = enrollment_history.groupby(pd.Grouper(key='start_date', freq='M')).size().reset_index(name='count')
                enrollments_over_time['cumulative_count'] = enrollments_over_time['count'].cumsum()
            else:
                enrollments_over_time = pd.DataFrame(columns=['start_date', 'count', 'cumulative_count'])
            
            # Calculate revenue over time
            if not enrollment_history.empty and 'start_date' in enrollment_history.columns and 'revenue' in enrollment_history.columns:
                enrollment_history['start_date'] = pd.to_datetime(enrollment_history['start_date'])
                revenue_over_time = enrollment_history.groupby(pd.Grouper(key='start_date', freq='M'))['revenue'].sum().reset_index()
                revenue_over_time['cumulative_revenue'] = revenue_over_time['revenue'].cumsum()
            else:
                revenue_over_time = pd.DataFrame(columns=['start_date', 'revenue', 'cumulative_revenue'])
            
            # Calculate client distribution
            if not enrollment_history.empty and 'client_industry' in enrollment_history.columns:
                client_distribution = enrollment_history.groupby('client_industry').agg({
                    'enrollment_id': 'count',
                    'revenue': 'sum'
                }).reset_index()
                client_distribution.columns = ['client_industry', 'enrollment_count', 'total_revenue']
                client_distribution = client_distribution.sort_values('enrollment_count', ascending=False)
            else:
                client_distribution = pd.DataFrame(columns=['client_industry', 'enrollment_count', 'total_revenue'])
            
            # Calculate feedback distribution
            if not enrollment_history.empty and 'feedback_score' in enrollment_history.columns:
                feedback_distribution = enrollment_history['feedback_score'].value_counts().sort_index().reset_index()
                feedback_distribution.columns = ['feedback_score', 'count']
            else:
                feedback_distribution = pd.DataFrame(columns=['feedback_score', 'count'])
            
            return {
                'program_info': program_info,
                'enrollment_history': enrollment_history,
                'opportunity_history': opportunity_history,
                'enrollments_over_time': enrollments_over_time,
                'revenue_over_time': revenue_over_time,
                'client_distribution': client_distribution,
                'feedback_distribution': feedback_distribution
            }
        except Exception as e:
            return {'error': str(e)}
    
    def create_category_distribution_chart(self, data=None):
        """
        Create a program category distribution chart
        
        Args:
            data: Optional pre-loaded data dictionary
            
        Returns:
            plotly.graph_objects.Figure: Category distribution chart
        """
        if data is None:
            overview = self.get_program_overview()
        else:
            overview = data
        
        if 'error' in overview:
            return None
        
        # Create category distribution chart
        if 'category_distribution' in overview and not overview['category_distribution'].empty:
            fig = px.pie(
                overview['category_distribution'], 
                values='count', 
                names='category',
                title='Program Distribution by Category',
                color_discrete_sequence=px.colors.qualitative.Plotly
            )
            fig.update_traces(textposition='inside', textinfo='percent+label')
            return fig
        
        return None
    
    def create_delivery_mode_chart(self, data=None):
        """
        Create a delivery mode distribution chart
        
        Args:
            data: Optional pre-loaded data dictionary
            
        Returns:
            plotly.graph_objects.Figure: Delivery mode distribution chart
        """
        if data is None:
            overview = self.get_program_overview()
        else:
            overview = data
        
        if 'error' in overview:
            return None
        
        # Create delivery mode distribution chart
        if 'delivery_mode_distribution' in overview and not overview['delivery_mode_distribution'].empty:
            fig = px.bar(
                overview['delivery_mode_distribution'],
                x='delivery_mode',
                y='count',
                title='Program Distribution by Delivery Mode',
                color='delivery_mode',
                text='count'
            )
            fig.update_traces(texttemplate='%{text}', textposition='outside')
            fig.update_layout(xaxis_title='Delivery Mode', yaxis_title='Number of Programs')
            return fig
        
        return None
    
    def create_duration_chart(self, data=None):
        """
        Create a duration distribution chart
        
        Args:
            data: Optional pre-loaded data dictionary
            
        Returns:
            plotly.graph_objects.Figure: Duration distribution chart
        """
        if data is None:
            overview = self.get_program_overview()
        else:
            overview = data
        
        if 'error' in overview:
            return None
        
        # Create duration distribution chart
        if 'duration_distribution' in overview and not overview['duration_distribution'].empty:
            # Define duration order
            duration_order = ['1 Day or Less', '2 Days', '3 Days', '1 Week', 'More than 1 Week']
            
            # Filter and sort data
            df = overview['duration_distribution'].copy()
            df = df[df['duration_category'].isin(duration_order)]
            df['duration_category'] = pd.Categorical(df['duration_category'], categories=duration_order, ordered=True)
            df = df.sort_values('duration_category')
            
            fig = px.bar(
                df,
                x='duration_category',
                y='count',
                title='Program Distribution by Duration',
                color='duration_category',
                color_discrete_sequence=px.colors.sequential.Viridis,
                text='count'
            )
            fig.update_traces(texttemplate='%{text}', textposition='outside')
            fig.update_layout(xaxis_title='Duration', yaxis_title='Number of Programs')
            return fig
        
        return None
    
    def create_top_programs_chart(self, data=None):
        """
        Create a top programs chart
        
        Args:
            data: Optional pre-loaded data dictionary
            
        Returns:
            plotly.graph_objects.Figure: Top programs chart
        """
        if data is None:
            overview = self.get_program_overview()
        else:
            overview = data
        
        if 'error' in overview:
            return None
        
        # Create top programs chart
        if 'top_programs' in overview and not overview['top_programs'].empty:
            df = overview['top_programs'].sort_values('enrollment_count')
            
            fig = px.bar(
                df,
                y='name',
                x='enrollment_count',
                title='Top Programs by Enrollment Count',
                color='category',
                orientation='h',
                text='enrollment_count'
            )
            fig.update_traces(texttemplate='%{text}', textposition='outside')
            fig.update_layout(yaxis_title='Program', xaxis_title='Enrollment Count')
            return fig
        
        return None
    
    def create_enrollment_trends_chart(self, data=None):
        """
        Create an enrollment trends chart
        
        Args:
            data: Optional pre-loaded data dictionary
            
        Returns:
            plotly.graph_objects.Figure: Enrollment trends chart
        """
        if data is None:
            trends_data = self.analyze_program_trends()
        else:
            trends_data = data
        
        if 'error' in trends_data:
            return None
        
        # Create enrollment trends chart
        if 'enrollment_trends' in trends_data and not trends_data['enrollment_trends'].empty:
            df = trends_data['enrollment_trends']
            
            # Create figure with secondary y-axis
            fig = make_subplots(specs=[[{"secondary_y": True}]])
            
            # Add enrollment count bar chart
            fig.add_trace(
                go.Bar(
                    x=df['month'],
                    y=df['enrollment_count'],
                    name='Enrollments',
                    marker_color='rgb(55, 83, 109)'
                ),
                secondary_y=False
            )
            
            # Add revenue line chart
            fig.add_trace(
                go.Scatter(
                    x=df['month'],
                    y=df['total_revenue'],
                    name='Revenue',
                    marker_color='rgb(26, 118, 255)',
                    mode='lines+markers'
                ),
                secondary_y=True
            )
            
            # Update layout
            fig.update_layout(
                title='Program Enrollment Trends Over Time',
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
            fig.update_yaxes(title_text="Enrollment Count", secondary_y=False)
            fig.update_yaxes(title_text="Revenue ($)", secondary_y=True)
            
            return fig
        
        return None
    
    def create_category_popularity_chart(self, data=None):
        """
        Create a category popularity chart
        
        Args:
            data: Optional pre-loaded data dictionary
            
        Returns:
            plotly.graph_objects.Figure: Category popularity chart
        """
        if data is None:
            popularity_data = self.analyze_program_popularity()
        else:
            popularity_data = data
        
        if 'error' in popularity_data:
            return None
        
        # Create category popularity chart
        if 'category_popularity' in popularity_data and not popularity_data['category_popularity'].empty:
            df = popularity_data['category_popularity']
            
            # Create figure with secondary y-axis
            fig = make_subplots(specs=[[{"secondary_y": True}]])
            
            # Add enrollment count bar chart
            fig.add_trace(
                go.Bar(
                    x=df['category'],
                    y=df['enrollment_count'],
                    name='Enrollments',
                    marker_color='rgb(55, 83, 109)'
                ),
                secondary_y=False
            )
            
            # Add revenue line chart
            fig.add_trace(
                go.Scatter(
                    x=df['category'],
                    y=df['total_revenue'],
                    name='Revenue',
                    marker_color='rgb(26, 118, 255)',
                    mode='lines+markers'
                ),
                secondary_y=True
            )
            
            # Update layout
            fig.update_layout(
                title='Program Category Popularity',
                xaxis_title='Category',
                legend=dict(
                    orientation="h",
                    yanchor="bottom",
                    y=1.02,
                    xanchor="right",
                    x=1
                )
            )
            
            # Set y-axes titles
            fig.update_yaxes(title_text="Enrollment Count", secondary_y=False)
            fig.update_yaxes(title_text="Revenue ($)", secondary_y=True)
            
            return fig
        
        return None
    
    def create_profitability_chart(self, data=None):
        """
        Create a program profitability chart
        
        Args:
            data: Optional pre-loaded data dictionary
            
        Returns:
            plotly.graph_objects.Figure: Profitability chart
        """
        if data is None:
            profitability_data = self.analyze_program_profitability()
        else:
            profitability_data = data
        
        if 'error' in profitability_data:
            return None
        
        # Create program profitability chart
        if 'program_profitability' in profitability_data and not profitability_data['program_profitability'].empty:
            df = profitability_data['program_profitability'].head(10)  # Top 10 programs by profit margin
            df = df.sort_values('profit_margin')
            
            fig = px.bar(
                df,
                y='name',
                x='profit_margin',
                title='Top 10 Programs by Profit Margin',
                color='profit_margin',
                color_continuous_scale='RdYlGn',
                orientation='h',
                text='profit_margin'
            )
            fig.update_traces(texttemplate='%{text:.1f}%', textposition='outside')
            fig.update_layout(yaxis_title='Program', xaxis_title='Profit Margin (%)')
            return fig
        
        return None
    
    def create_cost_breakdown_chart(self, data=None):
        """
        Create a cost breakdown chart
        
        Args:
            data: Optional pre-loaded data dictionary
            
        Returns:
            plotly.graph_objects.Figure: Cost breakdown chart
        """
        if data is None:
            profitability_data = self.analyze_program_profitability()
        else:
            profitability_data = data
        
        if 'error' in profitability_data:
            return None
        
        # Create cost breakdown chart
        if 'cost_breakdown' in profitability_data and not profitability_data['cost_breakdown'].empty:
            fig = px.pie(
                profitability_data['cost_breakdown'], 
                values='total_cost', 
                names='cost_type',
                title='Program Cost Breakdown',
                color_discrete_sequence=px.colors.sequential.RdBu
            )
            fig.update_traces(textposition='inside', textinfo='percent+label')
            return fig
        
        return None
    
    def close(self):
        """Close the database connection"""
        if hasattr(self, 'conn') and self.conn:
            self.conn.close()


# Example usage:
if __name__ == "__main__":
    analyzer = ProgramAnalyzer()
    
    # Example: Get program overview
    overview = analyzer.get_program_overview()
    print(f"Total programs: {overview.get('total_programs', 0)}")
    
    # Example: Create a chart
    fig = analyzer.create_category_distribution_chart()
    if fig:
        fig.show()
    
    analyzer.close()
