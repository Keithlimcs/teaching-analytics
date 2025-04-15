import pandas as pd
import numpy as np
import sqlite3
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

class ClientAnalyzer:
    """
    A class to analyze client trends for the Teaching Organization Analytics application.
    Provides comprehensive analysis of client buying patterns, segmentation, and retention.
    """
    
    def __init__(self, db_path='data/teaching_analytics.db'):
        """Initialize the analyzer with database connection"""
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        
    def get_client_overview(self):
        """
        Get an overview of client data
        
        Returns:
            dict: Overview statistics
        """
        try:
            # Get total clients
            query = "SELECT COUNT(*) FROM clients"
            total_clients = pd.read_sql(query, self.conn).iloc[0, 0]
            
            # Get industry distribution
            query = """
            SELECT industry, COUNT(*) as count 
            FROM clients 
            WHERE industry IS NOT NULL 
            GROUP BY industry 
            ORDER BY count DESC
            """
            industry_distribution = pd.read_sql(query, self.conn)
            
            # Get size distribution
            query = """
            SELECT size, COUNT(*) as count 
            FROM clients 
            WHERE size IS NOT NULL 
            GROUP BY size 
            ORDER BY CASE 
                WHEN size = 'Small' THEN 1
                WHEN size = 'Medium' THEN 2
                WHEN size = 'Large' THEN 3
                WHEN size = 'Enterprise' THEN 4
                ELSE 5
            END
            """
            size_distribution = pd.read_sql(query, self.conn)
            
            # Get region distribution
            query = """
            SELECT region, COUNT(*) as count 
            FROM clients 
            WHERE region IS NOT NULL 
            GROUP BY region 
            ORDER BY count DESC
            """
            region_distribution = pd.read_sql(query, self.conn)
            
            # Get top clients by spend
            query = """
            SELECT c.name, c.industry, c.size, c.total_spend
            FROM clients c
            WHERE c.total_spend > 0
            ORDER BY c.total_spend DESC
            LIMIT 10
            """
            top_clients = pd.read_sql(query, self.conn)
            
            # Get client acquisition over time
            query = """
            SELECT 
                strftime('%Y-%m', first_engagement_date) as month,
                COUNT(*) as new_clients
            FROM clients
            WHERE first_engagement_date IS NOT NULL
            GROUP BY month
            ORDER BY month
            """
            client_acquisition = pd.read_sql(query, self.conn)
            
            return {
                'total_clients': total_clients,
                'industry_distribution': industry_distribution,
                'size_distribution': size_distribution,
                'region_distribution': region_distribution,
                'top_clients': top_clients,
                'client_acquisition': client_acquisition
            }
        except Exception as e:
            return {'error': str(e)}
    
    def analyze_buying_patterns(self):
        """
        Analyze client buying patterns
        
        Returns:
            dict: Buying pattern analysis
        """
        try:
            # Get average spend by industry
            query = """
            SELECT 
                c.industry,
                AVG(c.total_spend) as avg_spend,
                COUNT(*) as client_count
            FROM clients c
            WHERE c.industry IS NOT NULL AND c.total_spend > 0
            GROUP BY c.industry
            ORDER BY avg_spend DESC
            """
            industry_spend = pd.read_sql(query, self.conn)
            
            # Get average spend by size
            query = """
            SELECT 
                c.size,
                AVG(c.total_spend) as avg_spend,
                COUNT(*) as client_count
            FROM clients c
            WHERE c.size IS NOT NULL AND c.total_spend > 0
            GROUP BY c.size
            ORDER BY CASE 
                WHEN c.size = 'Small' THEN 1
                WHEN c.size = 'Medium' THEN 2
                WHEN c.size = 'Large' THEN 3
                WHEN c.size = 'Enterprise' THEN 4
                ELSE 5
            END
            """
            size_spend = pd.read_sql(query, self.conn)
            
            # Get average spend by region
            query = """
            SELECT 
                c.region,
                AVG(c.total_spend) as avg_spend,
                COUNT(*) as client_count
            FROM clients c
            WHERE c.region IS NOT NULL AND c.total_spend > 0
            GROUP BY c.region
            ORDER BY avg_spend DESC
            """
            region_spend = pd.read_sql(query, self.conn)
            
            # Get program preferences by industry
            query = """
            SELECT 
                c.industry,
                p.category,
                COUNT(*) as enrollment_count
            FROM enrollments e
            JOIN clients c ON e.client_id = c.client_id
            JOIN programs p ON e.program_id = p.program_id
            WHERE c.industry IS NOT NULL AND p.category IS NOT NULL
            GROUP BY c.industry, p.category
            ORDER BY c.industry, enrollment_count DESC
            """
            industry_preferences = pd.read_sql(query, self.conn)
            
            # Get seasonal trends
            query = """
            SELECT 
                strftime('%m', e.start_date) as month,
                COUNT(*) as enrollment_count,
                SUM(e.revenue) as total_revenue
            FROM enrollments e
            WHERE e.start_date IS NOT NULL
            GROUP BY month
            ORDER BY month
            """
            seasonal_trends = pd.read_sql(query, self.conn)
            
            # Add month names for better readability
            month_names = {
                '01': 'January', '02': 'February', '03': 'March', '04': 'April',
                '05': 'May', '06': 'June', '07': 'July', '08': 'August',
                '09': 'September', '10': 'October', '11': 'November', '12': 'December'
            }
            if not seasonal_trends.empty and 'month' in seasonal_trends.columns:
                seasonal_trends['month_name'] = seasonal_trends['month'].map(month_names)
            
            return {
                'industry_spend': industry_spend,
                'size_spend': size_spend,
                'region_spend': region_spend,
                'industry_preferences': industry_preferences,
                'seasonal_trends': seasonal_trends
            }
        except Exception as e:
            return {'error': str(e)}
    
    def analyze_client_retention(self):
        """
        Analyze client retention
        
        Returns:
            dict: Client retention analysis
        """
        try:
            # Get repeat business statistics
            query = """
            SELECT 
                c.client_id,
                c.name,
                COUNT(e.enrollment_id) as enrollment_count
            FROM clients c
            JOIN enrollments e ON c.client_id = e.client_id
            GROUP BY c.client_id
            ORDER BY enrollment_count DESC
            """
            repeat_business = pd.read_sql(query, self.conn)
            
            # Calculate retention metrics
            if not repeat_business.empty:
                single_engagement = len(repeat_business[repeat_business['enrollment_count'] == 1])
                multiple_engagements = len(repeat_business[repeat_business['enrollment_count'] > 1])
                total_clients_with_enrollments = len(repeat_business)
                
                retention_rate = 0
                if total_clients_with_enrollments > 0:
                    retention_rate = (multiple_engagements / total_clients_with_enrollments) * 100
                
                # Get distribution of enrollment counts
                enrollment_distribution = repeat_business['enrollment_count'].value_counts().sort_index()
                enrollment_distribution = pd.DataFrame({
                    'enrollment_count': enrollment_distribution.index,
                    'client_count': enrollment_distribution.values
                })
            else:
                single_engagement = 0
                multiple_engagements = 0
                retention_rate = 0
                enrollment_distribution = pd.DataFrame(columns=['enrollment_count', 'client_count'])
            
            # Get time between enrollments
            query = """
            WITH client_enrollments AS (
                SELECT 
                    e.client_id,
                    e.start_date,
                    LAG(e.start_date) OVER (PARTITION BY e.client_id ORDER BY e.start_date) as prev_date
                FROM enrollments e
                WHERE e.start_date IS NOT NULL
                ORDER BY e.client_id, e.start_date
            )
            SELECT 
                client_id,
                julianday(start_date) - julianday(prev_date) as days_between_enrollments
            FROM client_enrollments
            WHERE prev_date IS NOT NULL
            """
            time_between = pd.read_sql(query, self.conn)
            
            # Calculate average time between enrollments
            avg_time_between = 0
            if not time_between.empty:
                avg_time_between = time_between['days_between_enrollments'].mean()
            
            return {
                'repeat_business': repeat_business,
                'single_engagement': single_engagement,
                'multiple_engagements': multiple_engagements,
                'retention_rate': retention_rate,
                'enrollment_distribution': enrollment_distribution,
                'avg_time_between_enrollments': avg_time_between,
                'time_between_enrollments': time_between
            }
        except Exception as e:
            return {'error': str(e)}
    
    def analyze_client_acquisition(self):
        """
        Analyze client acquisition
        
        Returns:
            dict: Client acquisition analysis
        """
        try:
            # Get client acquisition over time
            query = """
            SELECT 
                strftime('%Y-%m', first_engagement_date) as month,
                COUNT(*) as new_clients
            FROM clients
            WHERE first_engagement_date IS NOT NULL
            GROUP BY month
            ORDER BY month
            """
            acquisition_by_month = pd.read_sql(query, self.conn)
            
            # Get acquisition by industry over time
            query = """
            SELECT 
                strftime('%Y-%m', first_engagement_date) as month,
                industry,
                COUNT(*) as new_clients
            FROM clients
            WHERE first_engagement_date IS NOT NULL AND industry IS NOT NULL
            GROUP BY month, industry
            ORDER BY month, industry
            """
            acquisition_by_industry = pd.read_sql(query, self.conn)
            
            # Get acquisition by size over time
            query = """
            SELECT 
                strftime('%Y-%m', first_engagement_date) as month,
                size,
                COUNT(*) as new_clients
            FROM clients
            WHERE first_engagement_date IS NOT NULL AND size IS NOT NULL
            GROUP BY month, size
            ORDER BY month, size
            """
            acquisition_by_size = pd.read_sql(query, self.conn)
            
            # Get acquisition by region over time
            query = """
            SELECT 
                strftime('%Y-%m', first_engagement_date) as month,
                region,
                COUNT(*) as new_clients
            FROM clients
            WHERE first_engagement_date IS NOT NULL AND region IS NOT NULL
            GROUP BY month, region
            ORDER BY month, region
            """
            acquisition_by_region = pd.read_sql(query, self.conn)
            
            # Calculate growth rate
            if not acquisition_by_month.empty and len(acquisition_by_month) > 1:
                acquisition_by_month['cumulative_clients'] = acquisition_by_month['new_clients'].cumsum()
                
                # Calculate month-over-month growth rate
                acquisition_by_month['growth_rate'] = acquisition_by_month['cumulative_clients'].pct_change() * 100
                
                # First month growth rate is undefined, set to 0
                acquisition_by_month.loc[0, 'growth_rate'] = 0
            
            return {
                'acquisition_by_month': acquisition_by_month,
                'acquisition_by_industry': acquisition_by_industry,
                'acquisition_by_size': acquisition_by_size,
                'acquisition_by_region': acquisition_by_region
            }
        except Exception as e:
            return {'error': str(e)}
    
    def get_client_details(self, client_id):
        """
        Get detailed information for a specific client
        
        Args:
            client_id: ID of the client to analyze
            
        Returns:
            dict: Client details
        """
        try:
            # Get client information
            query = f"SELECT * FROM clients WHERE client_id = {client_id}"
            client_info = pd.read_sql(query, self.conn)
            
            if client_info.empty:
                return {'error': f"Client with ID {client_id} not found"}
            
            # Get enrollment history
            query = f"""
            SELECT 
                e.enrollment_id,
                p.name as program_name,
                p.category as program_category,
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
            JOIN programs p ON e.program_id = p.program_id
            WHERE e.client_id = {client_id}
            ORDER BY e.start_date DESC
            """
            enrollment_history = pd.read_sql(query, self.conn)
            
            # Get opportunity history
            query = f"""
            SELECT 
                o.opportunity_id,
                p.name as program_name,
                p.category as program_category,
                o.potential_revenue,
                o.estimated_participants,
                o.stage,
                o.probability,
                o.expected_close_date,
                o.actual_close_date,
                o.created_date,
                o.owner
            FROM opportunities o
            JOIN programs p ON o.program_id = p.program_id
            WHERE o.client_id = {client_id}
            ORDER BY o.created_date DESC
            """
            opportunity_history = pd.read_sql(query, self.conn)
            
            # Calculate spending over time
            if not enrollment_history.empty and 'start_date' in enrollment_history.columns and 'revenue' in enrollment_history.columns:
                enrollment_history['start_date'] = pd.to_datetime(enrollment_history['start_date'])
                spending_over_time = enrollment_history.groupby(pd.Grouper(key='start_date', freq='M'))['revenue'].sum().reset_index()
                spending_over_time['cumulative_spend'] = spending_over_time['revenue'].cumsum()
            else:
                spending_over_time = pd.DataFrame(columns=['start_date', 'revenue', 'cumulative_spend'])
            
            # Calculate program preferences
            if not enrollment_history.empty and 'program_category' in enrollment_history.columns:
                program_preferences = enrollment_history.groupby('program_category').agg({
                    'enrollment_id': 'count',
                    'revenue': 'sum'
                }).reset_index()
                program_preferences.columns = ['program_category', 'enrollment_count', 'total_revenue']
                program_preferences = program_preferences.sort_values('enrollment_count', ascending=False)
            else:
                program_preferences = pd.DataFrame(columns=['program_category', 'enrollment_count', 'total_revenue'])
            
            return {
                'client_info': client_info,
                'enrollment_history': enrollment_history,
                'opportunity_history': opportunity_history,
                'spending_over_time': spending_over_time,
                'program_preferences': program_preferences
            }
        except Exception as e:
            return {'error': str(e)}
    
    def create_client_segmentation_chart(self, data=None):
        """
        Create a client segmentation chart
        
        Args:
            data: Optional pre-loaded data dictionary
            
        Returns:
            plotly.graph_objects.Figure: Segmentation chart
        """
        if data is None:
            overview = self.get_client_overview()
        else:
            overview = data
        
        if 'error' in overview:
            return None
        
        # Create industry distribution chart
        if 'industry_distribution' in overview and not overview['industry_distribution'].empty:
            fig = px.pie(
                overview['industry_distribution'], 
                values='count', 
                names='industry',
                title='Client Distribution by Industry',
                color_discrete_sequence=px.colors.qualitative.Plotly
            )
            fig.update_traces(textposition='inside', textinfo='percent+label')
            return fig
        
        return None
    
    def create_client_size_chart(self, data=None):
        """
        Create a client size distribution chart
        
        Args:
            data: Optional pre-loaded data dictionary
            
        Returns:
            plotly.graph_objects.Figure: Size distribution chart
        """
        if data is None:
            overview = self.get_client_overview()
        else:
            overview = data
        
        if 'error' in overview:
            return None
        
        # Create size distribution chart
        if 'size_distribution' in overview and not overview['size_distribution'].empty:
            # Define size order
            size_order = ['Small', 'Medium', 'Large', 'Enterprise']
            
            # Filter and sort data
            df = overview['size_distribution'].copy()
            df = df[df['size'].isin(size_order)]
            df['size'] = pd.Categorical(df['size'], categories=size_order, ordered=True)
            df = df.sort_values('size')
            
            fig = px.bar(
                df,
                x='size',
                y='count',
                title='Client Distribution by Size',
                color='size',
                color_discrete_sequence=px.colors.sequential.Blues[2:],
                text='count'
            )
            fig.update_traces(texttemplate='%{text}', textposition='outside')
            fig.update_layout(xaxis_title='Company Size', yaxis_title='Number of Clients')
            return fig
        
        return None
    
    def create_region_chart(self, data=None):
        """
        Create a region distribution chart
        
        Args:
            data: Optional pre-loaded data dictionary
            
        Returns:
            plotly.graph_objects.Figure: Region distribution chart
        """
        if data is None:
            overview = self.get_client_overview()
        else:
            overview = data
        
        if 'error' in overview:
            return None
        
        # Create region distribution chart
        if 'region_distribution' in overview and not overview['region_distribution'].empty:
            fig = px.bar(
                overview['region_distribution'],
                x='region',
                y='count',
                title='Client Distribution by Region',
                color='count',
                color_continuous_scale='Viridis',
                text='count'
            )
            fig.update_traces(texttemplate='%{text}', textposition='outside')
            fig.update_layout(xaxis_title='Region', yaxis_title='Number of Clients')
            return fig
        
        return None
    
    def create_top_clients_chart(self, data=None):
        """
        Create a top clients chart
        
        Args:
            data: Optional pre-loaded data dictionary
            
        Returns:
            plotly.graph_objects.Figure: Top clients chart
        """
        if data is None:
            overview = self.get_client_overview()
        else:
            overview = data
        
        if 'error' in overview:
            return None
        
        # Create top clients chart
        if 'top_clients' in overview and not overview['top_clients'].empty:
            df = overview['top_clients'].sort_values('total_spend')
            
            fig = px.bar(
                df,
                y='name',
                x='total_spend',
                title='Top Clients by Total Spend',
                color='industry',
                orientation='h',
                text='total_spend'
            )
            fig.update_traces(texttemplate='$%{text:.2f}', textposition='outside')
            fig.update_layout(yaxis_title='Client', xaxis_title='Total Spend ($)')
            return fig
        
        return None
    
    def create_acquisition_chart(self, data=None):
        """
        Create a client acquisition chart
        
        Args:
            data: Optional pre-loaded data dictionary
            
        Returns:
            plotly.graph_objects.Figure: Acquisition chart
        """
        if data is None:
            acquisition_data = self.analyze_client_acquisition()
        else:
            acquisition_data = data
        
        if 'error' in acquisition_data:
            return None
        
        # Create acquisition chart
        if 'acquisition_by_month' in acquisition_data and not acquisition_data['acquisition_by_month'].empty:
            df = acquisition_data['acquisition_by_month']
            
            # Create figure with secondary y-axis
            fig = make_subplots(specs=[[{"secondary_y": True}]])
            
            # Add new clients bar chart
            fig.add_trace(
                go.Bar(
                    x=df['month'],
                    y=df['new_clients'],
                    name='New Clients',
                    marker_color='rgb(55, 83, 109)'
                ),
                secondary_y=False
            )
            
            # Add cumulative clients line chart if available
            if 'cumulative_clients' in df.columns:
                fig.add_trace(
                    go.Scatter(
                        x=df['month'],
                        y=df['cumulative_clients'],
                        name='Cumulative Clients',
                        marker_color='rgb(26, 118, 255)',
                        mode='lines+markers'
                    ),
                    secondary_y=True
                )
            
            # Add growth rate line chart if available
            if 'growth_rate' in df.columns:
                fig.add_trace(
                    go.Scatter(
                        x=df['month'],
                        y=df['growth_rate'],
                        name='Growth Rate (%)',
                        marker_color='rgb(219, 64, 82)',
                        mode='lines+markers',
                        line=dict(dash='dash')
                    ),
                    secondary_y=True
                )
            
            # Update layout
            fig.update_layout(
                title='Client Acquisition Over Time',
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
            fig.update_yaxes(title_text="New Clients", secondary_y=False)
            fig.update_yaxes(title_text="Cumulative Clients / Growth Rate (%)", secondary_y=True)
            
            return fig
        
        return None
    
    def create_buying_patterns_chart(self, data=None):
        """
        Create a buying patterns chart
        
        Args:
            data: Optional pre-loaded data dictionary
            
        Returns:
            plotly.graph_objects.Figure: Buying patterns chart
        """
        if data is None:
            patterns_data = self.analyze_buying_patterns()
        else:
            patterns_data = data
        
        if 'error' in patterns_data:
            return None
        
        # Create seasonal trends chart
        if 'seasonal_trends' in patterns_data and not patterns_data['seasonal_trends'].empty:
            df = patterns_data['seasonal_trends']
            
            # Create figure with secondary y-axis
            fig = make_subplots(specs=[[{"secondary_y": True}]])
            
            # Add enrollment count bar chart
            fig.add_trace(
                go.Bar(
                    x=df['month_name'] if 'month_name' in df.columns else df['month'],
                    y=df['enrollment_count'],
                    name='Enrollments',
                    marker_color='rgb(55, 83, 109)'
                ),
                secondary_y=False
            )
            
            # Add revenue line chart
            fig.add_trace(
                go.Scatter(
                    x=df['month_name'] if 'month_name' in df.columns else df['month'],
                    y=df['total_revenue'],
                    name='Revenue',
                    marker_color='rgb(26, 118, 255)',
                    mode='lines+markers'
                ),
                secondary_y=True
            )
            
            # Update layout
            fig.update_layout(
                title='Seasonal Enrollment Trends',
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
    
    def create_industry_spend_chart(self, data=None):
        """
        Create an industry spend chart
        
        Args:
            data: Optional pre-loaded data dictionary
            
        Returns:
            plotly.graph_objects.Figure: Industry spend chart
        """
        if data is None:
            patterns_data = self.analyze_buying_patterns()
        else:
            patterns_data = data
        
        if 'error' in patterns_data:
            return None
        
        # Create industry spend chart
        if 'industry_spend' in patterns_data and not patterns_data['industry_spend'].empty:
            df = patterns_data['industry_spend']
            
            # Create figure with secondary y-axis
            fig = make_subplots(specs=[[{"secondary_y": True}]])
            
            # Add average spend bar chart
            fig.add_trace(
                go.Bar(
                    x=df['industry'],
                    y=df['avg_spend'],
                    name='Average Spend',
                    marker_color='rgb(55, 83, 109)'
                ),
                secondary_y=False
            )
            
            # Add client count line chart
            fig.add_trace(
                go.Scatter(
                    x=df['industry'],
                    y=df['client_count'],
                    name='Client Count',
                    marker_color='rgb(26, 118, 255)',
                    mode='lines+markers'
                ),
                secondary_y=True
            )
            
            # Update layout
            fig.update_layout(
                title='Average Spend by Industry',
                xaxis_title='Industry',
                legend=dict(
                    orientation="h",
                    yanchor="bottom",
                    y=1.02,
                    xanchor="right",
                    x=1
                )
            )
            
            # Set y-axes titles
            fig.update_yaxes(title_text="Average Spend ($)", secondary_y=False)
            fig.update_yaxes(title_text="Client Count", secondary_y=True)
            
            return fig
        
        return None
    
    def create_retention_chart(self, data=None):
        """
        Create a client retention chart
        
        Args:
            data: Optional pre-loaded data dictionary
            
        Returns:
            plotly.graph_objects.Figure: Retention chart
        """
        if data is None:
            retention_data = self.analyze_client_retention()
        else:
            retention_data = data
        
        if 'error' in retention_data:
            return None
        
        # Create enrollment distribution chart
        if 'enrollment_distribution' in retention_data and not retention_data['enrollment_distribution'].empty:
            df = retention_data['enrollment_distribution']
            
            fig = px.bar(
                df,
                x='enrollment_count',
                y='client_count',
                title='Distribution of Enrollments per Client',
                color='enrollment_count',
                color_continuous_scale='Viridis',
                text='client_count'
            )
            fig.update_traces(texttemplate='%{text}', textposition='outside')
            fig.update_layout(
                xaxis_title='Number of Enrollments',
                yaxis_title='Number of Clients',
                xaxis=dict(tickmode='linear')
            )
            
            return fig
        
        return None
    
    def close(self):
        """Close the database connection"""
        if hasattr(self, 'conn') and self.conn:
            self.conn.close()


# Example usage:
if __name__ == "__main__":
    analyzer = ClientAnalyzer()
    
    # Example: Get client overview
    overview = analyzer.get_client_overview()
    print(f"Total clients: {overview.get('total_clients', 0)}")
    
    # Example: Create a chart
    fig = analyzer.create_client_segmentation_chart()
    if fig:
        fig.show()
    
    analyzer.close()
