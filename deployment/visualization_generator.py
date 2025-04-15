import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st
import sqlite3
import io
import base64

class VisualizationGenerator:
    """
    A class to generate visualizations for the Teaching Organization Analytics application.
    Provides comprehensive visualization capabilities for all analysis modules.
    """
    
    def __init__(self, db_path='data/teaching_analytics.db'):
        """Initialize the generator with database connection"""
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        
        # Set default color schemes
        self.color_schemes = {
            'categorical': px.colors.qualitative.Plotly,
            'sequential': px.colors.sequential.Viridis,
            'diverging': px.colors.diverging.RdYlGn,
            'revenue': 'rgb(26, 118, 255)',
            'cost': 'rgb(219, 64, 82)',
            'profit': 'rgb(46, 184, 46)',
            'margin': 'rgb(255, 127, 14)'
        }
    
    def create_dashboard_summary(self):
        """
        Create a dashboard summary with key metrics
        
        Returns:
            dict: Dashboard metrics and charts
        """
        try:
            # Get key metrics
            query = """
            SELECT 
                (SELECT COUNT(*) FROM clients) as total_clients,
                (SELECT COUNT(*) FROM programs) as total_programs,
                (SELECT COUNT(*) FROM enrollments) as total_enrollments,
                (SELECT COUNT(*) FROM opportunities) as total_opportunities,
                (SELECT COUNT(*) FROM opportunities WHERE stage = 'Closed Won') as won_opportunities,
                (SELECT SUM(revenue) FROM enrollments) as total_revenue,
                (SELECT SUM(revenue - (trainer_cost + logistics_cost + venue_cost + utilities_cost + materials_cost)) FROM enrollments) as total_profit
            """
            metrics = pd.read_sql(query, self.conn)
            
            # Get revenue by month
            query = """
            SELECT 
                strftime('%Y-%m', start_date) as month,
                SUM(revenue) as total_revenue
            FROM enrollments
            WHERE start_date IS NOT NULL
            GROUP BY month
            ORDER BY month
            """
            revenue_by_month = pd.read_sql(query, self.conn)
            
            # Get top programs by revenue
            query = """
            SELECT 
                p.name,
                SUM(e.revenue) as total_revenue
            FROM enrollments e
            JOIN programs p ON e.program_id = p.program_id
            GROUP BY p.program_id
            ORDER BY total_revenue DESC
            LIMIT 5
            """
            top_programs = pd.read_sql(query, self.conn)
            
            # Get top clients by revenue
            query = """
            SELECT 
                c.name,
                SUM(e.revenue) as total_revenue
            FROM enrollments e
            JOIN clients c ON e.client_id = c.client_id
            GROUP BY c.client_id
            ORDER BY total_revenue DESC
            LIMIT 5
            """
            top_clients = pd.read_sql(query, self.conn)
            
            # Get pipeline value
            query = """
            SELECT 
                SUM(potential_revenue * (probability / 100)) as weighted_value,
                SUM(potential_revenue) as total_value
            FROM opportunities
            WHERE stage NOT IN ('Closed Won', 'Closed Lost')
            """
            pipeline_value = pd.read_sql(query, self.conn)
            
            # Create revenue trend chart
            if not revenue_by_month.empty:
                revenue_chart = px.line(
                    revenue_by_month, 
                    x='month', 
                    y='total_revenue',
                    title='Revenue Trend',
                    markers=True
                )
                revenue_chart.update_layout(
                    xaxis_title='Month',
                    yaxis_title='Revenue ($)',
                    height=300
                )
            else:
                revenue_chart = None
            
            # Create top programs chart
            if not top_programs.empty:
                programs_chart = px.bar(
                    top_programs,
                    y='name',
                    x='total_revenue',
                    title='Top Programs by Revenue',
                    orientation='h',
                    color_discrete_sequence=[self.color_schemes['revenue']]
                )
                programs_chart.update_layout(
                    xaxis_title='Revenue ($)',
                    yaxis_title='Program',
                    height=300
                )
            else:
                programs_chart = None
            
            # Create top clients chart
            if not top_clients.empty:
                clients_chart = px.bar(
                    top_clients,
                    y='name',
                    x='total_revenue',
                    title='Top Clients by Revenue',
                    orientation='h',
                    color_discrete_sequence=[self.color_schemes['revenue']]
                )
                clients_chart.update_layout(
                    xaxis_title='Revenue ($)',
                    yaxis_title='Client',
                    height=300
                )
            else:
                clients_chart = None
            
            return {
                'metrics': metrics,
                'pipeline_value': pipeline_value,
                'revenue_chart': revenue_chart,
                'programs_chart': programs_chart,
                'clients_chart': clients_chart
            }
        except Exception as e:
            return {'error': str(e)}
    
    def create_client_visualizations(self):
        """
        Create client analysis visualizations
        
        Returns:
            dict: Client analysis charts
        """
        try:
            # Get client distribution by industry
            query = """
            SELECT 
                industry,
                COUNT(*) as client_count
            FROM clients
            WHERE industry IS NOT NULL
            GROUP BY industry
            ORDER BY client_count DESC
            """
            industry_distribution = pd.read_sql(query, self.conn)
            
            # Get client distribution by size
            query = """
            SELECT 
                size,
                COUNT(*) as client_count
            FROM clients
            WHERE size IS NOT NULL
            GROUP BY size
            ORDER BY 
                CASE size
                    WHEN 'Small' THEN 1
                    WHEN 'Medium' THEN 2
                    WHEN 'Large' THEN 3
                    WHEN 'Enterprise' THEN 4
                    ELSE 5
                END
            """
            size_distribution = pd.read_sql(query, self.conn)
            
            # Get client distribution by region
            query = """
            SELECT 
                region,
                COUNT(*) as client_count
            FROM clients
            WHERE region IS NOT NULL
            GROUP BY region
            ORDER BY client_count DESC
            """
            region_distribution = pd.read_sql(query, self.conn)
            
            # Get client spending by industry
            query = """
            SELECT 
                c.industry,
                SUM(e.revenue) as total_revenue,
                COUNT(DISTINCT c.client_id) as client_count,
                SUM(e.revenue) / COUNT(DISTINCT c.client_id) as avg_revenue_per_client
            FROM clients c
            JOIN enrollments e ON c.client_id = e.client_id
            WHERE c.industry IS NOT NULL
            GROUP BY c.industry
            ORDER BY total_revenue DESC
            """
            industry_spending = pd.read_sql(query, self.conn)
            
            # Get client spending by size
            query = """
            SELECT 
                c.size,
                SUM(e.revenue) as total_revenue,
                COUNT(DISTINCT c.client_id) as client_count,
                SUM(e.revenue) / COUNT(DISTINCT c.client_id) as avg_revenue_per_client
            FROM clients c
            JOIN enrollments e ON c.client_id = e.client_id
            WHERE c.size IS NOT NULL
            GROUP BY c.size
            ORDER BY 
                CASE c.size
                    WHEN 'Small' THEN 1
                    WHEN 'Medium' THEN 2
                    WHEN 'Large' THEN 3
                    WHEN 'Enterprise' THEN 4
                    ELSE 5
                END
            """
            size_spending = pd.read_sql(query, self.conn)
            
            # Get client spending over time
            query = """
            SELECT 
                strftime('%Y-%m', e.start_date) as month,
                COUNT(DISTINCT e.client_id) as active_clients,
                SUM(e.revenue) as total_revenue,
                SUM(e.revenue) / COUNT(DISTINCT e.client_id) as avg_revenue_per_client
            FROM enrollments e
            WHERE e.start_date IS NOT NULL
            GROUP BY month
            ORDER BY month
            """
            spending_over_time = pd.read_sql(query, self.conn)
            
            # Create industry distribution chart
            if not industry_distribution.empty:
                industry_chart = px.pie(
                    industry_distribution, 
                    values='client_count', 
                    names='industry',
                    title='Client Distribution by Industry',
                    color_discrete_sequence=self.color_schemes['categorical']
                )
                industry_chart.update_traces(textposition='inside', textinfo='percent+label')
            else:
                industry_chart = None
            
            # Create size distribution chart
            if not size_distribution.empty:
                size_chart = px.bar(
                    size_distribution,
                    x='size',
                    y='client_count',
                    title='Client Distribution by Size',
                    color='size',
                    color_discrete_sequence=self.color_schemes['categorical']
                )
                size_chart.update_layout(
                    xaxis_title='Size',
                    yaxis_title='Number of Clients'
                )
            else:
                size_chart = None
            
            # Create industry spending chart
            if not industry_spending.empty:
                industry_spending_chart = px.bar(
                    industry_spending,
                    x='industry',
                    y='total_revenue',
                    title='Client Spending by Industry',
                    color='industry',
                    color_discrete_sequence=self.color_schemes['categorical'],
                    text='total_revenue'
                )
                industry_spending_chart.update_traces(texttemplate='$%{text:,.0f}', textposition='outside')
                industry_spending_chart.update_layout(
                    xaxis_title='Industry',
                    yaxis_title='Total Revenue ($)'
                )
            else:
                industry_spending_chart = None
            
            # Create spending over time chart
            if not spending_over_time.empty:
                # Create figure with secondary y-axis
                spending_time_chart = make_subplots(specs=[[{"secondary_y": True}]])
                
                # Add revenue bar chart
                spending_time_chart.add_trace(
                    go.Bar(
                        x=spending_over_time['month'],
                        y=spending_over_time['total_revenue'],
                        name='Total Revenue',
                        marker_color=self.color_schemes['revenue']
                    ),
                    secondary_y=False
                )
                
                # Add active clients line chart
                spending_time_chart.add_trace(
                    go.Scatter(
                        x=spending_over_time['month'],
                        y=spending_over_time['active_clients'],
                        name='Active Clients',
                        marker_color=self.color_schemes['margin'],
                        mode='lines+markers'
                    ),
                    secondary_y=True
                )
                
                # Update layout
                spending_time_chart.update_layout(
                    title='Client Spending Over Time',
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
                spending_time_chart.update_yaxes(title_text="Revenue ($)", secondary_y=False)
                spending_time_chart.update_yaxes(title_text="Number of Clients", secondary_y=True)
            else:
                spending_time_chart = None
            
            return {
                'industry_chart': industry_chart,
                'size_chart': size_chart,
                'industry_spending_chart': industry_spending_chart,
                'spending_time_chart': spending_time_chart,
                'industry_distribution': industry_distribution,
                'size_distribution': size_distribution,
                'region_distribution': region_distribution,
                'industry_spending': industry_spending,
                'size_spending': size_spending,
                'spending_over_time': spending_over_time
            }
        except Exception as e:
            return {'error': str(e)}
    
    def create_program_visualizations(self):
        """
        Create program analysis visualizations
        
        Returns:
            dict: Program analysis charts
        """
        try:
            # Get program distribution by category
            query = """
            SELECT 
                category,
                COUNT(*) as program_count
            FROM programs
            WHERE category IS NOT NULL
            GROUP BY category
            ORDER BY program_count DESC
            """
            category_distribution = pd.read_sql(query, self.conn)
            
            # Get program distribution by delivery mode
            query = """
            SELECT 
                delivery_mode,
                COUNT(*) as program_count
            FROM programs
            WHERE delivery_mode IS NOT NULL
            GROUP BY delivery_mode
            ORDER BY program_count DESC
            """
            delivery_mode_distribution = pd.read_sql(query, self.conn)
            
            # Get program popularity by enrollment
            query = """
            SELECT 
                p.name,
                COUNT(e.enrollment_id) as enrollment_count,
                SUM(e.revenue) as total_revenue
            FROM programs p
            JOIN enrollments e ON p.program_id = e.program_id
            GROUP BY p.program_id
            ORDER BY enrollment_count DESC
            LIMIT 10
            """
            program_popularity = pd.read_sql(query, self.conn)
            
            # Get category popularity by enrollment
            query = """
            SELECT 
                p.category,
                COUNT(e.enrollment_id) as enrollment_count,
                SUM(e.revenue) as total_revenue,
                COUNT(DISTINCT p.program_id) as program_count
            FROM programs p
            JOIN enrollments e ON p.program_id = e.program_id
            WHERE p.category IS NOT NULL
            GROUP BY p.category
            ORDER BY enrollment_count DESC
            """
            category_popularity = pd.read_sql(query, self.conn)
            
            # Get delivery mode popularity by enrollment
            query = """
            SELECT 
                e.delivery_mode,
                COUNT(e.enrollment_id) as enrollment_count,
                SUM(e.revenue) as total_revenue
            FROM enrollments e
            WHERE e.delivery_mode IS NOT NULL
            GROUP BY e.delivery_mode
            ORDER BY enrollment_count DESC
            """
            delivery_mode_popularity = pd.read_sql(query, self.conn)
            
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
            
            # Create category distribution chart
            if not category_distribution.empty:
                category_chart = px.pie(
                    category_distribution, 
                    values='program_count', 
                    names='category',
                    title='Program Distribution by Category',
                    color_discrete_sequence=self.color_schemes['categorical']
                )
                category_chart.update_traces(textposition='inside', textinfo='percent+label')
            else:
                category_chart = None
            
            # Create delivery mode distribution chart
            if not delivery_mode_distribution.empty:
                delivery_chart = px.bar(
                    delivery_mode_distribution,
                    x='delivery_mode',
                    y='program_count',
                    title='Program Distribution by Delivery Mode',
                    color='delivery_mode',
                    color_discrete_sequence=self.color_schemes['categorical']
                )
                delivery_chart.update_layout(
                    xaxis_title='Delivery Mode',
                    yaxis_title='Number of Programs'
                )
            else:
                delivery_chart = None
            
            # Create program popularity chart
            if not program_popularity.empty:
                popularity_chart = px.bar(
                    program_popularity,
                    y='name',
                    x='enrollment_count',
                    title='Top Programs by Enrollment',
                    orientation='h',
                    color='total_revenue',
                    color_continuous_scale=self.color_schemes['sequential']
                )
                popularity_chart.update_layout(
                    xaxis_title='Number of Enrollments',
                    yaxis_title='Program'
                )
            else:
                popularity_chart = None
            
            # Create enrollment trends chart
            if not enrollment_trends.empty:
                # Create figure with secondary y-axis
                trends_chart = make_subplots(specs=[[{"secondary_y": True}]])
                
                # Add enrollment count bar chart
                trends_chart.add_trace(
                    go.Bar(
                        x=enrollment_trends['month'],
                        y=enrollment_trends['enrollment_count'],
                        name='Enrollments',
                        marker_color='rgb(55, 83, 109)'
                    ),
                    secondary_y=False
                )
                
                # Add revenue line chart
                trends_chart.add_trace(
                    go.Scatter(
                        x=enrollment_trends['month'],
                        y=enrollment_trends['total_revenue'],
                        name='Revenue',
                        marker_color=self.color_schemes['revenue'],
                        mode='lines+markers'
                    ),
                    secondary_y=True
                )
                
                # Update layout
                trends_chart.update_layout(
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
                trends_chart.update_yaxes(title_text="Enrollment Count", secondary_y=False)
                trends_chart.update_yaxes(title_text="Revenue ($)", secondary_y=True)
            else:
                trends_chart = None
            
            return {
                'category_chart': category_chart,
                'delivery_chart': delivery_chart,
                'popularity_chart': popularity_chart,
                'trends_chart': trends_chart,
                'category_distribution': category_distribution,
                'delivery_mode_distribution': delivery_mode_distribution,
                'program_popularity': program_popularity,
                'category_popularity': category_popularity,
                'delivery_mode_popularity': delivery_mode_popularity,
                'enrollment_trends': enrollment_trends
            }
        except Exception as e:
            return {'error': str(e)}
    
    def create_pipeline_visualizations(self):
        """
        Create pipeline analysis visualizations
        
        Returns:
            dict: Pipeline analysis charts
        """
        try:
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
            
            # Get pipeline value
            query = """
            SELECT 
                SUM(potential_revenue * (probability / 100)) as weighted_value,
                SUM(potential_revenue) as total_value
            FROM opportunities
            WHERE stage NOT IN ('Closed Won', 'Closed Lost')
            """
            pipeline_value = pd.read_sql(query, self.conn)
            
            # Get win rate by category
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
            win_rate_by_category = pd.read_sql(query, self.conn)
            
            if not win_rate_by_category.empty:
                win_rate_by_category['win_rate'] = (win_rate_by_category['won_count'] / win_rate_by_category['total_count']) * 100
            
            # Get pipeline over time
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
            
            # Create pipeline funnel chart
            if not stage_distribution.empty:
                # Define stage order
                stage_order = ['Lead', 'Prospect', 'Proposal', 'Negotiation', 'Closed Won']
                
                # Filter and sort data
                df = stage_distribution.copy()
                df = df[df['stage'].isin(stage_order)]
                
                # Create custom order for display
                display_df = df.copy()
                display_df['stage'] = pd.Categorical(display_df['stage'], categories=stage_order, ordered=True)
                display_df = display_df.sort_values('stage', ascending=False)
                
                funnel_chart = go.Figure(go.Funnel(
                    y=display_df['stage'],
                    x=display_df['count'],
                    textposition="inside",
                    textinfo="value+percent initial",
                    opacity=0.8,
                    marker={"color": ["#4169E1", "#3CB371", "#FFD700", "#FF8C00", "#32CD32"]},
                    connector={"line": {"color": "royalblue", "width": 1}}
                ))
                
                funnel_chart.update_layout(
                    title="Sales Pipeline Funnel",
                    font=dict(size=14)
                )
            else:
                funnel_chart = None
            
            # Create pipeline value chart
            if not stage_distribution.empty:
                # Define stage order
                stage_order = ['Lead', 'Prospect', 'Proposal', 'Negotiation', 'Closed Won']
                
                # Filter and sort data
                df = stage_distribution.copy()
                df = df[df['stage'].isin(stage_order)]
                df['stage'] = pd.Categorical(df['stage'], categories=stage_order, ordered=True)
                df = df.sort_values('stage')
                
                value_chart = px.bar(
                    df,
                    x='stage',
                    y='potential_revenue',
                    title='Pipeline Value by Stage',
                    color='stage',
                    color_discrete_sequence=self.color_schemes['categorical'],
                    text='potential_revenue'
                )
                value_chart.update_traces(texttemplate='$%{text:,.0f}', textposition='outside')
                value_chart.update_layout(xaxis_title='Stage', yaxis_title='Potential Revenue ($)')
            else:
                value_chart = None
            
            # Create win rate chart
            if not win_rate_by_category.empty:
                win_rate_chart = px.bar(
                    win_rate_by_category.sort_values('win_rate', ascending=False),
                    x='category',
                    y='win_rate',
                    title='Win Rate by Program Category',
                    color='win_rate',
                    color_continuous_scale=self.color_schemes['diverging'],
                    text='win_rate'
                )
                win_rate_chart.update_traces(texttemplate='%{text:.1f}%', textposition='outside')
                win_rate_chart.update_layout(xaxis_title='Category', yaxis_title='Win Rate (%)')
            else:
                win_rate_chart = None
            
            # Create pipeline trends chart
            if not pipeline_over_time.empty:
                # Create figure with secondary y-axis
                trends_chart = make_subplots(specs=[[{"secondary_y": True}]])
                
                # Add weighted pipeline bar chart
                trends_chart.add_trace(
                    go.Bar(
                        x=pipeline_over_time['month'],
                        y=pipeline_over_time['weighted_pipeline'],
                        name='Weighted Pipeline',
                        marker_color='rgb(55, 83, 109)'
                    ),
                    secondary_y=False
                )
                
                # Add closed won value line chart
                trends_chart.add_trace(
                    go.Scatter(
                        x=pipeline_over_time['month'],
                        y=pipeline_over_time['closed_won_value'],
                        name='Closed Won Value',
                        marker_color=self.color_schemes['revenue'],
                        mode='lines+markers'
                    ),
                    secondary_y=False
                )
                
                # Add open opportunities line chart
                trends_chart.add_trace(
                    go.Scatter(
                        x=pipeline_over_time['month'],
                        y=pipeline_over_time['open_opportunities'],
                        name='Open Opportunities',
                        marker_color=self.color_schemes['cost'],
                        mode='lines+markers'
                    ),
                    secondary_y=True
                )
                
                # Update layout
                trends_chart.update_layout(
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
                trends_chart.update_yaxes(title_text="Revenue ($)", secondary_y=False)
                trends_chart.update_yaxes(title_text="Number of Opportunities", secondary_y=True)
            else:
                trends_chart = None
            
            return {
                'funnel_chart': funnel_chart,
                'value_chart': value_chart,
                'win_rate_chart': win_rate_chart,
                'trends_chart': trends_chart,
                'stage_distribution': stage_distribution,
                'pipeline_value': pipeline_value,
                'win_rate_by_category': win_rate_by_category,
                'pipeline_over_time': pipeline_over_time
            }
        except Exception as e:
            return {'error': str(e)}
    
    def create_profitability_visualizations(self):
        """
        Create profitability analysis visualizations
        
        Returns:
            dict: Profitability analysis charts
        """
        try:
            # Get financial summary
            query = """
            SELECT 
                SUM(revenue) as total_revenue,
                SUM(trainer_cost + logistics_cost + venue_cost + utilities_cost + materials_cost) as total_costs,
                SUM(revenue - (trainer_cost + logistics_cost + venue_cost + utilities_cost + materials_cost)) as total_profit
            FROM enrollments
            """
            financial_summary = pd.read_sql(query, self.conn)
            
            # Calculate overall profit margin
            if financial_summary['total_revenue'].iloc[0] > 0:
                profit_margin = (financial_summary['total_profit'].iloc[0] / financial_summary['total_revenue'].iloc[0]) * 100
            else:
                profit_margin = 0
            
            # Get cost breakdown
            query = """
            SELECT 
                'Trainer Cost' as cost_type,
                SUM(trainer_cost) as total_cost,
                (SUM(trainer_cost) / SUM(trainer_cost + logistics_cost + venue_cost + utilities_cost + materials_cost)) * 100 as percentage
            FROM enrollments
            UNION ALL
            SELECT 
                'Logistics Cost' as cost_type,
                SUM(logistics_cost) as total_cost,
                (SUM(logistics_cost) / SUM(trainer_cost + logistics_cost + venue_cost + utilities_cost + materials_cost)) * 100 as percentage
            FROM enrollments
            UNION ALL
            SELECT 
                'Venue Cost' as cost_type,
                SUM(venue_cost) as total_cost,
                (SUM(venue_cost) / SUM(trainer_cost + logistics_cost + venue_cost + utilities_cost + materials_cost)) * 100 as percentage
            FROM enrollments
            UNION ALL
            SELECT 
                'Utilities Cost' as cost_type,
                SUM(utilities_cost) as total_cost,
                (SUM(utilities_cost) / SUM(trainer_cost + logistics_cost + venue_cost + utilities_cost + materials_cost)) * 100 as percentage
            FROM enrollments
            UNION ALL
            SELECT 
                'Materials Cost' as cost_type,
                SUM(materials_cost) as total_cost,
                (SUM(materials_cost) / SUM(trainer_cost + logistics_cost + venue_cost + utilities_cost + materials_cost)) * 100 as percentage
            FROM enrollments
            ORDER BY total_cost DESC
            """
            cost_breakdown = pd.read_sql(query, self.conn)
            
            # Get program profitability
            query = """
            SELECT 
                p.name,
                SUM(e.revenue) as total_revenue,
                SUM(e.trainer_cost + e.logistics_cost + e.venue_cost + e.utilities_cost + e.materials_cost) as total_costs,
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
            LIMIT 10
            """
            program_profitability = pd.read_sql(query, self.conn)
            
            # Get category profitability
            query = """
            SELECT 
                p.category,
                SUM(e.revenue) as total_revenue,
                SUM(e.trainer_cost + e.logistics_cost + e.venue_cost + e.utilities_cost + e.materials_cost) as total_costs,
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
            
            # Get profitability over time
            query = """
            SELECT 
                strftime('%Y-%m', start_date) as month,
                SUM(revenue) as total_revenue,
                SUM(trainer_cost + logistics_cost + venue_cost + utilities_cost + materials_cost) as total_costs,
                SUM(revenue - (trainer_cost + logistics_cost + venue_cost + utilities_cost + materials_cost)) as total_profit,
                CASE 
                    WHEN SUM(revenue) > 0 
                    THEN (SUM(revenue - (trainer_cost + logistics_cost + venue_cost + utilities_cost + materials_cost)) / SUM(revenue)) * 100 
                    ELSE 0 
                END as profit_margin
            FROM enrollments
            WHERE start_date IS NOT NULL
            GROUP BY month
            ORDER BY month
            """
            profitability_over_time = pd.read_sql(query, self.conn)
            
            # Create cost breakdown chart
            if not cost_breakdown.empty:
                cost_chart = px.pie(
                    cost_breakdown, 
                    values='total_cost', 
                    names='cost_type',
                    title='Cost Breakdown',
                    color_discrete_sequence=self.color_schemes['categorical']
                )
                cost_chart.update_traces(textposition='inside', textinfo='percent+label')
            else:
                cost_chart = None
            
            # Create program profitability chart
            if not program_profitability.empty:
                program_chart = px.bar(
                    program_profitability.sort_values('profit_margin'),
                    y='name',
                    x='profit_margin',
                    title='Top Programs by Profit Margin',
                    color='profit_margin',
                    color_continuous_scale=self.color_schemes['diverging'],
                    orientation='h',
                    text='profit_margin'
                )
                program_chart.update_traces(texttemplate='%{text:.1f}%', textposition='outside')
                program_chart.update_layout(yaxis_title='Program', xaxis_title='Profit Margin (%)')
            else:
                program_chart = None
            
            # Create category profitability chart
            if not category_profitability.empty:
                # Create figure with secondary y-axis
                category_chart = make_subplots(specs=[[{"secondary_y": True}]])
                
                # Add revenue and profit bars
                category_chart.add_trace(
                    go.Bar(
                        x=category_profitability['category'],
                        y=category_profitability['total_revenue'],
                        name='Revenue',
                        marker_color=self.color_schemes['revenue']
                    ),
                    secondary_y=False
                )
                
                category_chart.add_trace(
                    go.Bar(
                        x=category_profitability['category'],
                        y=category_profitability['total_profit'],
                        name='Profit',
                        marker_color=self.color_schemes['profit']
                    ),
                    secondary_y=False
                )
                
                # Add profit margin line
                category_chart.add_trace(
                    go.Scatter(
                        x=category_profitability['category'],
                        y=category_profitability['profit_margin'],
                        name='Profit Margin (%)',
                        line=dict(color=self.color_schemes['margin'], width=3),
                        mode='lines+markers'
                    ),
                    secondary_y=True
                )
                
                # Update layout
                category_chart.update_layout(
                    title='Profitability by Program Category',
                    xaxis_title='Category',
                    barmode='group',
                    legend=dict(
                        orientation="h",
                        yanchor="bottom",
                        y=1.02,
                        xanchor="right",
                        x=1
                    )
                )
                
                # Set y-axes titles
                category_chart.update_yaxes(title_text="Amount ($)", secondary_y=False)
                category_chart.update_yaxes(title_text="Profit Margin (%)", secondary_y=True)
            else:
                category_chart = None
            
            # Create profitability trends chart
            if not profitability_over_time.empty:
                # Create figure with secondary y-axis
                trends_chart = make_subplots(specs=[[{"secondary_y": True}]])
                
                # Add revenue and cost bars
                trends_chart.add_trace(
                    go.Bar(
                        x=profitability_over_time['month'],
                        y=profitability_over_time['total_revenue'],
                        name='Revenue',
                        marker_color=self.color_schemes['revenue']
                    ),
                    secondary_y=False
                )
                
                trends_chart.add_trace(
                    go.Bar(
                        x=profitability_over_time['month'],
                        y=profitability_over_time['total_costs'],
                        name='Costs',
                        marker_color=self.color_schemes['cost']
                    ),
                    secondary_y=False
                )
                
                # Add profit line
                trends_chart.add_trace(
                    go.Scatter(
                        x=profitability_over_time['month'],
                        y=profitability_over_time['total_profit'],
                        name='Profit',
                        line=dict(color=self.color_schemes['profit'], width=3),
                        mode='lines+markers'
                    ),
                    secondary_y=False
                )
                
                # Add profit margin line
                trends_chart.add_trace(
                    go.Scatter(
                        x=profitability_over_time['month'],
                        y=profitability_over_time['profit_margin'],
                        name='Profit Margin (%)',
                        line=dict(color=self.color_schemes['margin'], width=3, dash='dot'),
                        mode='lines+markers'
                    ),
                    secondary_y=True
                )
                
                # Update layout
                trends_chart.update_layout(
                    title='Profitability Trends Over Time',
                    xaxis_title='Month',
                    barmode='group',
                    legend=dict(
                        orientation="h",
                        yanchor="bottom",
                        y=1.02,
                        xanchor="right",
                        x=1
                    )
                )
                
                # Set y-axes titles
                trends_chart.update_yaxes(title_text="Amount ($)", secondary_y=False)
                trends_chart.update_yaxes(title_text="Profit Margin (%)", secondary_y=True)
            else:
                trends_chart = None
            
            return {
                'cost_chart': cost_chart,
                'program_chart': program_chart,
                'category_chart': category_chart,
                'trends_chart': trends_chart,
                'financial_summary': financial_summary,
                'profit_margin': profit_margin,
                'cost_breakdown': cost_breakdown,
                'program_profitability': program_profitability,
                'category_profitability': category_profitability,
                'profitability_over_time': profitability_over_time
            }
        except Exception as e:
            return {'error': str(e)}
    
    def create_custom_visualization(self, query_type, entity_type, metric, dimension, filters=None, limit=10):
        """
        Create a custom visualization based on user parameters
        
        Args:
            query_type: Type of query (e.g., 'top', 'trend', 'distribution')
            entity_type: Type of entity to analyze (e.g., 'clients', 'programs', 'enrollments')
            metric: Metric to measure (e.g., 'revenue', 'profit', 'count')
            dimension: Dimension to group by (e.g., 'industry', 'category', 'month')
            filters: Optional dictionary of filters to apply
            limit: Maximum number of results to return
            
        Returns:
            dict: Custom visualization data and chart
        """
        try:
            # Build SQL query based on parameters
            select_clause = []
            from_clause = []
            where_clause = []
            group_by_clause = []
            order_by_clause = []
            
            # Handle entity type
            if entity_type == 'clients':
                from_clause.append("clients c")
                if metric in ['revenue', 'profit', 'enrollment_count']:
                    from_clause.append("JOIN enrollments e ON c.client_id = e.client_id")
                
                if dimension in ['industry', 'size', 'region']:
                    select_clause.append(f"c.{dimension}")
                    group_by_clause.append(f"c.{dimension}")
                    where_clause.append(f"c.{dimension} IS NOT NULL")
                
            elif entity_type == 'programs':
                from_clause.append("programs p")
                if metric in ['revenue', 'profit', 'enrollment_count']:
                    from_clause.append("JOIN enrollments e ON p.program_id = e.program_id")
                
                if dimension in ['category', 'delivery_mode']:
                    select_clause.append(f"p.{dimension}")
                    group_by_clause.append(f"p.{dimension}")
                    where_clause.append(f"p.{dimension} IS NOT NULL")
                elif dimension == 'name':
                    select_clause.append("p.name")
                    group_by_clause.append("p.program_id")
                
            elif entity_type == 'enrollments':
                from_clause.append("enrollments e")
                if dimension in ['client', 'client_name']:
                    from_clause.append("JOIN clients c ON e.client_id = c.client_id")
                    select_clause.append("c.name as client_name")
                    group_by_clause.append("e.client_id")
                elif dimension in ['program', 'program_name']:
                    from_clause.append("JOIN programs p ON e.program_id = p.program_id")
                    select_clause.append("p.name as program_name")
                    group_by_clause.append("e.program_id")
                elif dimension == 'delivery_mode':
                    select_clause.append("e.delivery_mode")
                    group_by_clause.append("e.delivery_mode")
                    where_clause.append("e.delivery_mode IS NOT NULL")
                elif dimension == 'month':
                    select_clause.append("strftime('%Y-%m', e.start_date) as month")
                    group_by_clause.append("month")
                    where_clause.append("e.start_date IS NOT NULL")
                
            elif entity_type == 'opportunities':
                from_clause.append("opportunities o")
                if dimension in ['client', 'client_name']:
                    from_clause.append("JOIN clients c ON o.client_id = c.client_id")
                    select_clause.append("c.name as client_name")
                    group_by_clause.append("o.client_id")
                elif dimension in ['program', 'program_name']:
                    from_clause.append("JOIN programs p ON o.program_id = p.program_id")
                    select_clause.append("p.name as program_name")
                    group_by_clause.append("o.program_id")
                elif dimension == 'stage':
                    select_clause.append("o.stage")
                    group_by_clause.append("o.stage")
                elif dimension == 'month':
                    select_clause.append("strftime('%Y-%m', o.created_date) as month")
                    group_by_clause.append("month")
                    where_clause.append("o.created_date IS NOT NULL")
            
            # Handle metric
            if metric == 'revenue':
                select_clause.append("SUM(e.revenue) as total_revenue")
                order_by_clause.append("total_revenue DESC")
            elif metric == 'profit':
                select_clause.append("SUM(e.revenue - (e.trainer_cost + e.logistics_cost + e.venue_cost + e.utilities_cost + e.materials_cost)) as total_profit")
                order_by_clause.append("total_profit DESC")
            elif metric == 'profit_margin':
                select_clause.append("SUM(e.revenue) as total_revenue")
                select_clause.append("SUM(e.revenue - (e.trainer_cost + e.logistics_cost + e.venue_cost + e.utilities_cost + e.materials_cost)) as total_profit")
                select_clause.append("CASE WHEN SUM(e.revenue) > 0 THEN (SUM(e.revenue - (e.trainer_cost + e.logistics_cost + e.venue_cost + e.utilities_cost + e.materials_cost)) / SUM(e.revenue)) * 100 ELSE 0 END as profit_margin")
                order_by_clause.append("profit_margin DESC")
            elif metric == 'enrollment_count':
                select_clause.append("COUNT(e.enrollment_id) as enrollment_count")
                order_by_clause.append("enrollment_count DESC")
            elif metric == 'client_count':
                select_clause.append("COUNT(DISTINCT c.client_id) as client_count")
                order_by_clause.append("client_count DESC")
            elif metric == 'program_count':
                select_clause.append("COUNT(DISTINCT p.program_id) as program_count")
                order_by_clause.append("program_count DESC")
            elif metric == 'opportunity_count':
                select_clause.append("COUNT(o.opportunity_id) as opportunity_count")
                order_by_clause.append("opportunity_count DESC")
            elif metric == 'win_rate':
                select_clause.append("COUNT(CASE WHEN o.stage = 'Closed Won' THEN 1 END) as won_count")
                select_clause.append("COUNT(CASE WHEN o.stage = 'Closed Lost' THEN 1 END) as lost_count")
                select_clause.append("COUNT(*) as total_count")
                select_clause.append("CASE WHEN COUNT(*) > 0 THEN (COUNT(CASE WHEN o.stage = 'Closed Won' THEN 1 END) * 100.0 / COUNT(*)) ELSE 0 END as win_rate")
                order_by_clause.append("win_rate DESC")
            elif metric == 'pipeline_value':
                select_clause.append("SUM(o.potential_revenue) as total_value")
                select_clause.append("SUM(o.potential_revenue * (o.probability / 100)) as weighted_value")
                order_by_clause.append("weighted_value DESC")
            
            # Handle filters
            if filters:
                for key, value in filters.items():
                    if key == 'industry' and value:
                        where_clause.append(f"c.industry = '{value}'")
                    elif key == 'size' and value:
                        where_clause.append(f"c.size = '{value}'")
                    elif key == 'region' and value:
                        where_clause.append(f"c.region = '{value}'")
                    elif key == 'category' and value:
                        where_clause.append(f"p.category = '{value}'")
                    elif key == 'delivery_mode' and value:
                        if entity_type == 'programs':
                            where_clause.append(f"p.delivery_mode = '{value}'")
                        else:
                            where_clause.append(f"e.delivery_mode = '{value}'")
                    elif key == 'stage' and value:
                        where_clause.append(f"o.stage = '{value}'")
                    elif key == 'date_from' and value:
                        if entity_type == 'enrollments':
                            where_clause.append(f"e.start_date >= '{value}'")
                        elif entity_type == 'opportunities':
                            where_clause.append(f"o.created_date >= '{value}'")
                    elif key == 'date_to' and value:
                        if entity_type == 'enrollments':
                            where_clause.append(f"e.start_date <= '{value}'")
                        elif entity_type == 'opportunities':
                            where_clause.append(f"o.created_date <= '{value}'")
            
            # Build the complete query
            query = "SELECT " + ", ".join(select_clause)
            query += " FROM " + " ".join(from_clause)
            if where_clause:
                query += " WHERE " + " AND ".join(where_clause)
            if group_by_clause:
                query += " GROUP BY " + ", ".join(group_by_clause)
            if order_by_clause:
                query += " ORDER BY " + ", ".join(order_by_clause)
            query += f" LIMIT {limit}"
            
            # Execute the query
            data = pd.read_sql(query, self.conn)
            
            # Create appropriate visualization
            chart = None
            
            if not data.empty:
                if query_type == 'top' and dimension in ['industry', 'category', 'size', 'region', 'delivery_mode', 'client_name', 'program_name', 'stage']:
                    # For top N analysis, use bar chart
                    x_col = dimension if dimension not in ['client', 'program'] else f"{dimension}_name"
                    y_col = f"total_{metric}" if metric in ['revenue', 'profit'] else metric
                    
                    if metric == 'profit_margin':
                        y_col = 'profit_margin'
                    elif metric == 'win_rate':
                        y_col = 'win_rate'
                    elif metric == 'pipeline_value':
                        y_col = 'weighted_value'
                    
                    chart = px.bar(
                        data,
                        x=x_col,
                        y=y_col,
                        title=f"Top {dimension.capitalize()} by {metric.replace('_', ' ').capitalize()}",
                        color=x_col,
                        color_discrete_sequence=self.color_schemes['categorical']
                    )
                    chart.update_layout(
                        xaxis_title=dimension.capitalize(),
                        yaxis_title=metric.replace('_', ' ').capitalize()
                    )
                
                elif query_type == 'trend' and dimension == 'month':
                    # For trend analysis, use line chart
                    y_col = f"total_{metric}" if metric in ['revenue', 'profit'] else metric
                    
                    if metric == 'profit_margin':
                        y_col = 'profit_margin'
                    elif metric == 'win_rate':
                        y_col = 'win_rate'
                    elif metric == 'pipeline_value':
                        y_col = 'weighted_value'
                    
                    chart = px.line(
                        data,
                        x='month',
                        y=y_col,
                        title=f"{metric.replace('_', ' ').capitalize()} Trend Over Time",
                        markers=True
                    )
                    chart.update_layout(
                        xaxis_title='Month',
                        yaxis_title=metric.replace('_', ' ').capitalize()
                    )
                
                elif query_type == 'distribution' and dimension in ['industry', 'category', 'size', 'region', 'delivery_mode', 'stage']:
                    # For distribution analysis, use pie chart
                    value_col = f"total_{metric}" if metric in ['revenue', 'profit'] else metric
                    
                    if metric == 'profit_margin':
                        value_col = 'total_profit'
                    elif metric == 'win_rate':
                        value_col = 'total_count'
                    elif metric == 'pipeline_value':
                        value_col = 'weighted_value'
                    
                    chart = px.pie(
                        data,
                        values=value_col,
                        names=dimension,
                        title=f"{metric.replace('_', ' ').capitalize()} Distribution by {dimension.capitalize()}",
                        color_discrete_sequence=self.color_schemes['categorical']
                    )
                    chart.update_traces(textposition='inside', textinfo='percent+label')
                
                elif query_type == 'comparison' and dimension in ['industry', 'category', 'size', 'region', 'delivery_mode']:
                    # For comparison analysis, use grouped bar chart
                    if metric == 'profit_margin':
                        # Create figure with secondary y-axis
                        chart = make_subplots(specs=[[{"secondary_y": True}]])
                        
                        # Add revenue and profit bars
                        chart.add_trace(
                            go.Bar(
                                x=data[dimension],
                                y=data['total_revenue'],
                                name='Revenue',
                                marker_color=self.color_schemes['revenue']
                            ),
                            secondary_y=False
                        )
                        
                        chart.add_trace(
                            go.Bar(
                                x=data[dimension],
                                y=data['total_profit'],
                                name='Profit',
                                marker_color=self.color_schemes['profit']
                            ),
                            secondary_y=False
                        )
                        
                        # Add profit margin line
                        chart.add_trace(
                            go.Scatter(
                                x=data[dimension],
                                y=data['profit_margin'],
                                name='Profit Margin (%)',
                                line=dict(color=self.color_schemes['margin'], width=3),
                                mode='lines+markers'
                            ),
                            secondary_y=True
                        )
                        
                        # Update layout
                        chart.update_layout(
                            title=f"Profitability Comparison by {dimension.capitalize()}",
                            xaxis_title=dimension.capitalize(),
                            barmode='group',
                            legend=dict(
                                orientation="h",
                                yanchor="bottom",
                                y=1.02,
                                xanchor="right",
                                x=1
                            )
                        )
                        
                        # Set y-axes titles
                        chart.update_yaxes(title_text="Amount ($)", secondary_y=False)
                        chart.update_yaxes(title_text="Profit Margin (%)", secondary_y=True)
                    else:
                        chart = px.bar(
                            data,
                            x=dimension,
                            y=f"total_{metric}" if metric in ['revenue', 'profit'] else metric,
                            title=f"{metric.replace('_', ' ').capitalize()} Comparison by {dimension.capitalize()}",
                            color=dimension,
                            color_discrete_sequence=self.color_schemes['categorical']
                        )
                        chart.update_layout(
                            xaxis_title=dimension.capitalize(),
                            yaxis_title=metric.replace('_', ' ').capitalize()
                        )
            
            return {
                'data': data,
                'chart': chart,
                'query': query
            }
        except Exception as e:
            return {'error': str(e)}
    
    def generate_report(self, report_type, entity_id=None, format='html'):
        """
        Generate a comprehensive report
        
        Args:
            report_type: Type of report (e.g., 'dashboard', 'client', 'program', 'pipeline', 'profitability')
            entity_id: Optional ID of specific entity to report on
            format: Output format ('html', 'pdf', 'excel')
            
        Returns:
            dict: Report data and content
        """
        try:
            report_data = {}
            report_content = ""
            
            if report_type == 'dashboard':
                # Generate dashboard report
                dashboard_data = self.create_dashboard_summary()
                client_data = self.create_client_visualizations()
                program_data = self.create_program_visualizations()
                pipeline_data = self.create_pipeline_visualizations()
                profitability_data = self.create_profitability_visualizations()
                
                report_data = {
                    'dashboard': dashboard_data,
                    'client': client_data,
                    'program': program_data,
                    'pipeline': pipeline_data,
                    'profitability': profitability_data
                }
                
                # Generate HTML report
                if format == 'html':
                    report_content = """
                    <html>
                    <head>
                        <title>Teaching Organization Analytics Dashboard</title>
                        <style>
                            body { font-family: Arial, sans-serif; margin: 20px; }
                            h1 { color: #2c3e50; }
                            h2 { color: #3498db; margin-top: 30px; }
                            .metric-container { display: flex; flex-wrap: wrap; gap: 20px; margin: 20px 0; }
                            .metric-box { background-color: #f8f9fa; border-radius: 5px; padding: 15px; min-width: 200px; box-shadow: 0 2px 5px rgba(0,0,0,0.1); }
                            .metric-value { font-size: 24px; font-weight: bold; color: #2c3e50; }
                            .metric-label { font-size: 14px; color: #7f8c8d; }
                            .chart-container { margin: 30px 0; }
                        </style>
                    </head>
                    <body>
                        <h1>Teaching Organization Analytics Dashboard</h1>
                        
                        <h2>Key Metrics</h2>
                        <div class="metric-container">
                    """
                    
                    # Add key metrics
                    if 'metrics' in dashboard_data and not isinstance(dashboard_data['metrics'], str):
                        metrics = dashboard_data['metrics'].iloc[0]
                        
                        report_content += f"""
                            <div class="metric-box">
                                <div class="metric-value">{metrics['total_clients']}</div>
                                <div class="metric-label">Total Clients</div>
                            </div>
                            <div class="metric-box">
                                <div class="metric-value">{metrics['total_programs']}</div>
                                <div class="metric-label">Total Programs</div>
                            </div>
                            <div class="metric-box">
                                <div class="metric-value">{metrics['total_enrollments']}</div>
                                <div class="metric-label">Total Enrollments</div>
                            </div>
                            <div class="metric-box">
                                <div class="metric-value">${metrics['total_revenue']:,.2f}</div>
                                <div class="metric-label">Total Revenue</div>
                            </div>
                            <div class="metric-box">
                                <div class="metric-value">${metrics['total_profit']:,.2f}</div>
                                <div class="metric-label">Total Profit</div>
                            </div>
                        """
                    
                    report_content += """
                        </div>
                        
                        <h2>Revenue Trend</h2>
                        <div class="chart-container" id="revenue-chart"></div>
                        
                        <h2>Top Programs</h2>
                        <div class="chart-container" id="programs-chart"></div>
                        
                        <h2>Top Clients</h2>
                        <div class="chart-container" id="clients-chart"></div>
                        
                        <h2>Pipeline Overview</h2>
                        <div class="chart-container" id="pipeline-chart"></div>
                        
                        <h2>Profitability Analysis</h2>
                        <div class="chart-container" id="profitability-chart"></div>
                        
                        <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
                        <script>
                    """
                    
                    # Add chart data
                    if 'revenue_chart' in dashboard_data and dashboard_data['revenue_chart']:
                        report_content += f"var revenueChart = {dashboard_data['revenue_chart'].to_json()};\n"
                        report_content += "Plotly.newPlot('revenue-chart', revenueChart.data, revenueChart.layout);\n"
                    
                    if 'programs_chart' in dashboard_data and dashboard_data['programs_chart']:
                        report_content += f"var programsChart = {dashboard_data['programs_chart'].to_json()};\n"
                        report_content += "Plotly.newPlot('programs-chart', programsChart.data, programsChart.layout);\n"
                    
                    if 'clients_chart' in dashboard_data and dashboard_data['clients_chart']:
                        report_content += f"var clientsChart = {dashboard_data['clients_chart'].to_json()};\n"
                        report_content += "Plotly.newPlot('clients-chart', clientsChart.data, clientsChart.layout);\n"
                    
                    if 'funnel_chart' in pipeline_data and pipeline_data['funnel_chart']:
                        report_content += f"var pipelineChart = {pipeline_data['funnel_chart'].to_json()};\n"
                        report_content += "Plotly.newPlot('pipeline-chart', pipelineChart.data, pipelineChart.layout);\n"
                    
                    if 'trends_chart' in profitability_data and profitability_data['trends_chart']:
                        report_content += f"var profitabilityChart = {profitability_data['trends_chart'].to_json()};\n"
                        report_content += "Plotly.newPlot('profitability-chart', profitabilityChart.data, profitabilityChart.layout);\n"
                    
                    report_content += """
                        </script>
                    </body>
                    </html>
                    """
            
            elif report_type == 'client' and entity_id:
                # Generate client report
                query = f"SELECT * FROM clients WHERE client_id = {entity_id}"
                client_info = pd.read_sql(query, self.conn)
                
                if client_info.empty:
                    return {'error': f"Client with ID {entity_id} not found"}
                
                # Get client enrollments
                query = f"""
                SELECT 
                    e.enrollment_id,
                    p.name as program_name,
                    e.start_date,
                    e.end_date,
                    e.delivery_mode,
                    e.num_participants,
                    e.revenue,
                    e.trainer_cost + e.logistics_cost + e.venue_cost + e.utilities_cost + e.materials_cost as total_costs,
                    e.revenue - (e.trainer_cost + e.logistics_cost + e.venue_cost + e.utilities_cost + e.materials_cost) as profit,
                    e.status,
                    e.feedback_score
                FROM enrollments e
                JOIN programs p ON e.program_id = p.program_id
                WHERE e.client_id = {entity_id}
                ORDER BY e.start_date DESC
                """
                enrollments = pd.read_sql(query, self.conn)
                
                # Get client opportunities
                query = f"""
                SELECT 
                    o.opportunity_id,
                    p.name as program_name,
                    o.potential_revenue,
                    o.stage,
                    o.probability,
                    o.expected_close_date,
                    o.actual_close_date,
                    o.created_date,
                    o.owner
                FROM opportunities o
                JOIN programs p ON o.program_id = p.program_id
                WHERE o.client_id = {entity_id}
                ORDER BY o.created_date DESC
                """
                opportunities = pd.read_sql(query, self.conn)
                
                # Get spending over time
                query = f"""
                SELECT 
                    strftime('%Y-%m', e.start_date) as month,
                    SUM(e.revenue) as total_revenue,
                    SUM(e.trainer_cost + e.logistics_cost + e.venue_cost + e.utilities_cost + e.materials_cost) as total_costs,
                    SUM(e.revenue - (e.trainer_cost + e.logistics_cost + e.venue_cost + e.utilities_cost + e.materials_cost)) as total_profit
                FROM enrollments e
                WHERE e.client_id = {entity_id} AND e.start_date IS NOT NULL
                GROUP BY month
                ORDER BY month
                """
                spending_over_time = pd.read_sql(query, self.conn)
                
                # Get program preferences
                query = f"""
                SELECT 
                    p.category,
                    COUNT(e.enrollment_id) as enrollment_count,
                    SUM(e.revenue) as total_revenue
                FROM enrollments e
                JOIN programs p ON e.program_id = p.program_id
                WHERE e.client_id = {entity_id} AND p.category IS NOT NULL
                GROUP BY p.category
                ORDER BY enrollment_count DESC
                """
                program_preferences = pd.read_sql(query, self.conn)
                
                report_data = {
                    'client_info': client_info,
                    'enrollments': enrollments,
                    'opportunities': opportunities,
                    'spending_over_time': spending_over_time,
                    'program_preferences': program_preferences
                }
                
                # Create charts
                if not spending_over_time.empty:
                    spending_chart = px.line(
                        spending_over_time,
                        x='month',
                        y=['total_revenue', 'total_costs', 'total_profit'],
                        title='Client Spending Over Time',
                        markers=True,
                        color_discrete_map={
                            'total_revenue': self.color_schemes['revenue'],
                            'total_costs': self.color_schemes['cost'],
                            'total_profit': self.color_schemes['profit']
                        }
                    )
                    spending_chart.update_layout(
                        xaxis_title='Month',
                        yaxis_title='Amount ($)',
                        legend_title='Metric'
                    )
                    report_data['spending_chart'] = spending_chart
                
                if not program_preferences.empty:
                    preferences_chart = px.pie(
                        program_preferences,
                        values='enrollment_count',
                        names='category',
                        title='Program Category Preferences',
                        color_discrete_sequence=self.color_schemes['categorical']
                    )
                    preferences_chart.update_traces(textposition='inside', textinfo='percent+label')
                    report_data['preferences_chart'] = preferences_chart
                
                # Generate HTML report
                if format == 'html':
                    client_name = client_info['name'].iloc[0]
                    client_industry = client_info['industry'].iloc[0] if 'industry' in client_info and not pd.isna(client_info['industry'].iloc[0]) else 'N/A'
                    client_size = client_info['size'].iloc[0] if 'size' in client_info and not pd.isna(client_info['size'].iloc[0]) else 'N/A'
                    client_region = client_info['region'].iloc[0] if 'region' in client_info and not pd.isna(client_info['region'].iloc[0]) else 'N/A'
                    
                    report_content = f"""
                    <html>
                    <head>
                        <title>Client Report: {client_name}</title>
                        <style>
                            body {{ font-family: Arial, sans-serif; margin: 20px; }}
                            h1 {{ color: #2c3e50; }}
                            h2 {{ color: #3498db; margin-top: 30px; }}
                            .client-info {{ display: flex; flex-wrap: wrap; gap: 20px; margin: 20px 0; }}
                            .info-box {{ background-color: #f8f9fa; border-radius: 5px; padding: 15px; min-width: 200px; box-shadow: 0 2px 5px rgba(0,0,0,0.1); }}
                            .info-label {{ font-size: 14px; color: #7f8c8d; }}
                            .info-value {{ font-size: 18px; color: #2c3e50; }}
                            .chart-container {{ margin: 30px 0; }}
                            table {{ border-collapse: collapse; width: 100%; margin: 20px 0; }}
                            th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                            th {{ background-color: #f2f2f2; }}
                            tr:nth-child(even) {{ background-color: #f9f9f9; }}
                        </style>
                    </head>
                    <body>
                        <h1>Client Report: {client_name}</h1>
                        
                        <h2>Client Information</h2>
                        <div class="client-info">
                            <div class="info-box">
                                <div class="info-label">Industry</div>
                                <div class="info-value">{client_industry}</div>
                            </div>
                            <div class="info-box">
                                <div class="info-label">Size</div>
                                <div class="info-value">{client_size}</div>
                            </div>
                            <div class="info-box">
                                <div class="info-label">Region</div>
                                <div class="info-value">{client_region}</div>
                            </div>
                        </div>
                        
                        <h2>Spending Over Time</h2>
                        <div class="chart-container" id="spending-chart"></div>
                        
                        <h2>Program Preferences</h2>
                        <div class="chart-container" id="preferences-chart"></div>
                        
                        <h2>Enrollment History</h2>
                    """
                    
                    if not enrollments.empty:
                        report_content += """
                        <table>
                            <tr>
                                <th>Program</th>
                                <th>Start Date</th>
                                <th>Delivery Mode</th>
                                <th>Participants</th>
                                <th>Revenue</th>
                                <th>Profit</th>
                                <th>Status</th>
                            </tr>
                        """
                        
                        for _, row in enrollments.iterrows():
                            start_date = row['start_date'] if not pd.isna(row['start_date']) else 'N/A'
                            delivery_mode = row['delivery_mode'] if not pd.isna(row['delivery_mode']) else 'N/A'
                            participants = int(row['num_participants']) if not pd.isna(row['num_participants']) else 'N/A'
                            revenue = f"${row['revenue']:,.2f}" if not pd.isna(row['revenue']) else 'N/A'
                            profit = f"${row['profit']:,.2f}" if not pd.isna(row['profit']) else 'N/A'
                            status = row['status'] if not pd.isna(row['status']) else 'N/A'
                            
                            report_content += f"""
                            <tr>
                                <td>{row['program_name']}</td>
                                <td>{start_date}</td>
                                <td>{delivery_mode}</td>
                                <td>{participants}</td>
                                <td>{revenue}</td>
                                <td>{profit}</td>
                                <td>{status}</td>
                            </tr>
                            """
                        
                        report_content += "</table>"
                    else:
                        report_content += "<p>No enrollment history found.</p>"
                    
                    report_content += """
                        <h2>Opportunity Pipeline</h2>
                    """
                    
                    if not opportunities.empty:
                        report_content += """
                        <table>
                            <tr>
                                <th>Program</th>
                                <th>Stage</th>
                                <th>Potential Revenue</th>
                                <th>Probability</th>
                                <th>Expected Close</th>
                                <th>Owner</th>
                            </tr>
                        """
                        
                        for _, row in opportunities.iterrows():
                            stage = row['stage'] if not pd.isna(row['stage']) else 'N/A'
                            potential_revenue = f"${row['potential_revenue']:,.2f}" if not pd.isna(row['potential_revenue']) else 'N/A'
                            probability = f"{row['probability']}%" if not pd.isna(row['probability']) else 'N/A'
                            expected_close = row['expected_close_date'] if not pd.isna(row['expected_close_date']) else 'N/A'
                            owner = row['owner'] if not pd.isna(row['owner']) else 'N/A'
                            
                            report_content += f"""
                            <tr>
                                <td>{row['program_name']}</td>
                                <td>{stage}</td>
                                <td>{potential_revenue}</td>
                                <td>{probability}</td>
                                <td>{expected_close}</td>
                                <td>{owner}</td>
                            </tr>
                            """
                        
                        report_content += "</table>"
                    else:
                        report_content += "<p>No opportunities found.</p>"
                    
                    report_content += """
                        <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
                        <script>
                    """
                    
                    # Add chart data
                    if 'spending_chart' in report_data:
                        report_content += f"var spendingChart = {report_data['spending_chart'].to_json()};\n"
                        report_content += "Plotly.newPlot('spending-chart', spendingChart.data, spendingChart.layout);\n"
                    
                    if 'preferences_chart' in report_data:
                        report_content += f"var preferencesChart = {report_data['preferences_chart'].to_json()};\n"
                        report_content += "Plotly.newPlot('preferences-chart', preferencesChart.data, preferencesChart.layout);\n"
                    
                    report_content += """
                        </script>
                    </body>
                    </html>
                    """
            
            elif report_type == 'program' and entity_id:
                # Generate program report
                query = f"SELECT * FROM programs WHERE program_id = {entity_id}"
                program_info = pd.read_sql(query, self.conn)
                
                if program_info.empty:
                    return {'error': f"Program with ID {entity_id} not found"}
                
                # Get program enrollments
                query = f"""
                SELECT 
                    e.enrollment_id,
                    c.name as client_name,
                    e.start_date,
                    e.end_date,
                    e.delivery_mode,
                    e.num_participants,
                    e.revenue,
                    e.trainer_cost + e.logistics_cost + e.venue_cost + e.utilities_cost + e.materials_cost as total_costs,
                    e.revenue - (e.trainer_cost + e.logistics_cost + e.venue_cost + e.utilities_cost + e.materials_cost) as profit,
                    CASE 
                        WHEN e.revenue > 0 
                        THEN (e.revenue - (e.trainer_cost + e.logistics_cost + e.venue_cost + e.utilities_cost + e.materials_cost)) / e.revenue * 100 
                        ELSE 0 
                    END as profit_margin,
                    e.status,
                    e.feedback_score
                FROM enrollments e
                JOIN clients c ON e.client_id = c.client_id
                WHERE e.program_id = {entity_id}
                ORDER BY e.start_date DESC
                """
                enrollments = pd.read_sql(query, self.conn)
                
                # Get program opportunities
                query = f"""
                SELECT 
                    o.opportunity_id,
                    c.name as client_name,
                    o.potential_revenue,
                    o.stage,
                    o.probability,
                    o.expected_close_date,
                    o.actual_close_date,
                    o.created_date,
                    o.owner
                FROM opportunities o
                JOIN clients c ON o.client_id = c.client_id
                WHERE o.program_id = {entity_id}
                ORDER BY o.created_date DESC
                """
                opportunities = pd.read_sql(query, self.conn)
                
                # Get enrollment trends over time
                query = f"""
                SELECT 
                    strftime('%Y-%m', e.start_date) as month,
                    COUNT(e.enrollment_id) as enrollment_count,
                    SUM(e.revenue) as total_revenue,
                    SUM(e.trainer_cost + e.logistics_cost + e.venue_cost + e.utilities_cost + e.materials_cost) as total_costs,
                    SUM(e.revenue - (e.trainer_cost + e.logistics_cost + e.venue_cost + e.utilities_cost + e.materials_cost)) as total_profit,
                    CASE 
                        WHEN SUM(e.revenue) > 0 
                        THEN (SUM(e.revenue - (e.trainer_cost + e.logistics_cost + e.venue_cost + e.utilities_cost + e.materials_cost)) / SUM(e.revenue)) * 100 
                        ELSE 0 
                    END as profit_margin
                FROM enrollments e
                WHERE e.program_id = {entity_id} AND e.start_date IS NOT NULL
                GROUP BY month
                ORDER BY month
                """
                enrollment_trends = pd.read_sql(query, self.conn)
                
                # Get client distribution
                query = f"""
                SELECT 
                    c.industry,
                    COUNT(e.enrollment_id) as enrollment_count,
                    SUM(e.revenue) as total_revenue
                FROM enrollments e
                JOIN clients c ON e.client_id = c.client_id
                WHERE e.program_id = {entity_id} AND c.industry IS NOT NULL
                GROUP BY c.industry
                ORDER BY enrollment_count DESC
                """
                client_distribution = pd.read_sql(query, self.conn)
                
                # Get cost breakdown
                query = f"""
                SELECT 
                    'Trainer Cost' as cost_type,
                    SUM(trainer_cost) as total_cost,
                    (SUM(trainer_cost) / SUM(trainer_cost + logistics_cost + venue_cost + utilities_cost + materials_cost)) * 100 as percentage
                FROM enrollments
                WHERE program_id = {entity_id}
                UNION ALL
                SELECT 
                    'Logistics Cost' as cost_type,
                    SUM(logistics_cost) as total_cost,
                    (SUM(logistics_cost) / SUM(trainer_cost + logistics_cost + venue_cost + utilities_cost + materials_cost)) * 100 as percentage
                FROM enrollments
                WHERE program_id = {entity_id}
                UNION ALL
                SELECT 
                    'Venue Cost' as cost_type,
                    SUM(venue_cost) as total_cost,
                    (SUM(venue_cost) / SUM(trainer_cost + logistics_cost + venue_cost + utilities_cost + materials_cost)) * 100 as percentage
                FROM enrollments
                WHERE program_id = {entity_id}
                UNION ALL
                SELECT 
                    'Utilities Cost' as cost_type,
                    SUM(utilities_cost) as total_cost,
                    (SUM(utilities_cost) / SUM(trainer_cost + logistics_cost + venue_cost + utilities_cost + materials_cost)) * 100 as percentage
                FROM enrollments
                WHERE program_id = {entity_id}
                UNION ALL
                SELECT 
                    'Materials Cost' as cost_type,
                    SUM(materials_cost) as total_cost,
                    (SUM(materials_cost) / SUM(trainer_cost + logistics_cost + venue_cost + utilities_cost + materials_cost)) * 100 as percentage
                FROM enrollments
                WHERE program_id = {entity_id}
                ORDER BY total_cost DESC
                """
                cost_breakdown = pd.read_sql(query, self.conn)
                
                report_data = {
                    'program_info': program_info,
                    'enrollments': enrollments,
                    'opportunities': opportunities,
                    'enrollment_trends': enrollment_trends,
                    'client_distribution': client_distribution,
                    'cost_breakdown': cost_breakdown
                }
                
                # Create charts
                if not enrollment_trends.empty:
                    # Create figure with secondary y-axis
                    trends_chart = make_subplots(specs=[[{"secondary_y": True}]])
                    
                    # Add revenue and cost bars
                    trends_chart.add_trace(
                        go.Bar(
                            x=enrollment_trends['month'],
                            y=enrollment_trends['total_revenue'],
                            name='Revenue',
                            marker_color=self.color_schemes['revenue']
                        ),
                        secondary_y=False
                    )
                    
                    trends_chart.add_trace(
                        go.Bar(
                            x=enrollment_trends['month'],
                            y=enrollment_trends['total_costs'],
                            name='Costs',
                            marker_color=self.color_schemes['cost']
                        ),
                        secondary_y=False
                    )
                    
                    # Add profit margin line
                    trends_chart.add_trace(
                        go.Scatter(
                            x=enrollment_trends['month'],
                            y=enrollment_trends['profit_margin'],
                            name='Profit Margin (%)',
                            line=dict(color=self.color_schemes['margin'], width=3),
                            mode='lines+markers'
                        ),
                        secondary_y=True
                    )
                    
                    # Update layout
                    trends_chart.update_layout(
                        title='Program Performance Over Time',
                        xaxis_title='Month',
                        barmode='group',
                        legend=dict(
                            orientation="h",
                            yanchor="bottom",
                            y=1.02,
                            xanchor="right",
                            x=1
                        )
                    )
                    
                    # Set y-axes titles
                    trends_chart.update_yaxes(title_text="Amount ($)", secondary_y=False)
                    trends_chart.update_yaxes(title_text="Profit Margin (%)", secondary_y=True)
                    
                    report_data['trends_chart'] = trends_chart
                
                if not client_distribution.empty:
                    client_chart = px.pie(
                        client_distribution,
                        values='enrollment_count',
                        names='industry',
                        title='Client Industry Distribution',
                        color_discrete_sequence=self.color_schemes['categorical']
                    )
                    client_chart.update_traces(textposition='inside', textinfo='percent+label')
                    report_data['client_chart'] = client_chart
                
                if not cost_breakdown.empty:
                    cost_chart = px.pie(
                        cost_breakdown,
                        values='total_cost',
                        names='cost_type',
                        title='Cost Breakdown',
                        color_discrete_sequence=self.color_schemes['categorical']
                    )
                    cost_chart.update_traces(textposition='inside', textinfo='percent+label')
                    report_data['cost_chart'] = cost_chart
                
                # Generate HTML report
                if format == 'html':
                    program_name = program_info['name'].iloc[0]
                    program_category = program_info['category'].iloc[0] if 'category' in program_info and not pd.isna(program_info['category'].iloc[0]) else 'N/A'
                    program_delivery = program_info['delivery_mode'].iloc[0] if 'delivery_mode' in program_info and not pd.isna(program_info['delivery_mode'].iloc[0]) else 'N/A'
                    program_duration = program_info['duration'].iloc[0] if 'duration' in program_info and not pd.isna(program_info['duration'].iloc[0]) else 'N/A'
                    
                    report_content = f"""
                    <html>
                    <head>
                        <title>Program Report: {program_name}</title>
                        <style>
                            body {{ font-family: Arial, sans-serif; margin: 20px; }}
                            h1 {{ color: #2c3e50; }}
                            h2 {{ color: #3498db; margin-top: 30px; }}
                            .program-info {{ display: flex; flex-wrap: wrap; gap: 20px; margin: 20px 0; }}
                            .info-box {{ background-color: #f8f9fa; border-radius: 5px; padding: 15px; min-width: 200px; box-shadow: 0 2px 5px rgba(0,0,0,0.1); }}
                            .info-label {{ font-size: 14px; color: #7f8c8d; }}
                            .info-value {{ font-size: 18px; color: #2c3e50; }}
                            .chart-container {{ margin: 30px 0; }}
                            table {{ border-collapse: collapse; width: 100%; margin: 20px 0; }}
                            th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                            th {{ background-color: #f2f2f2; }}
                            tr:nth-child(even) {{ background-color: #f9f9f9; }}
                        </style>
                    </head>
                    <body>
                        <h1>Program Report: {program_name}</h1>
                        
                        <h2>Program Information</h2>
                        <div class="program-info">
                            <div class="info-box">
                                <div class="info-label">Category</div>
                                <div class="info-value">{program_category}</div>
                            </div>
                            <div class="info-box">
                                <div class="info-label">Delivery Mode</div>
                                <div class="info-value">{program_delivery}</div>
                            </div>
                            <div class="info-box">
                                <div class="info-label">Duration (hours)</div>
                                <div class="info-value">{program_duration}</div>
                            </div>
                        </div>
                        
                        <h2>Performance Over Time</h2>
                        <div class="chart-container" id="trends-chart"></div>
                        
                        <h2>Client Industry Distribution</h2>
                        <div class="chart-container" id="client-chart"></div>
                        
                        <h2>Cost Breakdown</h2>
                        <div class="chart-container" id="cost-chart"></div>
                        
                        <h2>Enrollment History</h2>
                    """
                    
                    if not enrollments.empty:
                        report_content += """
                        <table>
                            <tr>
                                <th>Client</th>
                                <th>Start Date</th>
                                <th>Delivery Mode</th>
                                <th>Participants</th>
                                <th>Revenue</th>
                                <th>Profit</th>
                                <th>Margin</th>
                            </tr>
                        """
                        
                        for _, row in enrollments.iterrows():
                            start_date = row['start_date'] if not pd.isna(row['start_date']) else 'N/A'
                            delivery_mode = row['delivery_mode'] if not pd.isna(row['delivery_mode']) else 'N/A'
                            participants = int(row['num_participants']) if not pd.isna(row['num_participants']) else 'N/A'
                            revenue = f"${row['revenue']:,.2f}" if not pd.isna(row['revenue']) else 'N/A'
                            profit = f"${row['profit']:,.2f}" if not pd.isna(row['profit']) else 'N/A'
                            margin = f"{row['profit_margin']:.1f}%" if not pd.isna(row['profit_margin']) else 'N/A'
                            
                            report_content += f"""
                            <tr>
                                <td>{row['client_name']}</td>
                                <td>{start_date}</td>
                                <td>{delivery_mode}</td>
                                <td>{participants}</td>
                                <td>{revenue}</td>
                                <td>{profit}</td>
                                <td>{margin}</td>
                            </tr>
                            """
                        
                        report_content += "</table>"
                    else:
                        report_content += "<p>No enrollment history found.</p>"
                    
                    report_content += """
                        <h2>Opportunity Pipeline</h2>
                    """
                    
                    if not opportunities.empty:
                        report_content += """
                        <table>
                            <tr>
                                <th>Client</th>
                                <th>Stage</th>
                                <th>Potential Revenue</th>
                                <th>Probability</th>
                                <th>Expected Close</th>
                                <th>Owner</th>
                            </tr>
                        """
                        
                        for _, row in opportunities.iterrows():
                            stage = row['stage'] if not pd.isna(row['stage']) else 'N/A'
                            potential_revenue = f"${row['potential_revenue']:,.2f}" if not pd.isna(row['potential_revenue']) else 'N/A'
                            probability = f"{row['probability']}%" if not pd.isna(row['probability']) else 'N/A'
                            expected_close = row['expected_close_date'] if not pd.isna(row['expected_close_date']) else 'N/A'
                            owner = row['owner'] if not pd.isna(row['owner']) else 'N/A'
                            
                            report_content += f"""
                            <tr>
                                <td>{row['client_name']}</td>
                                <td>{stage}</td>
                                <td>{potential_revenue}</td>
                                <td>{probability}</td>
                                <td>{expected_close}</td>
                                <td>{owner}</td>
                            </tr>
                            """
                        
                        report_content += "</table>"
                    else:
                        report_content += "<p>No opportunities found.</p>"
                    
                    report_content += """
                        <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
                        <script>
                    """
                    
                    # Add chart data
                    if 'trends_chart' in report_data:
                        report_content += f"var trendsChart = {report_data['trends_chart'].to_json()};\n"
                        report_content += "Plotly.newPlot('trends-chart', trendsChart.data, trendsChart.layout);\n"
                    
                    if 'client_chart' in report_data:
                        report_content += f"var clientChart = {report_data['client_chart'].to_json()};\n"
                        report_content += "Plotly.newPlot('client-chart', clientChart.data, clientChart.layout);\n"
                    
                    if 'cost_chart' in report_data:
                        report_content += f"var costChart = {report_data['cost_chart'].to_json()};\n"
                        report_content += "Plotly.newPlot('cost-chart', costChart.data, costChart.layout);\n"
                    
                    report_content += """
                        </script>
                    </body>
                    </html>
                    """
            
            # For Excel format, convert data to Excel file
            if format == 'excel' and report_data:
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                    for key, value in report_data.items():
                        if isinstance(value, pd.DataFrame):
                            value.to_excel(writer, sheet_name=key[:31])  # Excel sheet names limited to 31 chars
                
                output.seek(0)
                report_content = base64.b64encode(output.read()).decode('utf-8')
            
            return {
                'data': report_data,
                'content': report_content,
                'format': format
            }
        except Exception as e:
            return {'error': str(e)}
    
    def close(self):
        """Close the database connection"""
        if hasattr(self, 'conn') and self.conn:
            self.conn.close()


# Example usage:
if __name__ == "__main__":
    generator = VisualizationGenerator()
    
    # Example: Create dashboard summary
    dashboard = generator.create_dashboard_summary()
    print("Dashboard created")
    
    # Example: Create a custom visualization
    custom_viz = generator.create_custom_visualization(
        query_type='top',
        entity_type='programs',
        metric='revenue',
        dimension='category',
        limit=5
    )
    print("Custom visualization created")
    
    generator.close()
