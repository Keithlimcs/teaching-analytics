import pandas as pd
import numpy as np
import sqlite3
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st
from datetime import datetime, timedelta

class ProfitabilityTracker:
    """
    A class to track and analyze program profitability for the Teaching Organization Analytics application.
    Provides comprehensive analysis of revenue, costs, and profit margins.
    """
    
    def __init__(self, db_path='data/teaching_analytics.db'):
        """Initialize the tracker with database connection"""
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
    
    def get_profitability_overview(self):
        """
        Get an overview of profitability metrics
        
        Returns:
            dict: Overview statistics
        """
        try:
            # Get total revenue, costs, and profit
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
            
            # Get top profitable programs
            query = """
            SELECT 
                p.program_id,
                p.name,
                p.category,
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
            JOIN programs p ON e.program_id = p.program_id
            GROUP BY p.program_id
            ORDER BY total_profit DESC
            LIMIT 10
            """
            top_profitable_programs = pd.read_sql(query, self.conn)
            
            # Get top profitable clients
            query = """
            SELECT 
                c.client_id,
                c.name,
                c.industry,
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
            JOIN clients c ON e.client_id = c.client_id
            GROUP BY c.client_id
            ORDER BY total_profit DESC
            LIMIT 10
            """
            top_profitable_clients = pd.read_sql(query, self.conn)
            
            return {
                'financial_summary': financial_summary,
                'profit_margin': profit_margin,
                'cost_breakdown': cost_breakdown,
                'top_profitable_programs': top_profitable_programs,
                'top_profitable_clients': top_profitable_clients
            }
        except Exception as e:
            return {'error': str(e)}
    
    def analyze_profitability_by_program(self):
        """
        Analyze profitability by program
        
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
            ORDER BY total_profit DESC
            """
            program_profitability = pd.read_sql(query, self.conn)
            
            # Get category profitability
            query = """
            SELECT 
                p.category,
                COUNT(e.enrollment_id) as enrollment_count,
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
            ORDER BY total_profit DESC
            """
            category_profitability = pd.read_sql(query, self.conn)
            
            # Get delivery mode profitability
            query = """
            SELECT 
                p.delivery_mode,
                COUNT(e.enrollment_id) as enrollment_count,
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
            WHERE p.delivery_mode IS NOT NULL
            GROUP BY p.delivery_mode
            ORDER BY total_profit DESC
            """
            delivery_mode_profitability = pd.read_sql(query, self.conn)
            
            # Get program cost structure
            query = """
            SELECT 
                p.program_id,
                p.name,
                AVG(e.trainer_cost) as avg_trainer_cost,
                AVG(e.logistics_cost) as avg_logistics_cost,
                AVG(e.venue_cost) as avg_venue_cost,
                AVG(e.utilities_cost) as avg_utilities_cost,
                AVG(e.materials_cost) as avg_materials_cost,
                AVG(e.revenue) as avg_revenue,
                COUNT(e.enrollment_id) as enrollment_count
            FROM programs p
            JOIN enrollments e ON p.program_id = e.program_id
            GROUP BY p.program_id
            ORDER BY avg_revenue DESC
            """
            program_cost_structure = pd.read_sql(query, self.conn)
            
            return {
                'program_profitability': program_profitability,
                'category_profitability': category_profitability,
                'delivery_mode_profitability': delivery_mode_profitability,
                'program_cost_structure': program_cost_structure
            }
        except Exception as e:
            return {'error': str(e)}
    
    def analyze_profitability_by_client(self):
        """
        Analyze profitability by client
        
        Returns:
            dict: Client profitability analysis
        """
        try:
            # Get client profitability
            query = """
            SELECT 
                c.client_id,
                c.name,
                c.industry,
                c.size,
                c.region,
                COUNT(e.enrollment_id) as enrollment_count,
                SUM(e.revenue) as total_revenue,
                SUM(e.trainer_cost + e.logistics_cost + e.venue_cost + e.utilities_cost + e.materials_cost) as total_costs,
                SUM(e.revenue - (e.trainer_cost + e.logistics_cost + e.venue_cost + e.utilities_cost + e.materials_cost)) as total_profit,
                CASE 
                    WHEN SUM(e.revenue) > 0 
                    THEN (SUM(e.revenue - (e.trainer_cost + e.logistics_cost + e.venue_cost + e.utilities_cost + e.materials_cost)) / SUM(e.revenue)) * 100 
                    ELSE 0 
                END as profit_margin
            FROM clients c
            JOIN enrollments e ON c.client_id = e.client_id
            GROUP BY c.client_id
            ORDER BY total_profit DESC
            """
            client_profitability = pd.read_sql(query, self.conn)
            
            # Get industry profitability
            query = """
            SELECT 
                c.industry,
                COUNT(e.enrollment_id) as enrollment_count,
                SUM(e.revenue) as total_revenue,
                SUM(e.trainer_cost + e.logistics_cost + e.venue_cost + e.utilities_cost + e.materials_cost) as total_costs,
                SUM(e.revenue - (e.trainer_cost + e.logistics_cost + e.venue_cost + e.utilities_cost + e.materials_cost)) as total_profit,
                CASE 
                    WHEN SUM(e.revenue) > 0 
                    THEN (SUM(e.revenue - (e.trainer_cost + e.logistics_cost + e.venue_cost + e.utilities_cost + e.materials_cost)) / SUM(e.revenue)) * 100 
                    ELSE 0 
                END as profit_margin
            FROM clients c
            JOIN enrollments e ON c.client_id = e.client_id
            WHERE c.industry IS NOT NULL
            GROUP BY c.industry
            ORDER BY total_profit DESC
            """
            industry_profitability = pd.read_sql(query, self.conn)
            
            # Get region profitability
            query = """
            SELECT 
                c.region,
                COUNT(e.enrollment_id) as enrollment_count,
                SUM(e.revenue) as total_revenue,
                SUM(e.trainer_cost + e.logistics_cost + e.venue_cost + e.utilities_cost + e.materials_cost) as total_costs,
                SUM(e.revenue - (e.trainer_cost + e.logistics_cost + e.venue_cost + e.utilities_cost + e.materials_cost)) as total_profit,
                CASE 
                    WHEN SUM(e.revenue) > 0 
                    THEN (SUM(e.revenue - (e.trainer_cost + e.logistics_cost + e.venue_cost + e.utilities_cost + e.materials_cost)) / SUM(e.revenue)) * 100 
                    ELSE 0 
                END as profit_margin
            FROM clients c
            JOIN enrollments e ON c.client_id = e.client_id
            WHERE c.region IS NOT NULL
            GROUP BY c.region
            ORDER BY total_profit DESC
            """
            region_profitability = pd.read_sql(query, self.conn)
            
            # Get client size profitability
            query = """
            SELECT 
                c.size,
                COUNT(e.enrollment_id) as enrollment_count,
                SUM(e.revenue) as total_revenue,
                SUM(e.trainer_cost + e.logistics_cost + e.venue_cost + e.utilities_cost + e.materials_cost) as total_costs,
                SUM(e.revenue - (e.trainer_cost + e.logistics_cost + e.venue_cost + e.utilities_cost + e.materials_cost)) as total_profit,
                CASE 
                    WHEN SUM(e.revenue) > 0 
                    THEN (SUM(e.revenue - (e.trainer_cost + e.logistics_cost + e.venue_cost + e.utilities_cost + e.materials_cost)) / SUM(e.revenue)) * 100 
                    ELSE 0 
                END as profit_margin
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
            size_profitability = pd.read_sql(query, self.conn)
            
            return {
                'client_profitability': client_profitability,
                'industry_profitability': industry_profitability,
                'region_profitability': region_profitability,
                'size_profitability': size_profitability
            }
        except Exception as e:
            return {'error': str(e)}
    
    def analyze_profitability_trends(self):
        """
        Analyze profitability trends over time
        
        Returns:
            dict: Profitability trends analysis
        """
        try:
            # Get profitability trends over time
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
                END as profit_margin,
                COUNT(enrollment_id) as enrollment_count
            FROM enrollments
            WHERE start_date IS NOT NULL
            GROUP BY month
            ORDER BY month
            """
            profitability_over_time = pd.read_sql(query, self.conn)
            
            # Get category profitability trends
            query = """
            SELECT 
                strftime('%Y-%m', e.start_date) as month,
                p.category,
                SUM(e.revenue) as total_revenue,
                SUM(e.trainer_cost + e.logistics_cost + e.venue_cost + e.utilities_cost + e.materials_cost) as total_costs,
                SUM(e.revenue - (e.trainer_cost + e.logistics_cost + e.venue_cost + e.utilities_cost + e.materials_cost)) as total_profit,
                CASE 
                    WHEN SUM(e.revenue) > 0 
                    THEN (SUM(e.revenue - (e.trainer_cost + e.logistics_cost + e.venue_cost + e.utilities_cost + e.materials_cost)) / SUM(e.revenue)) * 100 
                    ELSE 0 
                END as profit_margin
            FROM enrollments e
            JOIN programs p ON e.program_id = p.program_id
            WHERE e.start_date IS NOT NULL AND p.category IS NOT NULL
            GROUP BY month, p.category
            ORDER BY month, p.category
            """
            category_trends = pd.read_sql(query, self.conn)
            
            # Get delivery mode profitability trends
            query = """
            SELECT 
                strftime('%Y-%m', e.start_date) as month,
                e.delivery_mode,
                SUM(e.revenue) as total_revenue,
                SUM(e.trainer_cost + e.logistics_cost + e.venue_cost + e.utilities_cost + e.materials_cost) as total_costs,
                SUM(e.revenue - (e.trainer_cost + e.logistics_cost + e.venue_cost + e.utilities_cost + e.materials_cost)) as total_profit,
                CASE 
                    WHEN SUM(e.revenue) > 0 
                    THEN (SUM(e.revenue - (e.trainer_cost + e.logistics_cost + e.venue_cost + e.utilities_cost + e.materials_cost)) / SUM(e.revenue)) * 100 
                    ELSE 0 
                END as profit_margin
            FROM enrollments e
            WHERE e.start_date IS NOT NULL AND e.delivery_mode IS NOT NULL
            GROUP BY month, e.delivery_mode
            ORDER BY month, e.delivery_mode
            """
            delivery_mode_trends = pd.read_sql(query, self.conn)
            
            # Get cost component trends
            query = """
            SELECT 
                strftime('%Y-%m', start_date) as month,
                AVG(trainer_cost) as avg_trainer_cost,
                AVG(logistics_cost) as avg_logistics_cost,
                AVG(venue_cost) as avg_venue_cost,
                AVG(utilities_cost) as avg_utilities_cost,
                AVG(materials_cost) as avg_materials_cost,
                AVG(revenue) as avg_revenue,
                COUNT(enrollment_id) as enrollment_count
            FROM enrollments
            WHERE start_date IS NOT NULL
            GROUP BY month
            ORDER BY month
            """
            cost_component_trends = pd.read_sql(query, self.conn)
            
            return {
                'profitability_over_time': profitability_over_time,
                'category_trends': category_trends,
                'delivery_mode_trends': delivery_mode_trends,
                'cost_component_trends': cost_component_trends
            }
        except Exception as e:
            return {'error': str(e)}
    
    def analyze_budget_vs_actual(self):
        """
        Analyze budget vs actual performance
        
        Returns:
            dict: Budget vs actual analysis
        """
        try:
            # Get budget vs actual by program
            query = """
            SELECT 
                p.program_id,
                p.name,
                p.category,
                SUM(e.revenue) as actual_revenue,
                SUM(e.budgeted_revenue) as budgeted_revenue,
                SUM(e.trainer_cost + e.logistics_cost + e.venue_cost + e.utilities_cost + e.materials_cost) as actual_costs,
                SUM(e.budgeted_costs) as budgeted_costs,
                SUM(e.revenue - (e.trainer_cost + e.logistics_cost + e.venue_cost + e.utilities_cost + e.materials_cost)) as actual_profit,
                SUM(e.budgeted_revenue - e.budgeted_costs) as budgeted_profit,
                CASE 
                    WHEN SUM(e.budgeted_revenue) > 0 
                    THEN (SUM(e.revenue) / SUM(e.budgeted_revenue)) * 100 
                    ELSE 0 
                END as revenue_achievement,
                CASE 
                    WHEN SUM(e.budgeted_costs) > 0 
                    THEN (SUM(e.trainer_cost + e.logistics_cost + e.venue_cost + e.utilities_cost + e.materials_cost) / SUM(e.budgeted_costs)) * 100 
                    ELSE 0 
                END as cost_achievement,
                CASE 
                    WHEN SUM(e.budgeted_profit) > 0 
                    THEN (SUM(e.revenue - (e.trainer_cost + e.logistics_cost + e.venue_cost + e.utilities_cost + e.materials_cost)) / SUM(e.budgeted_profit)) * 100 
                    ELSE 0 
                END as profit_achievement
            FROM programs p
            JOIN enrollments e ON p.program_id = e.program_id
            WHERE e.budgeted_revenue > 0 OR e.budgeted_costs > 0
            GROUP BY p.program_id
            ORDER BY actual_profit DESC
            """
            budget_vs_actual_by_program = pd.read_sql(query, self.conn)
            
            # Get budget vs actual by time period
            query = """
            SELECT 
                strftime('%Y-%m', start_date) as month,
                SUM(revenue) as actual_revenue,
                SUM(budgeted_revenue) as budgeted_revenue,
                SUM(trainer_cost + logistics_cost + venue_cost + utilities_cost + materials_cost) as actual_costs,
                SUM(budgeted_costs) as budgeted_costs,
                SUM(revenue - (trainer_cost + logistics_cost + venue_cost + utilities_cost + materials_cost)) as actual_profit,
                SUM(budgeted_revenue - budgeted_costs) as budgeted_profit,
                CASE 
                    WHEN SUM(budgeted_revenue) > 0 
                    THEN (SUM(revenue) / SUM(budgeted_revenue)) * 100 
                    ELSE 0 
                END as revenue_achievement,
                CASE 
                    WHEN SUM(budgeted_costs) > 0 
                    THEN (SUM(trainer_cost + logistics_cost + venue_cost + utilities_cost + materials_cost) / SUM(budgeted_costs)) * 100 
                    ELSE 0 
                END as cost_achievement,
                CASE 
                    WHEN SUM(budgeted_profit) > 0 
                    THEN (SUM(revenue - (trainer_cost + logistics_cost + venue_cost + utilities_cost + materials_cost)) / SUM(budgeted_profit)) * 100 
                    ELSE 0 
                END as profit_achievement
            FROM enrollments
            WHERE (budgeted_revenue > 0 OR budgeted_costs > 0) AND start_date IS NOT NULL
            GROUP BY month
            ORDER BY month
            """
            budget_vs_actual_by_time = pd.read_sql(query, self.conn)
            
            # Get budget vs actual by category
            query = """
            SELECT 
                p.category,
                SUM(e.revenue) as actual_revenue,
                SUM(e.budgeted_revenue) as budgeted_revenue,
                SUM(e.trainer_cost + e.logistics_cost + e.venue_cost + e.utilities_cost + e.materials_cost) as actual_costs,
                SUM(e.budgeted_costs) as budgeted_costs,
                SUM(e.revenue - (e.trainer_cost + e.logistics_cost + e.venue_cost + e.utilities_cost + e.materials_cost)) as actual_profit,
                SUM(e.budgeted_revenue - e.budgeted_costs) as budgeted_profit,
                CASE 
                    WHEN SUM(e.budgeted_revenue) > 0 
                    THEN (SUM(e.revenue) / SUM(e.budgeted_revenue)) * 100 
                    ELSE 0 
                END as revenue_achievement,
                CASE 
                    WHEN SUM(e.budgeted_costs) > 0 
                    THEN (SUM(e.trainer_cost + e.logistics_cost + e.venue_cost + e.utilities_cost + e.materials_cost) / SUM(e.budgeted_costs)) * 100 
                    ELSE 0 
                END as cost_achievement,
                CASE 
                    WHEN SUM(e.budgeted_profit) > 0 
                    THEN (SUM(e.revenue - (e.trainer_cost + e.logistics_cost + e.venue_cost + e.utilities_cost + e.materials_cost)) / SUM(e.budgeted_profit)) * 100 
                    ELSE 0 
                END as profit_achievement
            FROM programs p
            JOIN enrollments e ON p.program_id = e.program_id
            WHERE (e.budgeted_revenue > 0 OR e.budgeted_costs > 0) AND p.category IS NOT NULL
            GROUP BY p.category
            ORDER BY actual_profit DESC
            """
            budget_vs_actual_by_category = pd.read_sql(query, self.conn)
            
            return {
                'budget_vs_actual_by_program': budget_vs_actual_by_program,
                'budget_vs_actual_by_time': budget_vs_actual_by_time,
                'budget_vs_actual_by_category': budget_vs_actual_by_category
            }
        except Exception as e:
            return {'error': str(e)}
    
    def get_program_profitability_details(self, program_id):
        """
        Get detailed profitability information for a specific program
        
        Args:
            program_id: ID of the program to analyze
            
        Returns:
            dict: Program profitability details
        """
        try:
            # Get program information
            query = f"SELECT * FROM programs WHERE program_id = {program_id}"
            program_info = pd.read_sql(query, self.conn)
            
            if program_info.empty:
                return {'error': f"Program with ID {program_id} not found"}
            
            # Get enrollment profitability
            query = f"""
            SELECT 
                e.enrollment_id,
                c.name as client_name,
                e.start_date,
                e.delivery_mode,
                e.num_participants,
                e.revenue,
                e.trainer_cost,
                e.logistics_cost,
                e.venue_cost,
                e.utilities_cost,
                e.materials_cost,
                e.trainer_cost + e.logistics_cost + e.venue_cost + e.utilities_cost + e.materials_cost as total_costs,
                e.revenue - (e.trainer_cost + e.logistics_cost + e.venue_cost + e.utilities_cost + e.materials_cost) as profit,
                CASE 
                    WHEN e.revenue > 0 
                    THEN (e.revenue - (e.trainer_cost + e.logistics_cost + e.venue_cost + e.utilities_cost + e.materials_cost)) / e.revenue * 100 
                    ELSE 0 
                END as profit_margin,
                e.budgeted_revenue,
                e.budgeted_costs,
                e.budgeted_revenue - e.budgeted_costs as budgeted_profit
            FROM enrollments e
            JOIN clients c ON e.client_id = c.client_id
            WHERE e.program_id = {program_id}
            ORDER BY e.start_date DESC
            """
            enrollment_profitability = pd.read_sql(query, self.conn)
            
            # Get profitability summary
            query = f"""
            SELECT 
                COUNT(e.enrollment_id) as enrollment_count,
                SUM(e.revenue) as total_revenue,
                SUM(e.trainer_cost + e.logistics_cost + e.venue_cost + e.utilities_cost + e.materials_cost) as total_costs,
                SUM(e.revenue - (e.trainer_cost + e.logistics_cost + e.venue_cost + e.utilities_cost + e.materials_cost)) as total_profit,
                CASE 
                    WHEN SUM(e.revenue) > 0 
                    THEN (SUM(e.revenue - (e.trainer_cost + e.logistics_cost + e.venue_cost + e.utilities_cost + e.materials_cost)) / SUM(e.revenue)) * 100 
                    ELSE 0 
                END as profit_margin,
                SUM(e.budgeted_revenue) as total_budgeted_revenue,
                SUM(e.budgeted_costs) as total_budgeted_costs,
                SUM(e.budgeted_revenue - e.budgeted_costs) as total_budgeted_profit,
                CASE 
                    WHEN SUM(e.budgeted_revenue) > 0 
                    THEN (SUM(e.revenue) / SUM(e.budgeted_revenue)) * 100 
                    ELSE 0 
                END as revenue_achievement,
                CASE 
                    WHEN SUM(e.budgeted_costs) > 0 
                    THEN (SUM(e.trainer_cost + e.logistics_cost + e.venue_cost + e.utilities_cost + e.materials_cost) / SUM(e.budgeted_costs)) * 100 
                    ELSE 0 
                END as cost_achievement
            FROM enrollments e
            WHERE e.program_id = {program_id}
            """
            profitability_summary = pd.read_sql(query, self.conn)
            
            # Get cost breakdown
            query = f"""
            SELECT 
                'Trainer Cost' as cost_type,
                SUM(trainer_cost) as total_cost,
                (SUM(trainer_cost) / SUM(trainer_cost + logistics_cost + venue_cost + utilities_cost + materials_cost)) * 100 as percentage
            FROM enrollments
            WHERE program_id = {program_id}
            UNION ALL
            SELECT 
                'Logistics Cost' as cost_type,
                SUM(logistics_cost) as total_cost,
                (SUM(logistics_cost) / SUM(trainer_cost + logistics_cost + venue_cost + utilities_cost + materials_cost)) * 100 as percentage
            FROM enrollments
            WHERE program_id = {program_id}
            UNION ALL
            SELECT 
                'Venue Cost' as cost_type,
                SUM(venue_cost) as total_cost,
                (SUM(venue_cost) / SUM(trainer_cost + logistics_cost + venue_cost + utilities_cost + materials_cost)) * 100 as percentage
            FROM enrollments
            WHERE program_id = {program_id}
            UNION ALL
            SELECT 
                'Utilities Cost' as cost_type,
                SUM(utilities_cost) as total_cost,
                (SUM(utilities_cost) / SUM(trainer_cost + logistics_cost + venue_cost + utilities_cost + materials_cost)) * 100 as percentage
            FROM enrollments
            WHERE program_id = {program_id}
            UNION ALL
            SELECT 
                'Materials Cost' as cost_type,
                SUM(materials_cost) as total_cost,
                (SUM(materials_cost) / SUM(trainer_cost + logistics_cost + venue_cost + utilities_cost + materials_cost)) * 100 as percentage
            FROM enrollments
            WHERE program_id = {program_id}
            ORDER BY total_cost DESC
            """
            cost_breakdown = pd.read_sql(query, self.conn)
            
            # Get profitability by client
            query = f"""
            SELECT 
                c.client_id,
                c.name,
                c.industry,
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
            JOIN clients c ON e.client_id = c.client_id
            WHERE e.program_id = {program_id}
            GROUP BY c.client_id
            ORDER BY total_profit DESC
            """
            profitability_by_client = pd.read_sql(query, self.conn)
            
            # Get profitability by delivery mode
            query = f"""
            SELECT 
                e.delivery_mode,
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
            WHERE e.program_id = {program_id} AND e.delivery_mode IS NOT NULL
            GROUP BY e.delivery_mode
            ORDER BY total_profit DESC
            """
            profitability_by_delivery_mode = pd.read_sql(query, self.conn)
            
            # Get profitability trends over time
            query = f"""
            SELECT 
                strftime('%Y-%m', start_date) as month,
                COUNT(enrollment_id) as enrollment_count,
                SUM(revenue) as total_revenue,
                SUM(trainer_cost + logistics_cost + venue_cost + utilities_cost + materials_cost) as total_costs,
                SUM(revenue - (trainer_cost + logistics_cost + venue_cost + utilities_cost + materials_cost)) as total_profit,
                CASE 
                    WHEN SUM(revenue) > 0 
                    THEN (SUM(revenue - (trainer_cost + logistics_cost + venue_cost + utilities_cost + materials_cost)) / SUM(revenue)) * 100 
                    ELSE 0 
                END as profit_margin
            FROM enrollments
            WHERE program_id = {program_id} AND start_date IS NOT NULL
            GROUP BY month
            ORDER BY month
            """
            profitability_trends = pd.read_sql(query, self.conn)
            
            return {
                'program_info': program_info,
                'enrollment_profitability': enrollment_profitability,
                'profitability_summary': profitability_summary,
                'cost_breakdown': cost_breakdown,
                'profitability_by_client': profitability_by_client,
                'profitability_by_delivery_mode': profitability_by_delivery_mode,
                'profitability_trends': profitability_trends
            }
        except Exception as e:
            return {'error': str(e)}
    
    def create_profit_margin_chart(self, data=None):
        """
        Create a profit margin chart for top programs
        
        Args:
            data: Optional pre-loaded data dictionary
            
        Returns:
            plotly.graph_objects.Figure: Profit margin chart
        """
        if data is None:
            overview = self.get_profitability_overview()
        else:
            overview = data
        
        if 'error' in overview:
            return None
        
        # Create profit margin chart
        if 'top_profitable_programs' in overview and not overview['top_profitable_programs'].empty:
            df = overview['top_profitable_programs'].sort_values('profit_margin')
            
            fig = px.bar(
                df,
                y='name',
                x='profit_margin',
                title='Top Programs by Profit Margin',
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
            overview = self.get_profitability_overview()
        else:
            overview = data
        
        if 'error' in overview:
            return None
        
        # Create cost breakdown chart
        if 'cost_breakdown' in overview and not overview['cost_breakdown'].empty:
            fig = px.pie(
                overview['cost_breakdown'], 
                values='total_cost', 
                names='cost_type',
                title='Cost Breakdown',
                color_discrete_sequence=px.colors.sequential.RdBu
            )
            fig.update_traces(textposition='inside', textinfo='percent+label')
            return fig
        
        return None
    
    def create_profitability_trends_chart(self, data=None):
        """
        Create a profitability trends chart
        
        Args:
            data: Optional pre-loaded data dictionary
            
        Returns:
            plotly.graph_objects.Figure: Profitability trends chart
        """
        if data is None:
            trends_data = self.analyze_profitability_trends()
        else:
            trends_data = data
        
        if 'error' in trends_data:
            return None
        
        # Create profitability trends chart
        if 'profitability_over_time' in trends_data and not trends_data['profitability_over_time'].empty:
            df = trends_data['profitability_over_time']
            
            # Create figure with secondary y-axis
            fig = make_subplots(specs=[[{"secondary_y": True}]])
            
            # Add revenue and cost bars
            fig.add_trace(
                go.Bar(
                    x=df['month'],
                    y=df['total_revenue'],
                    name='Revenue',
                    marker_color='rgb(26, 118, 255)'
                ),
                secondary_y=False
            )
            
            fig.add_trace(
                go.Bar(
                    x=df['month'],
                    y=df['total_costs'],
                    name='Costs',
                    marker_color='rgb(219, 64, 82)'
                ),
                secondary_y=False
            )
            
            # Add profit line
            fig.add_trace(
                go.Scatter(
                    x=df['month'],
                    y=df['total_profit'],
                    name='Profit',
                    line=dict(color='rgb(46, 184, 46)', width=3),
                    mode='lines+markers'
                ),
                secondary_y=False
            )
            
            # Add profit margin line
            fig.add_trace(
                go.Scatter(
                    x=df['month'],
                    y=df['profit_margin'],
                    name='Profit Margin (%)',
                    line=dict(color='rgb(255, 127, 14)', width=3, dash='dot'),
                    mode='lines+markers'
                ),
                secondary_y=True
            )
            
            # Update layout
            fig.update_layout(
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
            fig.update_yaxes(title_text="Amount ($)", secondary_y=False)
            fig.update_yaxes(title_text="Profit Margin (%)", secondary_y=True)
            
            return fig
        
        return None
    
    def create_category_profitability_chart(self, data=None):
        """
        Create a category profitability chart
        
        Args:
            data: Optional pre-loaded data dictionary
            
        Returns:
            plotly.graph_objects.Figure: Category profitability chart
        """
        if data is None:
            program_data = self.analyze_profitability_by_program()
        else:
            program_data = data
        
        if 'error' in program_data:
            return None
        
        # Create category profitability chart
        if 'category_profitability' in program_data and not program_data['category_profitability'].empty:
            df = program_data['category_profitability']
            
            # Create figure with secondary y-axis
            fig = make_subplots(specs=[[{"secondary_y": True}]])
            
            # Add revenue and profit bars
            fig.add_trace(
                go.Bar(
                    x=df['category'],
                    y=df['total_revenue'],
                    name='Revenue',
                    marker_color='rgb(26, 118, 255)'
                ),
                secondary_y=False
            )
            
            fig.add_trace(
                go.Bar(
                    x=df['category'],
                    y=df['total_profit'],
                    name='Profit',
                    marker_color='rgb(46, 184, 46)'
                ),
                secondary_y=False
            )
            
            # Add profit margin line
            fig.add_trace(
                go.Scatter(
                    x=df['category'],
                    y=df['profit_margin'],
                    name='Profit Margin (%)',
                    line=dict(color='rgb(255, 127, 14)', width=3),
                    mode='lines+markers'
                ),
                secondary_y=True
            )
            
            # Update layout
            fig.update_layout(
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
            fig.update_yaxes(title_text="Amount ($)", secondary_y=False)
            fig.update_yaxes(title_text="Profit Margin (%)", secondary_y=True)
            
            return fig
        
        return None
    
    def create_budget_vs_actual_chart(self, data=None):
        """
        Create a budget vs actual chart
        
        Args:
            data: Optional pre-loaded data dictionary
            
        Returns:
            plotly.graph_objects.Figure: Budget vs actual chart
        """
        if data is None:
            budget_data = self.analyze_budget_vs_actual()
        else:
            budget_data = data
        
        if 'error' in budget_data:
            return None
        
        # Create budget vs actual chart by time
        if 'budget_vs_actual_by_time' in budget_data and not budget_data['budget_vs_actual_by_time'].empty:
            df = budget_data['budget_vs_actual_by_time']
            
            # Create figure with secondary y-axis
            fig = make_subplots(specs=[[{"secondary_y": True}]])
            
            # Add actual revenue and budgeted revenue bars
            fig.add_trace(
                go.Bar(
                    x=df['month'],
                    y=df['actual_revenue'],
                    name='Actual Revenue',
                    marker_color='rgb(26, 118, 255)'
                ),
                secondary_y=False
            )
            
            fig.add_trace(
                go.Bar(
                    x=df['month'],
                    y=df['budgeted_revenue'],
                    name='Budgeted Revenue',
                    marker_color='rgba(26, 118, 255, 0.5)'
                ),
                secondary_y=False
            )
            
            # Add actual profit and budgeted profit bars
            fig.add_trace(
                go.Bar(
                    x=df['month'],
                    y=df['actual_profit'],
                    name='Actual Profit',
                    marker_color='rgb(46, 184, 46)'
                ),
                secondary_y=False
            )
            
            fig.add_trace(
                go.Bar(
                    x=df['month'],
                    y=df['budgeted_profit'],
                    name='Budgeted Profit',
                    marker_color='rgba(46, 184, 46, 0.5)'
                ),
                secondary_y=False
            )
            
            # Add achievement percentage line
            fig.add_trace(
                go.Scatter(
                    x=df['month'],
                    y=df['profit_achievement'],
                    name='Profit Achievement (%)',
                    line=dict(color='rgb(255, 127, 14)', width=3),
                    mode='lines+markers'
                ),
                secondary_y=True
            )
            
            # Update layout
            fig.update_layout(
                title='Budget vs Actual Performance Over Time',
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
            fig.update_yaxes(title_text="Amount ($)", secondary_y=False)
            fig.update_yaxes(title_text="Achievement (%)", secondary_y=True)
            
            return fig
        
        return None
    
    def close(self):
        """Close the database connection"""
        if hasattr(self, 'conn') and self.conn:
            self.conn.close()


# Example usage:
if __name__ == "__main__":
    tracker = ProfitabilityTracker()
    
    # Example: Get profitability overview
    overview = tracker.get_profitability_overview()
    print(f"Total profit: ${overview.get('financial_summary', {}).get('total_profit', [0])[0]:,.2f}")
    
    # Example: Create a chart
    fig = tracker.create_profit_margin_chart()
    if fig:
        fig.show()
    
    tracker.close()
